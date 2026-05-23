/*
 * Copyright 2026 Typelevel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.kafka.otel4s.trace

import cats.effect.Concurrent
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.{CommittableConsumerRecord, ConsumerRecord, KafkaConsumer}
import fs2.kafka.otel4s.trace.instances._
import fs2.kafka.otel4s.trace.internal.Semconv
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.trace.{SpanContext, SpanKind, Tracer}

/** A consumer-bound tracing handle for `fs2-kafka`.
  *
  * Unlike producer tracing, consumer tracing cannot be a fully transparent drop-in replacement, because the important
  * `process` boundary lives in user code. This handle keeps the association with a specific [[fs2.kafka.KafkaConsumer]]
  * while still making `receive` and `process` explicit.
  */
trait TracedKafkaConsumer[F[_], K, V] {

  /** The underlying Kafka consumer this tracing handle is bound to.
    */
  def underlying: KafkaConsumer[F, K, V]

  /** Delegates to [[underlying.records]].
    *
    * Record emission itself is not treated as processing. Wrap the actual business logic with [[process]] or use
    * [[recordsWithProcess]] for the common `evalMap` shape.
    */
  def records: Stream[F, CommittableConsumerRecord[F, K, V]]

  /** Delegates to [[underlying.partitionedRecords]].
    *
    * This is a passthrough convenience so partition-oriented code can stay on the traced handle without reaching back
    * to [[underlying]].
    *
    * {{{
    * tracedConsumer.partitionedRecords
    * // is equivalent to
    * tracedConsumer.underlying.partitionedRecords
    * }}}
    */
  def partitionedRecords: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]]

  /** Delegates to [[underlying.partitionedStream]].
    *
    * This is a passthrough convenience so partition-oriented code can stay on the traced handle without reaching back
    * to [[underlying]].
    *
    * {{{
    * tracedConsumer.partitionedStream
    * // is equivalent to
    * tracedConsumer.underlying.partitionedStream
    * }}}
    */
  def partitionedStream: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]]

  /** Consume from all assigned partitions concurrently, tracing delivery of each emitted chunk to the supplied
    * callback.
    *
    * This helper models chunk delivery with `receive` spans. If you want per-record `process` spans, use
    * [[recordsWithProcess]] or wrap explicit record handling with [[process]].
    */
  def consumeChunk(
      processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow]
  ): F[Nothing]

  /** Evaluates `fa` inside a `poll` / `receive` span representing delivery of a non-committable chunk of records to
    * application code.
    */
  def receive[A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A]

  /** Evaluates `fa` inside a `poll` / `receive` span representing delivery of a committable chunk of records to
    * application code.
    *
    * This is a convenience overload of [[receive]] for `Chunk[CommittableConsumerRecord[...]]`. The commit handle is
    * preserved, but span attributes are derived from the wrapped [[CommittableConsumerRecord.record]] values.
    *
    * Is shorthand for:
    *
    * {{{
    * tracedConsumer.receive(records.map(_.record))(fa)
    * }}}
    */
  def receiveCommittable[A](records: Chunk[CommittableConsumerRecord[F, K, V]])(fa: F[A]): F[A]

  /** Evaluates `fa` inside a `process` span using trace context extracted from the record headers when available.
    */
  def process[A](record: ConsumerRecord[K, V])(fa: F[A]): F[A]

  /** Evaluates `fa` inside a `process` span using trace context extracted from the record headers when available.
    *
    * This is a convenience overload of [[process]] for [[CommittableConsumerRecord]] that forwards to the plain
    * [[ConsumerRecord]]-based variant using [[CommittableConsumerRecord.record]].
    *
    * Is shorthand for:
    *
    * {{{
    * tracedConsumer.process(committable.record)(fa)
    * }}}
    */
  def process[A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A]

  /** Convenience stream for the common traced-consumption shape: a chunk-level `receive` span around delivery, plus a
    * per-record `process` span for each record in that chunk.
    *
    * This is shorthand for consuming the underlying partitioned stream chunk-by-chunk, wrapping each chunk with
    * [[receiveCommittable]], and then wrapping each record callback with [[process]].
    *
    * Is shorthand for:
    *
    * {{{
    * tracedConsumer.partitionedStream
    *   .map(
    *     _.chunks.flatMap { chunk =>
    *       Stream
    *         .eval(tracedConsumer.receiveCommittable(chunk)(Concurrent[F].pure(chunk)))
    *         .flatMap(records =>
    *           Stream.chunk(records).evalMap(record => tracedConsumer.process(record)(f(record)))
    *         )
    *     }
    *   )
    *   .parJoinUnbounded
    * }}}
    */
  def recordsWithProcess[A](f: CommittableConsumerRecord[F, K, V] => F[A]): Stream[F, A]

}

object TracedKafkaConsumer {

  final private[otel4s] class Impl[F[_]: Concurrent: Tracer, K: KafkaMessageKey, V](
      override val underlying: KafkaConsumer[F, K, V],
      config: KafkaTracer.Config
  ) extends TracedKafkaConsumer[F, K, V] {

    private val clientId =
      underlying.settings.properties.get(CommonClientConfigs.CLIENT_ID_CONFIG)

    private val groupId =
      underlying.settings.properties.get(ConsumerConfig.GROUP_ID_CONFIG)

    override def records: Stream[F, CommittableConsumerRecord[F, K, V]] =
      underlying.records

    override def partitionedRecords: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]] =
      underlying.partitionedRecords

    override def partitionedStream: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]] =
      underlying.partitionedStream

    override def consumeChunk(
        processor: Chunk[ConsumerRecord[K, V]] => F[CommitNow]
    ): F[Nothing] =
      underlying
        .partitionedStream
        .map(
          _.chunks.evalMap { chunk =>
            receiveCommittable(chunk)(processor(chunk.map(_.record))).as(())
          }
        )
        .parJoinUnbounded
        .drain
        .compile
        .onlyOrError

    override def receive[A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] =
      if (records.isEmpty) fa
      else {
        val spanContext = Semconv.receiveSpanContext(records, clientId, groupId)
        val spanSetup = config.receiveSpanSetup(spanContext)
        creationContextLinks(records.toList).flatMap { links =>
          val builder = Tracer[F]
            .spanBuilder(spanSetup.spanName)
            .withSpanKind(SpanKind.Client)
            .withFinalizationStrategy(spanSetup.finalizationStrategy)
            .addAttributes(
              Semconv.receiveAttributes(spanContext, records) ++
                config.constAttributes ++
                spanSetup.attributes
            )
          val linked = links.foldLeft(builder) { case (acc, (ctx, attributes)) =>
            acc.addLink(ctx, attributes)
          }

          linked
            .build
            .surround(fa)
        }
      }

    override def receiveCommittable[A](
        records: Chunk[CommittableConsumerRecord[F, K, V]]
    )(fa: F[A]): F[A] =
      receive(records.map(_.record))(fa)

    override def process[A](record: ConsumerRecord[K, V])(fa: F[A]): F[A] = {
      val spanContext = Semconv.processSpanContext(record, clientId, groupId)
      val spanSetup = config.processSpanSetup(spanContext)
      creationContextLinks(record :: Nil).flatMap { links =>
        val builder = Tracer[F]
          .spanBuilder(spanSetup.spanName)
          .withSpanKind(SpanKind.Consumer)
          .withFinalizationStrategy(spanSetup.finalizationStrategy)
          .addAttributes(
            Semconv.processAttributes(spanContext, record) ++
              config.constAttributes ++
              spanSetup.attributes
          )
        val linked = links.foldLeft(builder) { case (acc, (ctx, attributes)) =>
          acc.addLink(ctx, attributes)
        }

        linked
          .build
          .surround(fa)
      }
    }

    override def process[A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A] =
      process(record.record)(fa)

    override def recordsWithProcess[A](
        f: CommittableConsumerRecord[F, K, V] => F[A]
    ): Stream[F, A] =
      underlying
        .partitionedStream
        .map(
          _.chunks
            .flatMap { chunk =>
              Stream
                .eval(receiveCommittable(chunk)(Concurrent[F].pure(chunk)))
                .flatMap(records =>
                  Stream.chunk(records).evalMap(record => process(record)(f(record)))
                )
            }
        )
        .parJoinUnbounded

    private def creationContextLinks(
        records: Iterable[ConsumerRecord[K, V]]
    ): F[List[(SpanContext, Attributes)]] =
      records
        .toList
        .flatTraverse { record =>
          Tracer[F]
            .joinOrRoot(record.headers)(Tracer[F].currentSpanContext)
            .map(_.tupleRight(Semconv.receiveLinkAttributes(record)).toList)
        }

  }

}

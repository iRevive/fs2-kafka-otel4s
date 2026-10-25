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

import cats.Parallel
import cats.effect.Concurrent
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.{CommittableConsumerRecord, CommittableOffsetBatch, ConsumerRecord, KafkaConsumer}
import org.typelevel.otel4s.trace.Tracer

/** A consumer-bound tracing handle for `fs2-kafka`.
  *
  * Unlike producer tracing, consumer tracing cannot be a fully transparent drop-in replacement, because the important
  * `process` boundary lives in user code. This handle keeps the association with a specific [[fs2.kafka.KafkaConsumer]]
  * while still making consumer tracing explicit.
  */
trait TracedKafkaConsumer[F[_], K, V] {

  /** The underlying Kafka consumer this tracing handle is bound to.
    */
  def underlying: KafkaConsumer[F, K, V]

  /** Configured process and receive operations bound to this consumer's cached metadata. */
  def spans: ConsumerSpans[F]

  /** Delegates to `underlying.records`.
    *
    * Record emission itself is not treated as processing. Wrap the actual business logic with `spans.process` or use
    * [[recordsTraced]] for the common `evalMap` shape.
    */
  @deprecated("Use underlying.records", "0.3")
  final def records: Stream[F, CommittableConsumerRecord[F, K, V]] =
    underlying.records

  /** Delegates to `underlying.partitionedRecords`.
    *
    * This is a passthrough convenience so partition-oriented code can stay on the traced handle without reaching back
    * to [[underlying]].
    */
  @deprecated("Use underlying.partitionedRecords", "0.3")
  final def partitionedRecords: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]] =
    underlying.partitionedRecords

  /** Delegates to `underlying.partitionedStream`.
    *
    * This is a passthrough convenience so partition-oriented code can stay on the traced handle without reaching back
    * to [[underlying]].
    */
  @deprecated("Use underlying.partitionedStream", "0.3")
  final def partitionedStream: Stream[F, Stream[F, CommittableConsumerRecord[F, K, V]]] =
    underlying.partitionedStream

  /** Delegates to `underlying.consumeChunk` without adding tracing.
    *
    * This passthrough keeps raw chunk-oriented `fs2-kafka` code available on the traced handle. Use
    * [[consumeChunkTraced]] for one batch `process` span or [[consumeRecordTraced]] for per-record `process` spans,
    * each with `receive` and owned `commit` tracing.
    */
  @deprecated("Use underlying.consumeChunk", "0.3")
  final def consumeChunk(
      chunkProcessor: Chunk[ConsumerRecord[K, V]] => F[CommitNow]
  )(implicit F: Concurrent[F], P: Parallel[F]): F[Nothing] =
    underlying.consumeChunk(chunkProcessor)

  /** Consume from all assigned partitions concurrently, tracing each emitted chunk.
    *
    * Each emitted committable chunk is wrapped in a chunk-level `receive` span via [[receiveCommittable]], then in one
    * batch `process` span while the supplied `chunkProcessor` runs. The callback receives the plain [[ConsumerRecord]]
    * values for that chunk and runs once. Offsets from the corresponding committable records are collected and
    * committed after the callback effect completes successfully, outside the receive span and inside a `commit` span.
    *
    * Configure [[KafkaTracer.Config.withoutReceiveSpans]], [[KafkaTracer.Config.withoutProcessSpans]], or
    * [[KafkaTracer.Config.withoutCommitSpans]] to suppress individual spans without changing consumption or commit
    * behavior.
    *
    * Execution scope for one chunk, with all spans enabled and no ambient application span:
    * {{{
    * Input chunk: [m1, m2, m3]
    *
    *                          time -->
    *                     +--------------------------+--------+
    * handler             | handleChunk([m1,m2,m3])  |commit  |
    *                     |       called once        |offsets |
    *                     +--------------------------+--------+
    *
    * Span lifetime                                             Controlled by
    * --------------------------------------------------------  -------------------------
    * > receive           [=========================]           withReceiveSpanSetup
    * > process batch     [=========================]           withBatchProcessSpanSetup
    * > commit                                       [=======]  withCommitSpanSetup
    * }}}
    *
    * Span rows are aligned because receive and process spans are independent roots. Bars show timing; widths are
    * illustrative. Returning `None` from a setup function removes only its span row; handler execution and commit
    * behavior are unchanged. Indentation is reserved for actual parent-child span relationships.
    *
    * Process and receive spans are roots linked to incoming message contexts. Failure or cancellation of the callback
    * skips the owned commit. Selecting the latest record per key inside the callback still commits the original chunk's
    * offsets after success. Explicit `spans.process` calls add spans only around selected work, controlled by
    * [[KafkaTracer.Config.withProcessSpanSetup]].
    *
    * To keep only those explicit per-record process spans, configure `withBatchProcessSpanSetup(_ => None)`.
    *
    * To create app child spans for each message you actually process, use the current batch process span as parent:
    * {{{
    * tracedConsumer.consumeChunkTraced { chunk =>
    *   chunk.traverse_ { record =>
    *     Tracer[IO].span("processor").surround(handle(record))
    *   }.as(CommitNow)
    * }
    * }}}
    * With all spans enabled and no ambient app span, the parentage is:
    * {{{
    * > receive
    * > process batch
    *   > processor (m1)
    *   > processor (m2)
    *   > processor (m3)
    * > commit
    * }}}
    */
  def consumeChunkTraced(chunkProcessor: Chunk[ConsumerRecord[K, V]] => F[CommitNow]): F[Nothing]

  /** Consume from all assigned partitions concurrently, tracing each emitted chunk.
    *
    * Each emitted committable chunk is split into offsets and plain [[ConsumerRecord]] values. The supplied
    * `recordProcessor` is evaluated once per record inside `process`, so each record gets its own `process` span using
    * trace context extracted from that record's headers when available. The chunk is wrapped in a `receive` span, and
    * offsets from the corresponding committable records are collected and committed after all records have been
    * processed successfully inside a `commit` span.
    *
    * Configure [[KafkaTracer.Config.withoutReceiveSpans]], [[KafkaTracer.Config.withoutProcessSpans]], or
    * [[KafkaTracer.Config.withoutCommitSpans]] to suppress individual spans without changing consumption or commit
    * behavior.
    *
    * Execution scope for one chunk, with all spans enabled and no ambient application span:
    * {{{
    * Input chunk: [m1, m2, m3]
    *
    *                          time -->
    *                     +--------+--------+--------+--------+
    * handler             |handle  |handle  |handle  |commit  |
    *                     |  m1    |  m2    |  m3    |offsets |
    *                     +--------+--------+--------+--------+
    *
    * Span lifetime                                             Controlled by
    * --------------------------------------------------------  -------------------------
    * > receive           [=========================]           withReceiveSpanSetup
    * > process (m1)      [=======]                             withProcessSpanSetup
    * > process (m2)               [=======]                    withProcessSpanSetup
    * > process (m3)                        [=======]           withProcessSpanSetup
    * > commit                                       [=======]  withCommitSpanSetup
    * }}}
    *
    * Span rows are aligned because receive and process spans are independent roots. Bars show timing; widths are
    * illustrative. Returning `None` from a setup function removes only its span row; handler execution and commit
    * behavior are unchanged. Indentation is reserved for actual parent-child span relationships.
    *
    * Process and receive spans are roots linked to incoming message contexts. Failure or cancellation of a handler
    * skips the owned commit.
    */
  def consumeRecordTraced[A](recordProcessor: ConsumerRecord[K, V] => F[A]): F[Nothing]

  /** Evaluates `fa` inside a `poll` / `receive` span representing delivery of a non-committable chunk of records to
    * application code.
    */
  @deprecated("Use spans.receive(records)(fa)", "0.3")
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
    * tracedConsumer.spans.receiveCommittable(records)(fa)
    * }}}
    */
  @deprecated("Use spans.receiveCommittable(records)(fa)", "0.3")
  def receiveCommittable[A](records: Chunk[CommittableConsumerRecord[F, K, V]])(fa: F[A]): F[A]

  /** Evaluates `fa` inside a `process` span using trace context extracted from the record headers when available.
    */
  @deprecated("Use spans.process(record)(fa)", "0.3")
  def process[A](record: ConsumerRecord[K, V])(fa: F[A]): F[A]

  /** Evaluates `fa` inside a `process` span using trace context extracted from the record headers when available.
    *
    * This is a convenience overload of `process` for [[CommittableConsumerRecord]] that forwards to the plain
    * [[ConsumerRecord]]-based variant using [[CommittableConsumerRecord.record]].
    *
    * Is shorthand for:
    *
    * {{{
    * tracedConsumer.spans.process(committable)(fa)
    * }}}
    */
  @deprecated("Use spans.process(record)(fa)", "0.3")
  def process[A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A]

  /** Convenience stream for the common traced-consumption shape: a chunk-level `receive` span around delivery, plus a
    * per-record `process` span for each record in that chunk.
    *
    * This is shorthand for consuming the underlying partitioned stream chunk-by-chunk, wrapping each chunk with
    * [[receiveCommittable]], and then wrapping each record callback with `process`.
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
    *
    * This method does not commit offsets or emit `commit` spans. Use it with a downstream commit pipe such as
    * `commitBatchWithin`; generic commit pipes only receive offsets, not the consumed records required for Kafka span
    * attributes.
    *
    * Execution scope for one chunk, with all spans enabled and no ambient application span:
    * {{{
    * Input chunk: [m1, m2, m3]
    *
    *                          time -->
    *                     +--------+--------+--------+--------+
    * work                |deliver |handle  |handle  |handle  |
    *                     | chunk  |  m1    |  m2    |  m3    |
    *                     +--------+--------+--------+--------+
    *
    * Span lifetime                                             Controlled by
    * --------------------------------------------------------  -------------------------
    * > receive           [=======]                             withReceiveSpanSetup
    * > process (m1)               [=======]                    withProcessSpanSetup
    * > process (m2)                        [=======]           withProcessSpanSetup
    * > process (m3)                                 [=======]  withProcessSpanSetup
    *
    * Results are emitted after each handler; no automatic commit.
    * }}}
    *
    * Span rows are aligned because receive and process spans are independent roots. Bars show timing; widths are
    * illustrative. Returning `None` from a setup function removes only its span row; handler execution and commit
    * behavior are unchanged. Indentation is reserved for actual parent-child span relationships.
    *
    * Process and receive spans are roots linked to incoming message contexts. A downstream standard fs2-kafka commit
    * pipe commits offsets without adding tracing on its own.
    */
  def recordsTraced[A](f: CommittableConsumerRecord[F, K, V] => F[A]): Stream[F, A]

}

object TracedKafkaConsumer {

  /** Creates a no-op traced consumer bound to `underlying`.
    *
    * Consumer operations are delegated without emitting spans or extracting trace context.
    */
  def noop[F[_]: Concurrent: Parallel, K, V](
      underlying: KafkaConsumer[F, K, V]
  ): TracedKafkaConsumer[F, K, V] =
    new Noop[F, K, V](underlying)

  final private[otel4s] class Impl[F[_]: Concurrent: Parallel: Tracer, K: KafkaMessageKey, V](
      override val underlying: KafkaConsumer[F, K, V],
      config: KafkaTracer.Config
  ) extends TracedKafkaConsumer[F, K, V] {

    private val spanCore =
      ConsumerSpans[F](ConsumerMetadata.Provider.fromConsumer(underlying), config)

    override val spans: ConsumerSpans[F] = spanCore

    override def consumeChunkTraced(
        chunkProcessor: Chunk[ConsumerRecord[K, V]] => F[CommitNow]
    ): F[Nothing] = {
      def handleChunk(chunk: Chunk[CommittableConsumerRecord[F, K, V]]): F[Unit] = {
        val (offsets, records) = offsetsAndRecords(chunk)

        spans.receiveCommittable(chunk)(spans.processBatch(records)(chunkProcessor(records))) >>
          spanCore.commit(records)(offsets.commit)
      }

      handleChunkImpl(handleChunk)
    }

    override def consumeRecordTraced[A](recordProcessor: ConsumerRecord[K, V] => F[A]): F[Nothing] = {
      def handleChunk(chunk: Chunk[CommittableConsumerRecord[F, K, V]]): F[Unit] = {
        val (offsets, records) = offsetsAndRecords(chunk)

        spans.receiveCommittable(chunk)(
          records.traverseVoid(record => spans.process(record)(recordProcessor(record)))
        ) >>
          spanCore.commit(records)(offsets.commit)
      }

      handleChunkImpl(handleChunk)
    }

    private def offsetsAndRecords(
        chunk: Chunk[CommittableConsumerRecord[F, K, V]]
    ): (CommittableOffsetBatch[F], Chunk[ConsumerRecord[K, V]]) =
      chunk.mapAccumulate(CommittableOffsetBatch.empty[F])((offsetBatch, committableRecord) =>
        (offsetBatch.updated(committableRecord.offset), committableRecord.record)
      )

    private def handleChunkImpl(handleChunk: Chunk[CommittableConsumerRecord[F, K, V]] => F[Unit]) =
      underlying.partitionedStream
        .map(_.chunks.evalMap(handleChunk))
        .parJoinUnbounded
        .drain
        .compile
        .onlyOrError

    override def receive[A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] =
      spans.receive(records)(fa)

    override def receiveCommittable[A](
        records: Chunk[CommittableConsumerRecord[F, K, V]]
    )(fa: F[A]): F[A] =
      spans.receiveCommittable(records)(fa)

    override def process[A](record: ConsumerRecord[K, V])(fa: F[A]): F[A] =
      spans.process(record)(fa)

    override def process[A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A] =
      spans.process(record)(fa)

    override def recordsTraced[A](
        f: CommittableConsumerRecord[F, K, V] => F[A]
    ): Stream[F, A] =
      underlying.partitionedStream
        .map(
          _.chunks
            .flatMap { chunk =>
              Stream
                .eval(spans.receiveCommittable(chunk)(Concurrent[F].pure(chunk)))
                .flatMap(records => Stream.chunk(records).evalMap(record => spans.process(record)(f(record))))
            }
        )
        .parJoinUnbounded

  }

  final private class Noop[F[_]: Concurrent: Parallel, K, V](
      override val underlying: KafkaConsumer[F, K, V]
  ) extends TracedKafkaConsumer[F, K, V] {

    override val spans: ConsumerSpans[F] = ConsumerSpans.noop[F]

    override def consumeChunkTraced(chunkProcessor: Chunk[ConsumerRecord[K, V]] => F[CommitNow]): F[Nothing] =
      underlying.consumeChunk(chunkProcessor)

    override def consumeRecordTraced[A](recordProcessor: ConsumerRecord[K, V] => F[A]): F[Nothing] =
      underlying.consumeChunk(records => records.traverseVoid(recordProcessor).as(CommitNow))

    override def recordsTraced[A](f: CommittableConsumerRecord[F, K, V] => F[A]): Stream[F, A] =
      underlying.partitionedStream
        .map(_.evalMap(f))
        .parJoinUnbounded

    override def receive[A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] =
      fa

    override def receiveCommittable[A](
        records: Chunk[CommittableConsumerRecord[F, K, V]]
    )(fa: F[A]): F[A] =
      fa

    override def process[A](record: ConsumerRecord[K, V])(fa: F[A]): F[A] =
      fa

    override def process[A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A] =
      fa

  }

}

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
import fs2.Chunk
import fs2.kafka.{CommittableConsumerRecord, ConsumerRecord}
import fs2.kafka.otel4s.trace.instances._
import fs2.kafka.otel4s.trace.internal.Semconv
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.trace.{SpanContext, SpanKind, Tracer}

import scala.util.chaining._

/** Configured consumer tracing operations independent of a live Kafka consumer.
  *
  * Each operation uses the corresponding setup function and attributes in [[KafkaTracer.Config]]. Returning `None`
  * suppresses the span while still evaluating the supplied effect. A noop tracer also evaluates effects directly and
  * performs no propagation. Empty receive chunks evaluate the effect without creating a span. Process spans remain
  * roots linked to recognized contexts from the record headers; receive spans remain roots linked to contexts from each
  * record in the chunk.
  */
sealed trait ConsumerSpans[F[_]] {

  /** Evaluates `fa` with process tracing configured for this record. */
  def process[K: KafkaMessageKey, V, A](record: ConsumerRecord[K, V])(fa: F[A]): F[A]

  /** Evaluates `fa` with process tracing configured for this committable record. */
  def process[K: KafkaMessageKey, V, A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A]

  /** Evaluates `fa` with receive tracing configured for this chunk of plain records. */
  def receive[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A]

  /** Evaluates `fa` with receive tracing configured for this chunk of committable records. */
  def receiveCommittable[K: KafkaMessageKey, V, A](records: Chunk[CommittableConsumerRecord[F, K, V]])(fa: F[A]): F[A]

}

private[trace] object ConsumerSpans {

  def apply[F[_]: Concurrent: Tracer](
      metadata: ConsumerMetadata.Provider[F],
      config: KafkaTracer.Config
  ): Impl[F] =
    new Impl(metadata, config)

  def noop[F[_]]: ConsumerSpans[F] =
    new Noop

  private final class Noop[F[_]] extends ConsumerSpans[F] {
    override def process[K: KafkaMessageKey, V, A](record: ConsumerRecord[K, V])(fa: F[A]): F[A] = fa
    override def process[K: KafkaMessageKey, V, A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A] = fa
    override def receive[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] = fa
    override def receiveCommittable[K: KafkaMessageKey, V, A](
        records: Chunk[CommittableConsumerRecord[F, K, V]]
    )(fa: F[A]): F[A] = fa
  }

  private[trace] final class Impl[F[_]: Concurrent: Tracer](
      metadata: ConsumerMetadata.Provider[F],
      config: KafkaTracer.Config
  ) extends ConsumerSpans[F] {

    override def receive[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] =
      if (records.isEmpty) fa
      else
        metadata.get.flatMap { resolved =>
          val context = Semconv.receiveSpanContext(records, resolved.clientId, resolved.groupId)
          config.receiveSpanSetup(context).fold(fa) { setup =>
            recordTraceContextLinks(records.toList).flatMap { links =>
              Tracer[F]
                .spanBuilder(setup.spanName)
                .root
                .withSpanKind(SpanKind.Client)
                .withFinalizationStrategy(setup.finalizationStrategy)
                .addAttributes(
                  Semconv.receiveAttributes(context, records) ++ config.constAttributes ++ setup.attributes
                )
                .pipe { builder =>
                  links.foldLeft(builder) { case (acc, (context, attributes)) => acc.addLink(context, attributes) }
                }
                .build
                .surround(fa)
            }
          }
        }

    override def receiveCommittable[K: KafkaMessageKey, V, A](
        records: Chunk[CommittableConsumerRecord[F, K, V]]
    )(fa: F[A]): F[A] = receive(records.map(_.record))(fa)

    override def process[K: KafkaMessageKey, V, A](record: ConsumerRecord[K, V])(fa: F[A]): F[A] =
      metadata.get.flatMap { resolved =>
        val context = Semconv.processSpanContext(record, resolved.clientId, resolved.groupId)
        config.processSpanSetup(context).fold(fa) { setup =>
          recordTraceContextLinks(record :: Nil).flatMap { links =>
            Tracer[F]
              .spanBuilder(setup.spanName)
              .root
              .withSpanKind(SpanKind.Consumer)
              .withFinalizationStrategy(setup.finalizationStrategy)
              .addAttributes(Semconv.processAttributes(context, record) ++ config.constAttributes ++ setup.attributes)
              .pipe { builder =>
                links.foldLeft(builder) { case (acc, (context, attributes)) => acc.addLink(context, attributes) }
              }
              .build
              .surround(fa)
          }
        }
      }

    override def process[K: KafkaMessageKey, V, A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A] =
      process(record.record)(fa)

    private[otel4s] def commit[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] =
      if (records.isEmpty) fa
      else
        metadata.get.flatMap { resolved =>
          val context = Semconv.commitSpanContext(records, resolved.clientId, resolved.groupId)
          config.commitSpanSetup(context).fold(fa) { setup =>
            Tracer[F]
              .spanBuilder(setup.spanName)
              .withSpanKind(SpanKind.Client)
              .withFinalizationStrategy(setup.finalizationStrategy)
              .addAttributes(Semconv.commitAttributes(context, records) ++ config.constAttributes ++ setup.attributes)
              .build
              .surround(fa)
          }
        }

    private def recordTraceContextLinks[K: KafkaMessageKey, V](
        records: Iterable[ConsumerRecord[K, V]]
    ): F[List[(SpanContext, Attributes)]] =
      records.toList.flatTraverse { record =>
        Tracer[F]
          .joinOrRoot(record.headers)(Tracer[F].currentSpanContext)
          .map(_.tupleRight(Semconv.receiveLinkAttributes(record)).toList)
      }

  }

}

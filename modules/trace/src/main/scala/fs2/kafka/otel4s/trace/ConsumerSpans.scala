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
  * Operations use the [[KafkaTracer.Config]] supplied when creating the owning [[KafkaTracer]]:
  *
  *   - [[KafkaTracer.Config.withProcessSpanSetup]] configures both single-record `process` overloads.
  *   - [[KafkaTracer.Config.withBatchProcessSpanSetup]] configures `processBatch`, including singleton batches,
  *     independently of single-record setup.
  *   - [[KafkaTracer.Config.withReceiveSpanSetup]] configures both `receive` and `receiveCommittable`.
  *
  * Each setup function receives operation metadata and can choose the span name, attributes, and finalization strategy.
  * Returning `None` skips span creation and link extraction while still evaluating the supplied effect. Metadata is
  * resolved before setup is evaluated. [[KafkaTracer.Config.withoutProcessSpans]] suppresses both single-record and
  * batch process spans; [[KafkaTracer.Config.withoutReceiveSpans]] suppresses both receive operations. A later setup
  * configuration replaces the corresponding hook and can re-enable that operation.
  *
  * Attributes are applied in order: derived semantic attributes, constant attributes configured with
  * [[KafkaTracer.Config.withConstAttributes]] or [[KafkaTracer.Config.addConstAttributes]], then setup attributes.
  * Later values override earlier values with the same key. [[KafkaTracer.Config.withServerAddress]] contributes
  * constant broker address attributes. Attribute overrides do not change the metadata passed to setup functions.
  *
  * Setup functions do not change span kind, parenting, or links. Process spans remain consumer roots linked to
  * recognized contexts from record headers; receive spans remain client roots linked to contexts from each record in
  * the chunk. [[KafkaTracer.Config.withBatchSpanMode]] controls producer batches and does not affect `processBatch`.
  * Send and commit setup options do not affect these operations; these methods do not publish records or commit
  * offsets.
  *
  * Empty receive and process batches evaluate the effect without metadata lookup, setup evaluation, or span creation.
  * Handles obtained from [[KafkaTracer.noop]] also evaluate effects directly, without metadata lookup or propagation.
  */
sealed trait ConsumerSpans[F[_]] {

  /** Evaluates `fa` with single-record tracing from [[KafkaTracer.Config.withProcessSpanSetup]]. */
  def process[K: KafkaMessageKey, V, A](record: ConsumerRecord[K, V])(fa: F[A]): F[A]

  /** Uses the same single-record setup as `process(record.record)(fa)`; does not commit the record's offset. */
  def process[K: KafkaMessageKey, V, A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A]

  /** Evaluates `fa` inside one root process span linked to every recognized context in the chunk.
    *
    * Empty chunks evaluate `fa` directly without metadata lookup or span setup. Nonempty chunks use
    * [[KafkaTracer.Config.withBatchProcessSpanSetup]], independently of single-record process setup.
    */
  def processBatch[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A]

  /** Evaluates `fa` with receive tracing from [[KafkaTracer.Config.withReceiveSpanSetup]]. */
  def receive[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A]

  /** Uses the same receive setup as `receive(records.map(_.record))(fa)`; does not commit offsets. */
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
    override def processBatch[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] = fa
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

    override def processBatch[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] =
      if (records.isEmpty) fa
      else
        metadata.get.flatMap { resolved =>
          val context = Semconv.batchProcessSpanContext(records, resolved.clientId, resolved.groupId)
          config.batchProcessSpanSetup(context).fold(fa) { setup =>
            recordTraceContextLinks(records.toList).flatMap { links =>
              Tracer[F]
                .spanBuilder(setup.spanName)
                .root
                .withSpanKind(SpanKind.Consumer)
                .withFinalizationStrategy(setup.finalizationStrategy)
                .addAttributes(
                  Semconv.batchProcessAttributes(context, records) ++ config.constAttributes ++ setup.attributes
                )
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

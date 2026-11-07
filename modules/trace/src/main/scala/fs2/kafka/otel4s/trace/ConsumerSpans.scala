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
import cats.effect.Temporal
import cats.syntax.all._
import fs2.{Chunk, Pipe}
import fs2.kafka.{CommittableConsumerRecord, CommittableOffset, CommittableOffsetBatch, ConsumerRecord}
import fs2.kafka.otel4s.trace.instances._
import fs2.kafka.otel4s.trace.internal.Semconv
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.trace.{SpanBuilder, SpanContext, SpanKind, Tracer}

import scala.util.chaining._
import scala.concurrent.duration.FiniteDuration

/** Traces consumer operations using supplied consumer metadata. A Kafka consumer instance is not required.
  *
  * Operations use the [[KafkaTracer.Config]] supplied when creating the owning [[KafkaTracer]]:
  *
  *   - [[KafkaTracer.Config.withProcessSpanSetup]] configures both single-record `process` overloads.
  *   - [[KafkaTracer.Config.withBatchProcessSpanSetup]] configures `processBatch`, including singleton batches,
  *     independently of single-record setup.
  *   - [[KafkaTracer.Config.withReceiveSpanSetup]] configures both `receive` and `receiveCommittable`.
  *   - [[KafkaTracer.Config.withCommitSpanSetup]] configures commits made by the consume helpers, `commitOffsets`, and
  *     `commitBatchWithin`.
  *
  * Each setup function receives operation metadata and can choose the span name, attributes, and finalization strategy.
  * Returning `None` skips span creation and link extraction while still evaluating the supplied effect. Metadata is
  * resolved before setup is evaluated. [[KafkaTracer.Config.withoutProcessSpans]] suppresses both single-record and
  * batch process spans; [[KafkaTracer.Config.withoutReceiveSpans]] suppresses both receive operations. A later setup
  * configuration replaces the corresponding hook and can re-enable that operation.
  * [[KafkaTracer.Config.withoutCommitSpans]] suppresses commit spans.
  *
  * Attributes are applied in order: derived semantic attributes, constant attributes configured with
  * [[KafkaTracer.Config.withConstAttributes]] or [[KafkaTracer.Config.addConstAttributes]], then setup attributes.
  * Later values override earlier values with the same key.
  * [[KafkaTracer.Config.withServerAddress(serverAddress:String,serverPort:Option[Int])*]] or
  * [[KafkaTracer.Config.withServerAddress(bootstrapServers:String)*]] contributes constant broker address attributes.
  * Attribute overrides do not change the metadata passed to setup functions.
  *
  * Setup functions do not change span kind or message links. Process spans are consumer roots linked to recognized
  * contexts from record headers by default. [[KafkaTracer.Config.SpanSetup.Process.relationship]] can make
  * single-record and singleton batch process spans children of the message context while retaining links. Receive spans
  * remain client roots linked to contexts from each record in the chunk. [[KafkaTracer.Config.withBatchSpanMode]]
  * controls producer batches and does not affect `processBatch`. Commit spans have kind `Client` and are always roots,
  * independent of the active span. Send setup options only affect producers. The primitive methods trace supplied
  * effects without calling a committer; the companion's `commitBatchWithin` extension batches offsets and traces the
  * resulting commit calls.
  *
  * Empty receive and process batches and offset maps evaluate the effect without metadata lookup, setup evaluation, or
  * span creation. Handles obtained from [[KafkaTracer.noop]] also evaluate effects directly, without metadata lookup or
  * propagation.
  */
sealed trait ConsumerSpans[F[_]] {

  /** Evaluates `fa` with single-record tracing from [[KafkaTracer.Config.withProcessSpanSetup]].
    *
    * With the corresponding span enabled:
    * {{{
    * Input record: m1
    *
    *                          time -->
    *                     +--------------------------+
    * effect              |            fa            |
    *                     +--------------------------+
    *
    * Span lifetime                                     Controlled by
    * ------------------------------------------------  -------------------------
    * > process (m1)      [=========================]   withProcessSpanSetup
    *
    * No automatic commit.
    * }}}
    *
    * Returning `None` from the setup function removes the span row; `fa` still runs.
    */
  def process[K: KafkaMessageKey, V, A](record: ConsumerRecord[K, V])(fa: F[A]): F[A]

  /** Uses the same single-record setup as `process(record.record)(fa)`; does not commit the record's offset.
    *
    * With the corresponding span enabled:
    * {{{
    * Input record: m1 (committable)
    *
    *                          time -->
    *                     +--------------------------+
    * effect              |            fa            |
    *                     +--------------------------+
    *
    * Span lifetime                                     Controlled by
    * ------------------------------------------------  -------------------------
    * > process (m1)      [=========================]   withProcessSpanSetup
    *
    * No automatic commit.
    * }}}
    *
    * Returning `None` from the setup function removes the span row; `fa` still runs.
    */
  def process[K: KafkaMessageKey, V, A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A]

  /** Evaluates `fa` inside one process span linked to every recognized context in the chunk.
    *
    * Multiple-record batches are roots. Singleton batches follow [[KafkaTracer.Config.SpanSetup.Process.relationship]].
    *
    * Empty chunks evaluate `fa` directly without metadata lookup or span setup. Nonempty chunks use
    * [[KafkaTracer.Config.withBatchProcessSpanSetup]], independently of single-record process setup.
    *
    * With the corresponding span enabled and nonempty input:
    * {{{
    * Input chunk: [m1, m2, m3]
    *
    *                          time -->
    *                     +--------------------------+
    * effect              |            fa            |
    *                     +--------------------------+
    *
    * Span lifetime                                     Controlled by
    * ------------------------------------------------  -------------------------
    * > process batch     [=========================]   withBatchProcessSpanSetup
    *
    * No automatic commit.
    * }}}
    *
    * Returning `None` from the setup function removes the span row; `fa` still runs.
    */
  def processBatch[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A]

  /** Evaluates `fa` with receive tracing from [[KafkaTracer.Config.withReceiveSpanSetup]].
    *
    * With the corresponding span enabled and nonempty input:
    * {{{
    * Input chunk: [m1, m2, m3]
    *
    *                          time -->
    *                     +--------------------------+
    * effect              |            fa            |
    *                     +--------------------------+
    *
    * Span lifetime                                     Controlled by
    * ------------------------------------------------  -------------------------
    * > receive           [=========================]   withReceiveSpanSetup
    *
    * No automatic processing or commit is added.
    * }}}
    *
    * Returning `None` from the setup function removes the span row; `fa` still runs.
    */
  def receive[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A]

  /** Uses the same receive setup as `receive(records.map(_.record))(fa)`; does not commit offsets.
    *
    * With the corresponding span enabled and nonempty input:
    * {{{
    * Input chunk: [m1, m2, m3] (committable)
    *
    *                          time -->
    *                     +--------------------------+
    * effect              |            fa            |
    *                     +--------------------------+
    *
    * Span lifetime                                     Controlled by
    * ------------------------------------------------  -------------------------
    * > receive           [=========================]   withReceiveSpanSetup
    *
    * No automatic processing or commit is added.
    * }}}
    *
    * Returning `None` from the setup function removes the span row; `fa` still runs.
    */
  def receiveCommittable[K: KafkaMessageKey, V, A](records: Chunk[CommittableConsumerRecord[F, K, V]])(fa: F[A]): F[A]

  /** Runs `fa` inside a commit span using [[KafkaTracer.Config.withCommitSpanSetup]].
    *
    * Supply the actual commit effect as `fa`. The map provides topics and partitions; it cannot tell us the record
    * count, record offsets, keys, tombstones, or message links. Empty maps run `fa` without a span or metadata lookup.
    * Commit spans are always roots, independent of the active span.
    */
  def commitOffsets[A](offsets: Map[TopicPartition, OffsetAndMetadata])(fa: F[A]): F[A]

}

object ConsumerSpans {

  /** Adds an explicit traced commit pipe to a consumer span handle.
    *
    * A batch is committed when it reaches `n` offsets or `d` elapses, following `fs2-kafka`'s `commitBatchWithin`
    * behavior. Each original committer receives its own span-wrapped commit call. The wait for a batch does not have a
    * span. Offset notifications do not reveal a safe record count, so commit spans keep `recordCount` unknown. Use
    * offsets produced by the consumer described by this handle's metadata. Each commit span is a root, independent of
    * the active span when its commit call runs. The no-op handle and disabled commit setup still commit offsets
    * normally.
    *
    * This is an explicit traced pipe. The upstream `fs2.kafka.commitBatchWithin` remains available and does not add
    * tracing by itself.
    */
  implicit final class ConsumerSpansSyntax[F[_]](private val spans: ConsumerSpans[F]) extends AnyVal {

    def commitBatchWithin(n: Int, d: FiniteDuration)(implicit
        F: Temporal[F],
        P: Parallel[F]
    ): Pipe[F, CommittableOffset[F], Unit] =
      commitBatches(spans, n, d)

  }

  private[trace] def apply[F[_]: Concurrent: Tracer](
      metadata: ConsumerMetadata.Provider[F],
      config: KafkaTracer.Config
  ): Impl[F] =
    new Impl(metadata, config)

  private[trace] def noop[F[_]]: ConsumerSpans[F] =
    new Noop

  private final class Noop[F[_]] extends ConsumerSpans[F] {
    override def process[K: KafkaMessageKey, V, A](record: ConsumerRecord[K, V])(fa: F[A]): F[A] = fa
    override def process[K: KafkaMessageKey, V, A](record: CommittableConsumerRecord[F, K, V])(fa: F[A]): F[A] = fa
    override def processBatch[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] = fa
    override def receive[K: KafkaMessageKey, V, A](records: Chunk[ConsumerRecord[K, V]])(fa: F[A]): F[A] = fa
    override def receiveCommittable[K: KafkaMessageKey, V, A](
        records: Chunk[CommittableConsumerRecord[F, K, V]]
    )(fa: F[A]): F[A] = fa

    override def commitOffsets[A](offsets: Map[TopicPartition, OffsetAndMetadata])(fa: F[A]): F[A] = fa

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
          processSpanBuilder(setup, record :: Nil).flatMap { builder =>
            builder
              .withSpanKind(SpanKind.Consumer)
              .withFinalizationStrategy(setup.finalizationStrategy)
              .addAttributes(Semconv.processAttributes(context, record) ++ config.constAttributes ++ setup.attributes)
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
            processSpanBuilder(setup, records.toList).flatMap { builder =>
              builder
                .withSpanKind(SpanKind.Consumer)
                .withFinalizationStrategy(setup.finalizationStrategy)
                .addAttributes(
                  Semconv.batchProcessAttributes(context, records) ++ config.constAttributes ++ setup.attributes
                )
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
          commitWithContext(context, Semconv.commitAttributes(context, records))(fa)
        }

    override def commitOffsets[A](offsets: Map[TopicPartition, OffsetAndMetadata])(fa: F[A]): F[A] =
      if (offsets.isEmpty) fa
      else
        metadata.get.flatMap { resolved =>
          val context = Semconv.offsetCommitSpanContext(offsets, resolved.clientId, resolved.groupId)
          commitWithContext(context, Semconv.offsetCommitAttributes(context, offsets))(fa)
        }

    private def commitWithContext[A](context: CommitSpanContext, attributes: => Attributes)(fa: F[A]): F[A] =
      config.commitSpanSetup(context).fold(fa) { setup =>
        Tracer[F]
          .spanBuilder(setup.spanName)
          .root
          .withSpanKind(SpanKind.Client)
          .withFinalizationStrategy(setup.finalizationStrategy)
          .addAttributes(attributes ++ config.constAttributes ++ setup.attributes)
          .build
          .surround(fa)
      }

    private def processSpanBuilder[K: KafkaMessageKey, V](
        setup: KafkaTracer.Config.SpanSetup.Process,
        records: List[ConsumerRecord[K, V]]
    ): F[SpanBuilder[F]] =
      for {
        ambient <- Tracer[F].currentSpanContext
        links <- recordTraceContextLinks(records)
      } yield {
        val linked = links.foldLeft(Tracer[F].spanBuilder(setup.spanName).root) {
          case (builder, (context, attributes)) =>
            builder.addLink(context, attributes)
        }
        if (setup.relationship == ProcessSpanRelationship.ParentFromRecord && records.size == 1)
          links.headOption.fold(linked) { case (parent, _) =>
            val parented = linked.withParent(parent)
            ambient
              .filter { context =>
                context.isValid && (context.traceId != parent.traceId || context.spanId != parent.spanId)
              }
              .fold(parented)(context => parented.addLink(context))
          }
        else linked
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

  private def commitBatches[F[_]: Temporal: Parallel](
      spans: ConsumerSpans[F],
      n: Int,
      d: FiniteDuration
  ): Pipe[F, CommittableOffset[F], Unit] =
    _.groupWithin(n, d).evalMap { chunk =>
      val batch = CommittableOffsetBatch.fromFoldable(chunk)
      batch.offsets.toList.parTraverse_ { case (committer, offsets) =>
        spans.commitOffsets(offsets)(committer.commit(offsets))
      }
    }

}

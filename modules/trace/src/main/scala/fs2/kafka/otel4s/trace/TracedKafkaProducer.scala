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

import cats.{Applicative, Parallel}
import cats.effect.syntax.all._
import cats.effect.{MonadCancelThrow, Outcome, Resource}
import cats.syntax.all._
import fs2.Chunk
import fs2.kafka._
import fs2.kafka.otel4s.trace.instances._
import fs2.kafka.otel4s.trace.internal.{KafkaClientId, Semconv}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerGroupMetadata, OffsetAndMetadata}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.trace._

import scala.util.chaining._

/** A tracing handle bound to a specific [[fs2.kafka.KafkaProducer]].
  *
  * This handle keeps producer-specific tracing helpers explicit while still implementing [[fs2.kafka.KafkaProducer]].
  * The traced `produce` implementation preserves fs2-kafka's original two-stage contract and can create per-record
  * producer spans separately from the Kafka client send operation.
  *
  * A single record without recognized trace context in its headers uses one `PRODUCER` `send` span and propagates that
  * span's context. A single record with trace context in its headers uses a linked `CLIENT` `send` span instead. For
  * batches, [[BatchSpanMode]] controls whether records without trace context receive individual `PRODUCER` `create`
  * spans or share the batch `send` span's context.
  *
  * To ensure emitted `send` spans are finalized, callers should evaluate the returned await effect, for example via
  * `producer.produce(records).flatten`.
  */
trait TracedKafkaProducer[F[_], K, V] extends KafkaProducer.WithSettings[F, K, V] {

  /** Injects the current tracing context into the headers of a single record without producing it.
    *
    * A context is recognized when the configured propagator can extract a usable span context from the record headers.
    * For example, when W3C Trace Context propagation is configured, this requires a structurally valid `traceparent`;
    * an unsampled context is still valid. Other configured propagators, such as B3, may recognize different headers.
    * When duplicate Kafka headers exist for the same propagation key, the last matching header determines the extracted
    * context. Missing, malformed, or null authoritative values are treated as no context.
    *
    * If the record headers already contain recognized trace context, they are preserved as-is. Otherwise the current
    * span context is propagated when one exists. This method does not create a span.
    *
    * Most callers should not call this method before [[produce]], because `produce` automatically injects the
    * appropriate context. Use it only when a specific current context should deliberately become the record trace
    * context before later publication. Doing so intentionally affects span topology: producing the injected record
    * creates a `CLIENT` send span linked to that context, whereas producing the same record directly creates a
    * `PRODUCER` send span and injects the send span's context.
    */
  def injectHeaders(record: ProducerRecord[K, V]): F[ProducerRecord[K, V]]

  /** Injects the current tracing context into the headers of all records in a batch without producing them or creating
    * spans.
    *
    * Most callers should pass the batch directly to [[produce]], which automatically injects an appropriate context for
    * every record. Use this method only when a specific current context should deliberately become the record trace
    * context before later publication.
    *
    * Recognized trace context in the record headers is preserved independently for each record. Other records receive
    * the current span context when one exists. Injecting the same current application context into an otherwise
    * uninstrumented batch means the later batch send links once per record to that shared context and does not
    * synthesize per-record `create` spans. Directly producing that batch in [[BatchSpanMode.PerRecordSpans]] would
    * synthesize a distinct span and trace context for every record without one.
    */
  def injectHeaders(records: ProducerRecords[K, V]): F[ProducerRecords[K, V]]

  /** Produces records using fs2-kafka's two-stage contract and traces record preparation and sending.
    *
    * Producer spans follow these rules:
    *
    *   - empty input delegates to the underlying producer and creates no spans;
    *   - one record without recognized trace context in its headers creates one `PRODUCER` `send <topic>` span, with no
    *     links, and injects that span's context into the record;
    *   - one record with recognized trace context in its headers creates one `CLIENT` `send <topic>` span linked to
    *     that context and preserves the record headers;
    *   - in [[BatchSpanMode.PerRecordSpans]], a batch creates one `CLIENT` send span with one link per record. Each
    *     record without trace context gets a generated `PRODUCER` `create <topic>` span;
    *   - in [[BatchSpanMode.SharedSendSpan]], no `create` spans are generated. Trace context already present in record
    *     headers is preserved and linked, while other records receive the send span's context. The send span is
    *     `PRODUCER` when its context is used by at least one record, otherwise `CLIENT`.
    *
    * The ambient current span remains the normal parent of producer spans. Contexts extracted from record headers are
    * represented as links rather than selected as parents.
    *
    * The outer effect submits the records. The returned effect waits for Kafka completion and finalizes the send span.
    * Callers should normally evaluate both stages with `produce(records).flatten`.
    */
  override def produce(records: ProducerRecords[K, V]): F[F[ProducerResult[K, V]]]

  /** Switches serializers while preserving tracing semantics for the new key type.
    *
    * Prefer this method over [[withSerializers]] when working with traced producers. It requires a new
    * [[KafkaMessageKey]] instance so producer span attributes such as `messaging.kafka.message.key` can continue to be
    * derived safely after the key type changes.
    */
  def tracedWithSerializers[K2: KafkaMessageKey, V2](
      keySerializer: KeySerializer[F, K2],
      valueSerializer: ValueSerializer[F, V2]
  ): TracedKafkaProducer[F, K2, V2]

  /** Switches serializers but drops traced key-attribute derivation for the new key type.
    *
    * This method exists because [[TracedKafkaProducer]] extends fs2-kafka's producer API, but it is not the preferred
    * traced path. After the key type changes, the implementation falls back to a no-op [[KafkaMessageKey]], so spans
    * will continue to be emitted but `messaging.kafka.message.key` will no longer be derived automatically.
    *
    * Use [[tracedWithSerializers]] instead when you want to preserve full traced-producer behavior.
    */
  @deprecated(
    "Use tracedWithSerializers instead. withSerializers keeps tracing but drops KafkaMessageKey-based key extraction for the new key type.",
    since = "0.1"
  )
  override def withSerializers[K2, V2](
      keySerializer: KeySerializer[F, K2],
      valueSerializer: ValueSerializer[F, V2]
  ): KafkaProducer.WithSettings[F, K2, V2]
}

object TracedKafkaProducer {

  /** Creates a no-op traced producer bound to `underlying`.
    *
    * Producer operations are delegated without emitting spans or propagating trace context.
    */
  def noop[F[_]: Applicative, K, V](
      underlying: KafkaProducer.WithSettings[F, K, V]
  ): TracedKafkaProducer[F, K, V] =
    new Noop[F, K, V](underlying)

  final private[otel4s] class Impl[F[_]: MonadCancelThrow: Parallel: Tracer, K: KafkaMessageKey, V](
      underlying: KafkaProducer.WithSettings[F, K, V],
      config: KafkaTracer.Config,
  ) extends TracedKafkaProducer[F, K, V] {

    private val clientId =
      KafkaClientId[F](
        underlying.settings.properties.get(CommonClientConfigs.CLIENT_ID_CONFIG),
        underlying.metrics
      )

    override def injectHeaders(record: ProducerRecord[K, V]): F[ProducerRecord[K, V]] =
      Tracer[F]
        .joinOrRoot(record.headers)(Tracer[F].currentSpanContext)
        .flatMap {
          // if the record headers already have propagated span details, we don't need to inject anything
          case Some(_) =>
            MonadCancelThrow[F].pure(record)

          case None =>
            Tracer[F].propagate(record.headers).map(record.withHeaders)
        }

    override def injectHeaders(records: ProducerRecords[K, V]): F[ProducerRecords[K, V]] =
      records.traverse(injectHeaders)

    override def produce(records: ProducerRecords[K, V]): F[F[ProducerResult[K, V]]] =
      if (records.isEmpty) {
        underlying.produce(records)
      } else {
        clientId.get.flatMap { clientId =>
          prepareBatch(records, clientId).allocatedCase.flatMap { case (prepared, releasePrepared) =>
            val spanContext = Semconv.sendSpanContext(prepared.records, clientId)
            val spanSetup = config.sendSpanSetup(spanContext)

            val span = Tracer[F]
              .spanBuilder(spanSetup.spanName)
              .withSpanKind(prepared.sendKind)
              .withFinalizationStrategy(spanSetup.finalizationStrategy)
              .addAttributes(
                Semconv.sendAttributes(spanContext, prepared.records) ++
                  config.constAttributes ++
                  spanSetup.attributes
              )
              .pipe { builder =>
                prepared.sendLinks
                  .foldLeft(builder) { case (acc, (ctx, attributes)) =>
                    acc.addLink(ctx, attributes)
                  }
              }
              .build

            MonadCancelThrow[F].uncancelable { poll =>
              span.resource.allocatedCase
                .flatMap { case (res, release) =>
                  val outerProduce =
                    if (prepared.injectSendSpanContext)
                      prepared.records
                        .traverse(record => injectHeaders(record))
                        .flatMap(record => underlying.produce(record))
                    else
                      underlying.produce(prepared.records)

                  poll(res.trace(outerProduce))
                    .guaranteeCase {
                      case Outcome.Succeeded(_) =>
                        releasePrepared(Resource.ExitCase.Succeeded)
                      case Outcome.Errored(e) =>
                        releasePrepared(Resource.ExitCase.Errored(e)) *>
                          release(Resource.ExitCase.Errored(e))
                      case Outcome.Canceled() =>
                        releasePrepared(Resource.ExitCase.Canceled) *>
                          release(Resource.ExitCase.Canceled)
                    }
                    .map { awaitResult =>
                      res
                        .trace(awaitResult)
                        .flatTap { result =>
                          res.span.addAttributes(Semconv.sendResultAttributes(result))
                        }
                        .guaranteeCase {
                          case Outcome.Succeeded(_) => release(Resource.ExitCase.Succeeded)
                          case Outcome.Errored(e)   => release(Resource.ExitCase.Errored(e))
                          case Outcome.Canceled()   => release(Resource.ExitCase.Canceled)
                        }
                    }
                }
            }
          }
        }
      }

    override def produceAndCommitTransactionally(
        records: TransactionalProducerRecords[F, K, V]
    ): F[ProducerResult[K, V]] = {
      if (records.isEmpty) {
        underlying.produceAndCommitTransactionally(records)
      } else {
        val grouped =
          records.foldLeft(
            Map.empty[
              KafkaCommitter[F],
              (Map[TopicPartition, OffsetAndMetadata], Chunk[ProducerRecord[K, V]])
            ]
          ) { case (acc, recordBatch) =>
            val offset = recordBatch.offset
            val committer = offset.committer
            val nextOffsets = acc
              .get(committer)
              .map(_._1)
              .getOrElse(Map.empty)
              .updatedWith(offset.topicPartition) {
                case existing @ Some(current) if current.offset >= offset.offsetAndMetadata.offset =>
                  existing
                case Some(_) | None =>
                  Some(offset.offsetAndMetadata)
              }
            val nextRecords =
              acc.get(committer).map(_._2).getOrElse(Chunk.empty) ++ recordBatch.records

            acc.updated(committer, nextOffsets -> nextRecords)
          }

        transaction.surround {
          Chunk
            .from(grouped.toList)
            .parFlatTraverse { case (committer, offsets) =>
              for {
                metadata <- committer.metadata
                result <- produce(offsets._2).flatten
                _ <- sendOffsetsToTransaction(offsets._1, metadata)
              } yield result
            }
        }
      }
    }

    override def sendOffsetsToTransaction(
        offsets: Map[TopicPartition, OffsetAndMetadata],
        groupMetadata: ConsumerGroupMetadata
    ): F[Unit] =
      underlying.sendOffsetsToTransaction(offsets, groupMetadata)

    override def produceTransactionally(records: ProducerRecords[K, V]): F[ProducerResult[K, V]] =
      if (records.isEmpty) {
        underlying.produceTransactionally(records)
      } else {
        transaction.surround(produce(records).flatten)
      }

    override def initTransactions: F[Unit] =
      underlying.initTransactions

    override def transaction: Resource[F, Unit] =
      underlying.transaction

    override def metrics: F[Map[MetricName, Metric]] =
      underlying.metrics

    override def partitionsFor(topic: String): F[List[PartitionInfo]] =
      underlying.partitionsFor(topic)

    override def settings: ProducerSettings[F, K, V] =
      underlying.settings

    override def withSerializers[K2, V2](
        keySerializer: KeySerializer[F, K2],
        valueSerializer: ValueSerializer[F, V2]
    ): KafkaProducer.WithSettings[F, K2, V2] = {
      import KafkaMessageKey.Noop._
      new Impl[F, K2, V2](
        underlying.withSerializers(keySerializer, valueSerializer),
        config
      )
    }

    override def tracedWithSerializers[K2: KafkaMessageKey, V2](
        keySerializer: KeySerializer[F, K2],
        valueSerializer: ValueSerializer[F, V2]
    ): TracedKafkaProducer[F, K2, V2] =
      new Impl[F, K2, V2](
        underlying.withSerializers(keySerializer, valueSerializer),
        config
      )

    /** @param needsSendSpanContextInjection - whether this record needs the eventual `send` span's trace context injected.
      *
      * `true` means the record has no recognized trace context and no dedicated `create` span was synthesized.
      *
      * `false` means the record headers already contain trace context, either from the caller or from a synthesized
      * `create` span during batch preparation.
      */
    private case class PreparedRecord(
        record: ProducerRecord[K, V],
        needsSendSpanContextInjection: Boolean,
        sendLink: Option[(SpanContext, Attributes)]
    )

    private case class PreparedBatch(
        records: ProducerRecords[K, V],
        sendKind: SpanKind,
        sendLinks: List[(SpanContext, Attributes)],
        injectSendSpanContext: Boolean
    )

    private def prepareRecord(
        record: ProducerRecord[K, V],
        createRecordSpan: Boolean,
        clientId: Option[String]
    ): Resource[F, PreparedRecord] =
      Resource
        .eval(Tracer[F].joinOrRoot(record.headers)(Tracer[F].currentSpanContext))
        .flatMap {
          case Some(ctx) =>
            Resource.pure(
              PreparedRecord(
                record = record,
                needsSendSpanContextInjection = false,
                sendLink = Some(ctx -> Semconv.sendLinkAttributes(record))
              )
            )

          case None if createRecordSpan =>
            Tracer[F]
              .spanBuilder(Semconv.createSpanName(record.topic))
              .withSpanKind(SpanKind.Producer)
              .addAttributes(
                Semconv.createAttributes(record, clientId) ++ config.constAttributes
              )
              .build
              .resource
              .evalMap { res =>
                res
                  .trace(injectHeaders(record))
                  .map { injected =>
                    PreparedRecord(
                      record = injected,
                      needsSendSpanContextInjection = false,
                      sendLink = Some(res.span.context -> Semconv.sendLinkAttributes(record))
                    )
                  }
              }

          case None =>
            Resource.pure(
              PreparedRecord(
                record = record,
                needsSendSpanContextInjection = true,
                sendLink = None
              )
            )
        }

    private def prepareBatch(
        records: ProducerRecords[K, V],
        clientId: Option[String]
    ): Resource[F, PreparedBatch] = {
      val useCreateSpans =
        records.size > 1 && config.batchSpanMode == BatchSpanMode.PerRecordSpans

      records.toList
        .traverse(prepareRecord(_, useCreateSpans, clientId))
        .map { prepared =>
          val injectSendSpanContext = prepared.exists(_.needsSendSpanContextInjection)

          PreparedBatch(
            records = ProducerRecords(prepared.map(_.record)),
            sendKind = if (injectSendSpanContext) SpanKind.Producer else SpanKind.Client,
            sendLinks = prepared.flatMap(_.sendLink),
            injectSendSpanContext = injectSendSpanContext
          )
        }
    }
  }

  final private class Noop[F[_]: Applicative, K, V](underlying: KafkaProducer.WithSettings[F, K, V])
      extends TracedKafkaProducer[F, K, V] {

    override def injectHeaders(record: ProducerRecord[K, V]): F[ProducerRecord[K, V]] =
      Applicative[F].pure(record)

    override def injectHeaders(records: ProducerRecords[K, V]): F[ProducerRecords[K, V]] =
      Applicative[F].pure(records)

    override def produce(records: ProducerRecords[K, V]): F[F[ProducerResult[K, V]]] =
      underlying.produce(records)

    override def produceAndCommitTransactionally(
        records: TransactionalProducerRecords[F, K, V]
    ): F[ProducerResult[K, V]] =
      underlying.produceAndCommitTransactionally(records)

    override def sendOffsetsToTransaction(
        offsets: Map[TopicPartition, OffsetAndMetadata],
        groupMetadata: ConsumerGroupMetadata
    ): F[Unit] =
      underlying.sendOffsetsToTransaction(offsets, groupMetadata)

    override def produceTransactionally(records: ProducerRecords[K, V]): F[ProducerResult[K, V]] =
      underlying.produceTransactionally(records)

    override def initTransactions: F[Unit] =
      underlying.initTransactions

    override def transaction: Resource[F, Unit] =
      underlying.transaction

    override def metrics: F[Map[MetricName, Metric]] =
      underlying.metrics

    override def partitionsFor(topic: String): F[List[PartitionInfo]] =
      underlying.partitionsFor(topic)

    override def settings: ProducerSettings[F, K, V] =
      underlying.settings

    override def withSerializers[K2, V2](
        keySerializer: KeySerializer[F, K2],
        valueSerializer: ValueSerializer[F, V2]
    ): KafkaProducer.WithSettings[F, K2, V2] =
      new Noop(underlying.withSerializers(keySerializer, valueSerializer))

    override def tracedWithSerializers[K2: KafkaMessageKey, V2](
        keySerializer: KeySerializer[F, K2],
        valueSerializer: ValueSerializer[F, V2]
    ): TracedKafkaProducer[F, K2, V2] =
      new Noop(underlying.withSerializers(keySerializer, valueSerializer))

  }

}

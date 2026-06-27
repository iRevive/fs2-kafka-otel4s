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
  * The traced `produce` implementation preserves fs2-kafka's original two-stage contract.
  *
  * To ensure emitted `send` spans are finalized, callers should evaluate the returned await effect, for example via
  * `producer.produce(records).flatten`.
  */
trait TracedKafkaProducer[F[_], K, V] extends KafkaProducer.WithSettings[F, K, V] {

  /** Injects the current tracing context into the headers of a single record without producing it.
    *
    * If the record already carries a recognized propagated context, it is preserved as-is. When duplicate Kafka headers
    * exist for the same propagation key, the last matching header determines the extracted context.
    */
  def injectHeaders(record: ProducerRecord[K, V]): F[ProducerRecord[K, V]]

  /** Injects the current tracing context into the headers of all records in a batch without producing them.
    */
  def injectHeaders(records: ProducerRecords[K, V]): F[ProducerRecords[K, V]]

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
                    if (prepared.sendKind == SpanKind.Producer)
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

    /** @param usesSendSpanAsCreationContext - whether this record relies on the eventual `send` span to represent message
      * creation.
      *
      * `true` means the record has no pre-existing creation context and no dedicated `create` span was synthesized, so
      * the batch `send` span itself is the creation context for this record.
      *
      * `false` means the record already has its own creation context, either from propagated headers already on the
      * record or from a synthesized `create` span during batch preparation.
      */
    private case class PreparedRecord(
        record: ProducerRecord[K, V],
        usesSendSpanAsCreationContext: Boolean,
        sendLink: Option[(SpanContext, Attributes)]
    )

    private case class PreparedBatch(
        records: ProducerRecords[K, V],
        sendKind: SpanKind,
        sendLinks: List[(SpanContext, Attributes)]
    )

    private def prepareRecord(
        record: ProducerRecord[K, V],
        createCreationContext: Boolean,
        clientId: Option[String]
    ): Resource[F, PreparedRecord] =
      Resource
        .eval(Tracer[F].joinOrRoot(record.headers)(Tracer[F].currentSpanContext))
        .flatMap {
          case Some(ctx) =>
            Resource.pure(
              PreparedRecord(
                record = record,
                usesSendSpanAsCreationContext = false,
                sendLink = Some(ctx -> Semconv.sendLinkAttributes(record))
              )
            )

          case None if createCreationContext =>
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
                      usesSendSpanAsCreationContext = false,
                      sendLink = Some(res.span.context -> Semconv.sendLinkAttributes(record))
                    )
                  }
              }

          case None =>
            Resource.pure(
              PreparedRecord(
                record = record,
                usesSendSpanAsCreationContext = true,
                sendLink = None
              )
            )
        }

    private def prepareBatch(
        records: ProducerRecords[K, V],
        clientId: Option[String]
    ): Resource[F, PreparedBatch] = {
      val useCreateSpans = records.size > 1

      records.toList
        .traverse(prepareRecord(_, useCreateSpans, clientId))
        .map { prepared =>
          val sendUsesOwnContext = prepared.forall(_.usesSendSpanAsCreationContext)

          PreparedBatch(
            records = ProducerRecords(prepared.map(_.record)),
            sendKind = if (sendUsesOwnContext) SpanKind.Producer else SpanKind.Client,
            sendLinks = prepared.flatMap(_.sendLink)
          )
        }
    }
  }

}

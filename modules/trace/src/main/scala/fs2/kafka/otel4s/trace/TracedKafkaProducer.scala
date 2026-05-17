package fs2.kafka.otel4s.trace

import cats.Parallel
import cats.effect.syntax.all._
import cats.effect.{MonadCancelThrow, Outcome, Resource}
import cats.syntax.all._
import fs2.Chunk
import fs2.kafka._
import fs2.kafka.otel4s.trace.instances._
import fs2.kafka.otel4s.trace.internal.Semconv
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

  def tracedWithSerializers[K2: KafkaMessageKey, V2](
      keySerializer: KeySerializer[F, K2],
      valueSerializer: ValueSerializer[F, V2]
  ): TracedKafkaProducer[F, K2, V2]
}

object TracedKafkaProducer {

  final private[otel4s] class Impl[F[_]: MonadCancelThrow: Parallel: Tracer, K: KafkaMessageKey, V](
      underlying: KafkaProducer.WithSettings[F, K, V],
      config: KafkaTracer.Config,
  ) extends TracedKafkaProducer[F, K, V] {

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
        prepareBatch(records).flatMap { prepared =>
          val spanContext = Semconv.sendSpanContext(underlying.settings, prepared.records)
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
                      .flatMap(underlying.produce(_))
                  else
                    underlying.produce(prepared.records)

                poll(res.trace(outerProduce))
                  .guaranteeCase {
                    case Outcome.Succeeded(_) =>
                      prepared.releaseCreateSpans(Resource.ExitCase.Succeeded)
                    case Outcome.Errored(e) =>
                      prepared.releaseCreateSpans(Resource.ExitCase.Errored(e)) *>
                        release(Resource.ExitCase.Errored(e))
                    case Outcome.Canceled() =>
                      prepared.releaseCreateSpans(Resource.ExitCase.Canceled) *>
                        release(Resource.ExitCase.Canceled)
                  }
                  .map { awaitResult =>
                    MonadCancelThrow[F].guaranteeCase(
                      res
                        .trace(awaitResult)
                        .flatTap { result =>
                          res.span.addAttributes(Semconv.sendResultAttributes(result))
                        }
                    ) {
                      case Outcome.Succeeded(_) => release(Resource.ExitCase.Succeeded)
                      case Outcome.Errored(e)   => release(Resource.ExitCase.Errored(e))
                      case Outcome.Canceled()   => release(Resource.ExitCase.Canceled)
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

    // todo: add test that where the key type changes
    override def tracedWithSerializers[K2: KafkaMessageKey, V2](
        keySerializer: KeySerializer[F, K2],
        valueSerializer: ValueSerializer[F, V2]
    ): TracedKafkaProducer[F, K2, V2] =
      new Impl[F, K2, V2](
        underlying.withSerializers(keySerializer, valueSerializer),
        config
      )

    private case class PreparedRecord(
        record: ProducerRecord[K, V],
        usesSendSpanAsCreationContext: Boolean,
        sendLink: Option[(SpanContext, Attributes)],
        releaseCreateSpan: Option[Resource.ExitCase => F[Unit]]
    )

    private case class PreparedBatch(
        records: ProducerRecords[K, V],
        sendKind: SpanKind,
        sendLinks: List[(SpanContext, Attributes)],
        releaseCreateSpans: Resource.ExitCase => F[Unit]
    )

    private def prepareRecord(
        record: ProducerRecord[K, V],
        createCreationContext: Boolean
    ): F[PreparedRecord] =
      Tracer[F]
        .joinOrRoot(record.headers)(Tracer[F].currentSpanContext)
        .flatMap {
          case Some(ctx) =>
            MonadCancelThrow[F].pure(
              PreparedRecord(
                record = record,
                usesSendSpanAsCreationContext = false,
                sendLink = Some(ctx -> Semconv.sendLinkAttributes(record)),
                releaseCreateSpan = None
              )
            )

          case None if createCreationContext =>
            Tracer[F]
              .spanBuilder(Semconv.createSpanName(record.topic))
              .withSpanKind(SpanKind.Producer)
              .addAttributes(
                Semconv.createAttributes(underlying.settings, record) ++ config.constAttributes
              )
              .build
              .resource
              .allocatedCase
              .flatMap { case (res, release) =>
                res
                  .trace(injectHeaders(record))
                  .map { injected =>
                    PreparedRecord(
                      record = injected,
                      usesSendSpanAsCreationContext = false,
                      sendLink = Some(res.span.context -> Semconv.sendLinkAttributes(record)),
                      releaseCreateSpan = Some(release)
                    )
                  }
              }

          case None =>
            MonadCancelThrow[F].pure(
              PreparedRecord(
                record = record,
                usesSendSpanAsCreationContext = true,
                sendLink = None,
                releaseCreateSpan = None
              )
            )
        }

    private def prepareBatch(records: ProducerRecords[K, V]): F[PreparedBatch] = {
      val useCreateSpans = records.size > 1

      records.toList
        .traverse(prepareRecord(_, useCreateSpans))
        .map { prepared =>
          val sendUsesOwnContext = prepared.forall(_.usesSendSpanAsCreationContext)
          val releaseCreateSpans = (exitCase: Resource.ExitCase) =>
            prepared.reverse
              .traverse_(_.releaseCreateSpan.fold(MonadCancelThrow[F].unit)(_(exitCase)))

          PreparedBatch(
            records = ProducerRecords(prepared.map(_.record)),
            sendKind = if (sendUsesOwnContext) SpanKind.Producer else SpanKind.Client,
            sendLinks = prepared.flatMap(_.sendLink),
            releaseCreateSpans = releaseCreateSpans
          )
        }
    }
  }

}

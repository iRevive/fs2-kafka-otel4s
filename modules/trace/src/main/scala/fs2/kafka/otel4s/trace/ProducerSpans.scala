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

import cats.effect.{MonadCancelThrow, Outcome, Resource}
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.kafka._
import fs2.kafka.otel4s.trace.instances._
import fs2.kafka.otel4s.trace.internal.Semconv
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.trace._

import scala.util.chaining._

/** Configured producer tracing operations independent of a live Kafka producer.
  *
  * Uses the same span policy, batch mode, header preservation, and result attributes as [[TracedKafkaProducer]].
  * Suppressing send spans retains header propagation and configured per-record create spans. A noop handle calls the
  * callback directly without propagation or metadata lookup.
  */
sealed trait ProducerSpans[F[_]] {

  /** Prepares records and submits them through `send`, which must publish the supplied prepared records.
    *
    * The outer effect submits records; the returned acknowledgment effect completes and finalizes the send span.
    * Evaluate both stages, normally with `.flatten`. Abandoning the acknowledgment effect leaves the send span open.
    * Empty input still calls `send`, without creating spans or resolving metadata. Each invocation resolves metadata
    * through the supplied provider. Transaction orchestration belongs to the caller or [[TracedKafkaProducer]].
    */
  def produce[K: KafkaMessageKey, V](records: ProducerRecords[K, V])(
      send: ProducerRecords[K, V] => F[F[ProducerResult[K, V]]]
  ): F[F[ProducerResult[K, V]]]

}

private[trace] object ProducerSpans {

  def apply[F[_]: MonadCancelThrow: Tracer](
      metadata: ProducerMetadata.Provider[F],
      config: KafkaTracer.Config
  ): Impl[F] = new Impl(metadata, config)

  def noop[F[_]]: ProducerSpans[F] = new Noop[F]

  private final class Noop[F[_]] extends ProducerSpans[F] {
    override def produce[K: KafkaMessageKey, V](records: ProducerRecords[K, V])(
        send: ProducerRecords[K, V] => F[F[ProducerResult[K, V]]]
    ): F[F[ProducerResult[K, V]]] = send(records)
  }

  private[trace] final class Impl[F[_]: MonadCancelThrow: Tracer](
      metadata: ProducerMetadata.Provider[F],
      config: KafkaTracer.Config
  ) extends ProducerSpans[F] {

    private[trace] def injectHeaders[K, V](record: ProducerRecord[K, V]): F[ProducerRecord[K, V]] =
      Tracer[F]
        .joinOrRoot(record.headers)(Tracer[F].currentSpanContext)
        .flatMap {
          // if the record headers already have propagated span details, we don't need to inject anything
          case Some(_) =>
            MonadCancelThrow[F].pure(record)

          case None =>
            Tracer[F].propagate(record.headers).map(record.withHeaders)
        }

    private[trace] def injectHeaders[K, V](records: ProducerRecords[K, V]): F[ProducerRecords[K, V]] =
      records.traverse(record => injectHeaders(record))

    override def produce[K: KafkaMessageKey, V](records: ProducerRecords[K, V])(
        send: ProducerRecords[K, V] => F[F[ProducerResult[K, V]]]
    ): F[F[ProducerResult[K, V]]] =
      if (records.isEmpty) {
        send(records)
      } else {
        metadata.get.flatMap { resolved =>
          val clientId = resolved.clientId
          prepareBatch(records, clientId).use { prepared =>
            val spanContext = Semconv.sendSpanContext(prepared.records, clientId)
            val spanSetup = config.sendSpanSetup(spanContext)

            spanSetup.fold(producePrepared(prepared)(send)) { setup =>
              val span = Tracer[F]
                .spanBuilder(setup.spanName)
                .withSpanKind(prepared.sendKind)
                .withFinalizationStrategy(setup.finalizationStrategy)
                .addAttributes(
                  Semconv.sendAttributes(spanContext, prepared.records) ++
                    config.constAttributes ++
                    setup.attributes
                )
                .pipe { builder =>
                  prepared.sendLinks.foldLeft(builder) { case (acc, (ctx, attributes)) =>
                    acc.addLink(ctx, attributes)
                  }
                }
                .build

              MonadCancelThrow[F].uncancelable { poll =>
                span.resource.allocatedCase
                  .flatMap { case (res, release) =>
                    poll(res.trace(producePrepared(prepared)(send)))
                      .guaranteeCase {
                        case Outcome.Succeeded(_) => MonadCancelThrow[F].unit
                        case Outcome.Errored(e)   => release(Resource.ExitCase.Errored(e))
                        case Outcome.Canceled()   => release(Resource.ExitCase.Canceled)
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
      }

    private def producePrepared[K, V](prepared: PreparedBatch[K, V])(
        send: ProducerRecords[K, V] => F[F[ProducerResult[K, V]]]
    ): F[F[ProducerResult[K, V]]] =
      if (prepared.injectSendSpanContext)
        prepared.records
          .traverse(record => injectHeaders(record))
          .flatMap(record => send(record))
      else
        send(prepared.records)

    /** @param needsSendSpanContextInjection - whether this record needs the eventual `send` span's trace context injected.
      *
      * `true` means the record has no recognized trace context and no dedicated `create` span was synthesized.
      *
      * `false` means the record headers already contain trace context, either from the caller or from a synthesized
      * `create` span during batch preparation.
      */
    private case class PreparedRecord[K, V](
        record: ProducerRecord[K, V],
        needsSendSpanContextInjection: Boolean,
        sendLink: Option[(SpanContext, Attributes)]
    )

    private case class PreparedBatch[K, V](
        records: ProducerRecords[K, V],
        sendKind: SpanKind,
        sendLinks: List[(SpanContext, Attributes)],
        injectSendSpanContext: Boolean
    )

    private def prepareRecord[K: KafkaMessageKey, V](
        record: ProducerRecord[K, V],
        createRecordSpan: Boolean,
        clientId: Option[String]
    ): Resource[F, PreparedRecord[K, V]] =
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

    private def prepareBatch[K: KafkaMessageKey, V](
        records: ProducerRecords[K, V],
        clientId: Option[String]
    ): Resource[F, PreparedBatch[K, V]] = {
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

}

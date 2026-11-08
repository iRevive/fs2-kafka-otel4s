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

import cats.effect.{Deferred, IO, Outcome, Ref, Resource}
import cats.syntax.all._
import fs2.Chunk
import fs2.kafka._
import io.opentelemetry.api.baggage.Baggage
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.context.ContextManager
import org.typelevel.otel4s.oteljava.context.Context
import org.typelevel.otel4s.oteljava.testkit.OtelJavaTestkit
import org.typelevel.otel4s.oteljava.testkit.trace.{
  SpanExpectation,
  StatusExpectation,
  TraceExpectation,
  TraceForestExpectation
}
import org.typelevel.otel4s.trace.{Tracer, TracerProvider}

@scala.annotation.nowarn("msg=method withSerializers in trait TracedKafkaProducer is deprecated")
final class ProducerAcknowledgmentContextSuite extends KafkaTracingTestSupport {

  private val records = ProducerRecords.one(ProducerRecord("orders", "key", "value"))
  private val failure = new RuntimeException("acknowledgment failed")

  // Emit observations as child spans so the expectation API checks context values and exact parentage together.
  private def observe(
      testkit: OtelJavaTestkit[IO],
      tracer: Tracer[IO],
      key: Context.Key[String],
      name: String
  ): IO[Unit] =
    testkit.contextManager.current.flatMap { context =>
      tracer
        .spanBuilder(name)
        .addAttributes(
          Attribute(
            "test.baggage",
            Option(Baggage.fromContext(context.underlying).getEntryValue("tenant")).getOrElse("")
          ),
          Attribute("test.custom", context.get(key).getOrElse(""))
        )
        .build
        .surround(IO.unit)
    }

  private def observation(name: String, value: String): TraceExpectation =
    TraceExpectation.leaf(
      SpanExpectation
        .internal(name)
        .scopeName("app")
        .status(StatusExpectation.unset)
        .attributesSubset(Attribute("test.baggage", value), Attribute("test.custom", value))
    )

  private def withValues[A](testkit: OtelJavaTestkit[IO], key: Context.Key[String], value: String)(fa: IO[A]): IO[A] =
    testkit.contextManager.current.flatMap { current =>
      val baggage = Baggage.builder().put("tenant", value).build()
      val context = Context.wrap(baggage.storeInContext(current.underlying)).updated(key, value)
      testkit.contextManager.scope(context)(fa)
    }

  private def recordingProducer[K, V](
      producerSettings: ProducerSettings[IO, K, V],
      send: IO[IO[Unit]]
  ): KafkaProducer.WithSettings[IO, K, V] =
    new StubKafkaProducer[K, V](producerSettings) {
      override def produce(records: ProducerRecords[K, V]): IO[IO[ProducerResult[K, V]]] =
        send.map(_.as(Chunk.empty))

      override def withSerializers[K2, V2](
          keySerializer: KeySerializer[IO, K2],
          valueSerializer: ValueSerializer[IO, V2]
      ): KafkaProducer.WithSettings[IO, K2, V2] =
        recordingProducer(
          producerSettings.withSerializers(Resource.pure(keySerializer), Resource.pure(valueSerializer)),
          send
        )
    }

  for {
    sendEnabled <- List(true, false)
    completion <- List("success", "failure", "cancellation")
    bound <- List(false, true)
  } {
    test(s"full context survives deferred $completion with sendEnabled=$sendEnabled, bound=$bound") {
      OtelJavaTestkit.inMemory[IO]().use { testkit =>
        implicit val tp: TracerProvider[IO] = testkit.tracerProvider
        implicit val contextManager: ContextManager[IO] = testkit.contextManager
        for {
          app <- tp.get("app")
          key <- Context.Key.unique[IO, String]("custom")
          started <- Deferred[IO, Unit]
          config = if (sendEnabled) KafkaTracer.Config.default else KafkaTracer.Config.default.withoutSendSpans
          kafka <- KafkaTracer.resource[IO](config).use(IO.pure(_))
          acknowledge = (observe(testkit, app, key, "acknowledged") *>
            started.complete(()) *> (completion match {
              case "failure"      => IO.raiseError[Unit](failure)
              case "cancellation" => IO.never[Unit]
              case _              => IO.unit
            })).guarantee(observe(testkit, app, key, "ack-finalizer"))
          send = observe(testkit, app, key, "submitted").as(acknowledge)
          await <- withValues(testkit, key, "submission") {
            app.span("submission").surround {
              if (bound) {
                val producer = kafka
                  .producer(recordingProducer(ProducerSettings[IO, String, String], send))
                  .tracedWithSerializers(Serializer[IO, String], Serializer[IO, String])
                  .withSerializers(Serializer[IO, String], Serializer[IO, String])
                producer.produce(records)
              } else {
                kafka
                  .producerSpans(ProducerMetadata.Provider.constant[IO](ProducerMetadata(None)))
                  .produce(records)(_ => send.map(_.as(Chunk.empty)))
              }
            }
          }
          _ <- withValues(testkit, key, "caller") {
            app.span("caller").surround {
              val operation = await.void.guarantee(observe(testkit, app, key, "caller-restored"))
              val run =
                if (completion == "cancellation")
                  operation.start.flatMap { fiber =>
                    started.get *> fiber.cancel *> fiber.join.flatMap {
                      case Outcome.Canceled()   => observe(testkit, app, key, "result-cancellation")
                      case Outcome.Errored(_)   => observe(testkit, app, key, "result-failure")
                      case Outcome.Succeeded(_) => observe(testkit, app, key, "result-success")
                    }
                  }
                else
                  operation.attempt.flatMap {
                    case Left(error) => observe(testkit, app, key, s"result-failure: ${error.getMessage}")
                    case Right(_)    => observe(testkit, app, key, "result-success")
                  }
              observe(testkit, app, key, "caller-before") *> run *> observe(testkit, app, key, "caller-after")
            }
          }
          spans <- testkit.finishedSpans
        } yield {
          val observations = List(
            observation("submitted", "submission"),
            observation("acknowledged", "submission"),
            observation("ack-finalizer", "submission")
          )
          val sendStatus = completion match {
            case "failure"      => StatusExpectation.error.description(failure.getMessage)
            case "cancellation" => StatusExpectation.error.description("canceled")
            case _              => StatusExpectation.unset
          }
          val submissionChildren =
            if (sendEnabled)
              List(
                TraceExpectation.ordered(
                  SpanExpectation.producer("send orders").scopeName("fs2.kafka").status(sendStatus),
                  observations *
                )
              )
            else observations
          val result = if (completion == "failure") s"result-failure: ${failure.getMessage}" else s"result-$completion"
          assertExpected(
            spans,
            TraceForestExpectation.unordered(
              root(SpanExpectation.internal("submission").status(StatusExpectation.unset), submissionChildren *),
              root(
                SpanExpectation.internal("caller").status(StatusExpectation.unset),
                observation("caller-before", "caller"),
                observation("caller-restored", "caller"),
                observation(result, "caller"),
                observation("caller-after", "caller")
              )
            )
          )
        }
      }
    }
  }

  test("concurrent acknowledgments retain distinct submission contexts") {
    OtelJavaTestkit.inMemory[IO]().use { testkit =>
      implicit val tp: TracerProvider[IO] = testkit.tracerProvider
      implicit val contextManager: ContextManager[IO] = testkit.contextManager
      for {
        app <- tp.get("app")
        key <- Context.Key.unique[IO, String]("custom")
        kafka <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        spans = kafka.producerSpans(ProducerMetadata.Provider.constant[IO](ProducerMetadata(None)))
        arrived <- Ref.of[IO, Int](0)
        bothStarted <- Deferred[IO, Unit]
        acks <- List("first", "second").traverse { value =>
          withValues(testkit, key, value) {
            app.span(value).surround {
              spans.produce(records) { _ =>
                observe(testkit, app, key, "submitted").as(
                  (arrived.updateAndGet(_ + 1).flatMap(n => if (n == 2) bothStarted.complete(()).void else IO.unit) *>
                    bothStarted.get *> IO.cede *> observe(testkit, app, key, "acknowledged")).as(Chunk.empty)
                )
              }
            }
          }
        }
        _ <- withValues(testkit, key, "caller") {
          app.span("caller").surround {
            observe(testkit, app, key, "caller-before") *> acks.parSequence *>
              observe(testkit, app, key, "caller-after")
          }
        }
        finished <- testkit.finishedSpans
      } yield assertExpected(
        finished,
        TraceForestExpectation.unordered(
          root(
            SpanExpectation.internal("first"),
            TraceExpectation.ordered(
              SpanExpectation.producer("send orders").status(StatusExpectation.unset),
              observation("submitted", "first"),
              observation("acknowledged", "first")
            )
          ),
          root(
            SpanExpectation.internal("second"),
            TraceExpectation.ordered(
              SpanExpectation.producer("send orders").status(StatusExpectation.unset),
              observation("submitted", "second"),
              observation("acknowledged", "second")
            )
          ),
          root(
            SpanExpectation.internal("caller"),
            observation("caller-before", "caller"),
            observation("caller-after", "caller")
          )
        )
      )
    }
  }

  test("empty and noop producers leave deferred acknowledgment in the caller context") {
    OtelJavaTestkit.inMemory[IO]().use { testkit =>
      implicit val tp: TracerProvider[IO] = testkit.tracerProvider
      implicit val contextManager: ContextManager[IO] = testkit.contextManager
      for {
        app <- tp.get("app")
        key <- Context.Key.unique[IO, String]("custom")
        kafka <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        metadata = new ProducerMetadata.Provider[IO] {
          def get: IO[ProducerMetadata] = IO.raiseError(new AssertionError("metadata must not be read"))
        }
        _ <- List(kafka -> Chunk.empty[ProducerRecord[String, String]], KafkaTracer.noop[IO] -> records).traverse_ {
          case (handle, input) =>
            for {
              await <- withValues(testkit, key, "submission") {
                app.span("submission").surround {
                  handle.producerSpans(metadata).produce(input) { _ =>
                    IO.pure(observe(testkit, app, key, "acknowledged").as(Chunk.empty))
                  }
                }
              }
              _ <- withValues(testkit, key, "caller") {
                app.span("caller").surround {
                  await *> observe(testkit, app, key, "caller-after")
                }
              }
            } yield ()
        }
        spans <- testkit.finishedSpans
      } yield {
        val submission = root(SpanExpectation.internal("submission").status(StatusExpectation.unset))
        val caller = root(
          SpanExpectation.internal("caller").status(StatusExpectation.unset),
          observation("acknowledged", "caller"),
          observation("caller-after", "caller")
        )
        assertExpected(spans, TraceForestExpectation.unordered(submission, caller, submission, caller))
      }
    }
  }
}

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

import cats.effect.{Deferred, IO, Ref}
import fs2.Chunk
import fs2.kafka._
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.typelevel.otel4s.oteljava.testkit.trace.{SpanExpectation, StatusExpectation, TraceForestExpectation}
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes
import org.typelevel.otel4s.trace.TracerProvider

final class ProducerSpansSuite extends KafkaTracingTestSupport {

  private def sendSpan = SpanExpectation
    .producer("send orders")
    .scopeName("fs2.kafka")
    .attributesSubset(
      MessagingExperimentalAttributes.MessagingClientId("explicit-client")
    )

  private def createSpan = root(
    SpanExpectation
      .producer("create orders")
      .scopeName("fs2.kafka")
      .attributesSubset(
        MessagingExperimentalAttributes.MessagingClientId("explicit-client")
      )
  )

  private def provider = ProducerMetadata.Provider.constant[IO](ProducerMetadata(Some("explicit-client")))
  private val records = ProducerRecords.one(ProducerRecord("orders", "key", "value"))

  for (mode <- List(BatchSpanMode.PerRecordSpans, BatchSpanMode.SharedSendSpan)) {
    test(s"client-free $mode sends prepared records and preserves two-stage completion") {
      KafkaTracerTestkit.create().use { testkit =>
        implicit val tp: TracerProvider[IO] = testkit.tracerProvider
        for {
          tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default.withBatchSpanMode(mode))
          completed <- Ref[IO].of(false)
          spans = tracer.producerSpans(provider)
          await <- spans.produce(records ++ records) { prepared =>
            IO {
              assertEquals(prepared.size, 2)
              assert(prepared.forall(_.headers.exists("traceparent")))
            }.as(
              completed
                .set(true)
                .as(Chunk.empty[(ProducerRecord[String, String], RecordMetadata)])
            )
          }
          before <- testkit.finishedSpans
          isCompleted <- completed.get
          _ <- IO {
            assert(!isCompleted)
            assertExpected(
              before,
              if (mode == BatchSpanMode.PerRecordSpans)
                TraceForestExpectation.unordered(createSpan, createSpan)
              else TraceForestExpectation.empty
            )
          }
          _ <- await
          after <- testkit.finishedSpans
        } yield {
          assertExpected(
            after,
            if (mode == BatchSpanMode.PerRecordSpans)
              TraceForestExpectation.unordered(
                createSpan,
                createSpan,
                root(
                  SpanExpectation
                    .client("send orders")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingClientId("explicit-client")
                    )
                )
              )
            else TraceForestExpectation.unordered(root(sendSpan))
          )
        }
      }
    }
  }

  test("one handle supports different key types and attaches broker result attributes") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tp: TracerProvider[IO] = testkit.tracerProvider
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        spans = tracer.producerSpans(provider)
        first <- spans.produce(records)(StubKafkaProducer.metadataOnly[String, String]().produce).flatten
        _ <- spans
          .produce(ProducerRecords.one(ProducerRecord("orders", 1, "value")))(
            StubKafkaProducer.metadataOnly[Int, String]().produce
          )
          .flatten
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(first.size, 1)
        val expected = root(
          sendSpan.attributesSubset(
            MessagingExperimentalAttributes.MessagingClientId("explicit-client"),
            MessagingExperimentalAttributes.MessagingKafkaOffset(42L)
          )
        )
        assertExpected(finished, TraceForestExpectation.unordered(expected, expected))
      }
    }
  }

  for {
    partition <- List(RecordMetadata.UNKNOWN_PARTITION, 0)
    offset <- List(-1L, 0L, 42L)
    count <- List(1, 2)
  } {
    test(s"result enrichment with partition=$partition offset=$offset count=$count") {
      KafkaTracerTestkit.create().use { testkit =>
        implicit val tp: TracerProvider[IO] = testkit.tracerProvider
        val metadata = new RecordMetadata(new TopicPartition("orders", partition), offset, 0, -1L, 0, 0)
        for {
          tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default.withBatchSpanMode(BatchSpanMode.SharedSendSpan))
          _ <- tracer
            .producerSpans(provider)
            .produce(Chunk.from(List.fill(count)(ProducerRecord("orders", "key", "value"))))(
              StubKafkaProducer.withResultMetadata[String, String](metadata).produce
            )
            .flatten
          finished <- testkit.finishedSpans
        } yield {
          assertEquals(finished.size, 1)
          val attributes = finished.head.getAttributes
          assertEquals(
            Option(
              attributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.destination.partition.id"))
            ),
            Option.when(count == 1 && partition >= 0)(partition.toString)
          )
          assertEquals(
            Option(attributes.get(io.opentelemetry.api.common.AttributeKey.longKey("messaging.kafka.offset")))
              .map(_.longValue),
            Option.when(count == 1 && offset >= 0)(offset)
          )
        }
      }
    }
  }

  test("empty and noop operations pass original records through without resolving metadata") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tp: TracerProvider[IO] = testkit.tracerProvider
      val failingProvider = new ProducerMetadata.Provider[IO] {
        def get: IO[ProducerMetadata] = IO.raiseError(new AssertionError("metadata lookup"))
      }
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        calls <- Ref[IO].of(0)
        _ <- tracer
          .producerSpans(failingProvider)
          .produce(Chunk.empty[ProducerRecord[String, String]]) { prepared =>
            IO(assert(prepared.isEmpty)) *> calls.update(_ + 1).as(IO.pure(Chunk.empty))
          }
          .flatten
        _ <- KafkaTracer
          .noop[IO]
          .producerSpans(failingProvider)
          .produce(records) { prepared =>
            IO(assertEquals(prepared, records)) *> calls.update(_ + 1).as(IO.pure(Chunk.empty))
          }
          .flatten
        count <- calls.get
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(count, 2)
        assertEquals(finished, Nil)
      }
    }
  }

  test("suppression still injects ambient context and preserves recognized record headers") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tp: TracerProvider[IO] = testkit.tracerProvider
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default.withoutSendSpans)
        prepared <- testkit.appTracer.span("app").surround {
          tracer
            .producerSpans(provider)
            .produce(records) { prepared =>
              IO.pure(
                IO.pure(
                  prepared.map(
                    _ -> new RecordMetadata(
                      new TopicPartition("orders", 0),
                      42L,
                      0,
                      0L,
                      0,
                      0
                    )
                  )
                )
              )
            }
            .flatten
        }
        original = prepared.map(_._1)
        _ <- tracer
          .producerSpans(provider)
          .produce(original) { next =>
            IO(assertEquals(next, original)).as(IO.pure(Chunk.empty))
          }
          .flatten
        finished <- testkit.finishedSpans
      } yield assertExpected(finished, TraceForestExpectation.unordered(root(SpanExpectation.internal("app"))))
    }
  }

  for (ackStage <- List(false, true)) {
    test(s"failure finalizes once at acknowledgment stage = $ackStage") {
      KafkaTracerTestkit.create().use { testkit =>
        implicit val tp: TracerProvider[IO] = testkit.tracerProvider
        val failure = new RuntimeException("send failed")
        for {
          tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
          result <- tracer
            .producerSpans(provider)
            .produce(records) { _ =>
              if (ackStage) IO.pure(IO.raiseError[ProducerResult[String, String]](failure))
              else IO.raiseError[IO[ProducerResult[String, String]]](failure)
            }
            .flatten
            .attempt
          finished <- testkit.finishedSpans
        } yield {
          assertEquals(result, Left(failure))
          assertExpected(finished, TraceForestExpectation.unordered(root(sendSpan.status(StatusExpectation.error))))
        }
      }
    }

    test(s"cancellation finalizes once at acknowledgment stage = $ackStage") {
      KafkaTracerTestkit.create().use { testkit =>
        implicit val tp: TracerProvider[IO] = testkit.tracerProvider
        for {
          tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
          started <- Deferred[IO, Unit]
          pending = started.complete(()) *> IO.never[ProducerResult[String, String]]
          fiber <- tracer
            .producerSpans(provider)
            .produce(records) { _ =>
              if (ackStage) IO.pure(pending) else pending.map(IO.pure(_))
            }
            .flatten
            .start
          _ <- started.get
          _ <- fiber.cancel
          finished <- testkit.finishedSpans
        } yield assertExpected(
          finished,
          TraceForestExpectation.unordered(root(sendSpan.status(StatusExpectation.error)))
        )
      }
    }
  }
}

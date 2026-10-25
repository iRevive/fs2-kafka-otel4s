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
import io.opentelemetry.api.common.AttributeKey
import org.typelevel.otel4s.oteljava.testkit.trace.{SpanExpectation, TraceForestExpectation}
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.oteljava.testkit.trace.{
  LinkExpectation,
  LinkSetExpectation,
  SpanContextExpectation,
  TraceExpectation
}
import org.typelevel.otel4s.trace.Tracer
import scala.jdk.CollectionConverters._

final class ConsumerSpansSuite extends KafkaTracingTestSupport {

  test("explicit and consumer-bound spans preserve policy, precedence, and extracted links") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      import testkit.appTracer

      val metadata = ConsumerMetadata(Some("orders-client"), Some("orders-group"))
      val consumer = StubKafkaConsumer.metadataOnly[String, String]("orders-client", "orders-group")
      val config =
        KafkaTracer.Config.default
          .withConstAttributes(Attributes(MessagingExperimentalAttributes.MessagingClientId("constant-client")))
          .withProcessSpanSetup { context =>
            Some(
              KafkaTracer.Config.SpanSetup.Process(
                s"custom-process ${context.topic}",
                Attributes(
                  MessagingExperimentalAttributes.MessagingClientId("setup-client"),
                  MessagingExperimentalAttributes.MessagingOperationName("custom-process")
                ),
                KafkaTracer.Config.Defaults.spanFinalizationStrategy
              )
            )
          }

      for {
        producer <- testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer-client"))
        produced <- appTracer.rootSpan("producer-root").surround {
          for {
            span <- Tracer[IO].currentSpanOrThrow
            record <- producer.injectHeaders(ProducerRecord("orders", "order-1", "payload"))
          } yield (span.context, record)
        }
        (producerContext, propagated) = produced
        record = ConsumerRecord("orders", 2, 17L, "order-1", "payload").withHeaders(propagated.headers)
        committable = StubKafkaConsumer.committableRecord(record)
        tracer <- KafkaTracer.create[IO](config)
        boundConsumer = tracer.consumer(consumer)
        bound = boundConsumer.spans
        explicit = tracer.consumerSpans(provider(metadata))
        _ <- bound.receive(Chunk.singleton(record))(IO.unit)
        _ <- explicit.receive(Chunk.singleton(record))(IO.unit)
        _ <- bound.receiveCommittable(Chunk.singleton(committable))(IO.unit)
        _ <- explicit.receiveCommittable(Chunk.singleton(committable))(IO.unit)
        _ <- bound.process(record)(IO.unit)
        _ <- explicit.process(record)(IO.unit)
        _ <- bound.process(committable)(IO.unit)
        _ <- explicit.process(committable)(IO.unit)
        spans <- testkit.finishedSpans
        _ <- IO {
          val expectedProcess = TraceExpectation.leaf(
            SpanExpectation
              .consumer("custom-process orders")
              .attributesSubset(
                MessagingExperimentalAttributes.MessagingClientId("setup-client"),
                MessagingExperimentalAttributes.MessagingConsumerGroupName("orders-group"),
                MessagingExperimentalAttributes.MessagingOperationName("custom-process")
              )
              .noParentSpanContext
              .links(
                LinkSetExpectation.exactly(
                  LinkExpectation.any.spanContext(
                    SpanContextExpectation.any
                      .traceIdHex(producerContext.traceIdHex)
                      .spanIdHex(producerContext.spanIdHex)
                  )
                )
              )
          )
          val expectedReceive = TraceExpectation.leaf(
            SpanExpectation
              .client("poll orders")
              .attributesSubset(
                MessagingExperimentalAttributes.MessagingClientId("constant-client"),
                MessagingExperimentalAttributes.MessagingConsumerGroupName("orders-group")
              )
              .noParentSpanContext
              .links(
                LinkSetExpectation.exactly(
                  LinkExpectation.any.spanContext(
                    SpanContextExpectation.any
                      .traceIdHex(producerContext.traceIdHex)
                      .spanIdHex(producerContext.spanIdHex)
                  )
                )
              )
          )
          assertExpected(
            spans,
            TraceForestExpectation.unordered(
              root(SpanExpectation.internal("producer-root").scopeName("fs2.kafka.otel4s.tests")),
              expectedProcess,
              expectedProcess,
              expectedProcess,
              expectedProcess,
              expectedReceive,
              expectedReceive,
              expectedReceive,
              expectedReceive
            )
          )
        }
      } yield ()
    }
  }

  test("one explicit handle accepts records with different key types and two handles select metadata explicitly") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        first = tracer.consumerSpans(provider(ConsumerMetadata(Some("first-client"), Some("group-a"))))
        second = tracer.consumerSpans(provider(ConsumerMetadata(Some("second-client"), Some("group-b"))))
        _ <- first.process(ConsumerRecord("topic", 0, 1L, "key", "raw"))(IO.unit)
        _ <- first.process(ConsumerRecord("topic", 0, 2L, 1, "decoded"))(IO.unit)
        _ <- second.process(ConsumerRecord("topic", 0, 1L, "key", "other"))(IO.unit)
        spans <- testkit.finishedSpans
        clientIds = spans.map(_.getAttributes.get(AttributeKey.stringKey("messaging.client.id"))).sorted
        _ <- IO(assertEquals(clientIds, List("first-client", "first-client", "second-client")))
      } yield ()
    }
  }

  test("the four explicit process and receive overloads create the configured spans") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      val record = ConsumerRecord("topic", 0, 1L, "key", "value")
      val committable = StubKafkaConsumer.committableRecord(record)

      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        spans = tracer.consumerSpans(provider(ConsumerMetadata(Some("client"), Some("group"))))
        _ <- spans.receive(Chunk.singleton(record))(IO.unit)
        _ <- spans.receiveCommittable(Chunk.singleton(committable))(IO.unit)
        _ <- spans.process(record)(IO.unit)
        _ <- spans.process(committable)(IO.unit)
        finished <- testkit.finishedSpans
      } yield assertEquals(
        finished.map(_.getName).sorted,
        List("poll topic", "poll topic", "process topic", "process topic")
      )
    }
  }

  test("processBatch creates one linked span around one callback and summarizes mixed records") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      import testkit.appTracer
      for {
        producer <- testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer"))
        first <- Tracer[IO].rootSpan("source-one").surround {
          for {
            context <- Tracer[IO].currentSpanOrThrow.map(_.context)
            record <- producer.injectHeaders(ProducerRecord("orders-a", "one", "a"))
          } yield (context, record.headers)
        }
        second <- Tracer[IO].rootSpan("source-two").surround {
          for {
            context <- Tracer[IO].currentSpanOrThrow.map(_.context)
            record <- producer.injectHeaders(ProducerRecord("orders-b", "two", "b"))
          } yield (context, record.headers)
        }
        (firstContext, firstHeaders) = first
        (secondContext, secondHeaders) = second
        records = Chunk(
          ConsumerRecord("orders-a", 0, 11L, "one", "a").withHeaders(firstHeaders),
          ConsumerRecord("orders-b", 1, 12L, "two", "b").withHeaders(secondHeaders)
        )
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        spans = tracer.consumerSpans(provider(ConsumerMetadata(Some("client"), Some("group"))))
        callbackCount <- Ref[IO].of(0)
        _ <- appTracer.rootSpan("ambient").surround {
          spans.processBatch(records)(
            callbackCount.update(_ + 1) *> Tracer[IO].spanBuilder("callback").build.surround(IO.unit)
          )
        }
        count <- callbackCount.get
        finished <- testkit.finishedSpans
        _ <- IO {
          val batch = finished.find(_.getName == "process").get
          assertEquals(count, 1)
          assertEquals(batch.getLinks.size(), 2)
          assertEquals(batch.getParentSpanId, "0000000000000000")
          assert(batch.getLinks.asScala.exists(_.getSpanContext.getSpanId == firstContext.spanIdHex))
          assert(batch.getLinks.asScala.exists(_.getSpanContext.getSpanId == secondContext.spanIdHex))
          assertEquals(finished.find(_.getName == "callback").get.getParentSpanId, batch.getSpanId)
          assertEquals(batch.getAttributes.get(AttributeKey.stringKey("messaging.destination.name")), null)
          assertEquals(batch.getAttributes.get(AttributeKey.stringKey("messaging.destination.partition.id")), null)
          assert(batch.getAttributes.get(AttributeKey.longKey("messaging.batch.message_count")) == 2L)
          assertEquals(batch.getAttributes.get(AttributeKey.longKey("messaging.kafka.offset")), null)
        }
      } yield ()
    }
  }

  test("processBatch uses its own setup context and emits count for a singleton batch") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      val record = ConsumerRecord("orders", 3, 21L, "key", "value")
      for {
        tracer <- KafkaTracer.create[IO](
          KafkaTracer.Config.default
            .addConstAttributes(MessagingExperimentalAttributes.MessagingOperationName("constant"))
            .withBatchProcessSpanSetup { context =>
              Some(
                KafkaTracer.Config.SpanSetup.Process(
                  s"batch ${context.recordCount} ${context.topics.size} ${context.offset.getOrElse(-1L)}",
                  Attributes(MessagingExperimentalAttributes.MessagingOperationName("setup")),
                  KafkaTracer.Config.Defaults.spanFinalizationStrategy
                )
              )
            }
        )
        spans = tracer.consumerSpans(provider(ConsumerMetadata(None, None)))
        _ <- spans.processBatch(Chunk.singleton(record))(IO.unit)
        finished <- testkit.finishedSpans
      } yield {
        val batch = finished.head
        assertEquals(batch.getName, "batch 1 1 21")
        assert(batch.getAttributes.get(AttributeKey.longKey("messaging.batch.message_count")) == 1L)
        assert(batch.getAttributes.get(AttributeKey.longKey("messaging.kafka.offset")) == 21L)
        assertEquals(batch.getAttributes.get(AttributeKey.stringKey("messaging.operation.name")), "setup")
      }
    }
  }

  test("single-record and batch process setup hooks suppress independently") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      val record = ConsumerRecord("topic", 0, 1L, "key", "value")
      val config = KafkaTracer.Config.default.withBatchProcessSpanSetup(_ => None)
      for {
        tracer <- KafkaTracer.create[IO](config)
        spans = tracer.consumerSpans(provider(ConsumerMetadata(None, None)))
        _ <- spans.processBatch(Chunk.singleton(record))(IO.unit)
        _ <- spans.process(record)(IO.unit)
        finished <- testkit.finishedSpans
      } yield assertEquals(finished.map(_.getName), List("process topic"))
    }
  }

  test("explicit consumer spans evaluate the supplied provider, while noop does not") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      val record = ConsumerRecord("topic", 0, 1L, "key", "value")
      for {
        gets <- Ref[IO].of(0)
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        provider = new ConsumerMetadata.Provider[IO] {
          override def get: IO[ConsumerMetadata] =
            gets.update(_ + 1).as(ConsumerMetadata(Some("provided-client"), Some("provided-group")))
        }
        spans = tracer.consumerSpans(provider)
        _ <- spans.process(record)(IO.unit)
        emptyResult <- tracer
          .consumerSpans(new ConsumerMetadata.Provider[IO] {
            override def get: IO[ConsumerMetadata] = IO.raiseError(new AssertionError("empty batch evaluated metadata"))
          })
          .processBatch(Chunk.empty[ConsumerRecord[String, String]])(IO.pure("empty"))
        result <- KafkaTracer
          .noop[IO]
          .consumerSpans(new ConsumerMetadata.Provider[IO] {
            override def get: IO[ConsumerMetadata] = IO.raiseError(new AssertionError("noop evaluated metadata"))
          })
          .process(record)(IO.pure("ran"))
        noopBatch <- KafkaTracer
          .noop[IO]
          .consumerSpans(new ConsumerMetadata.Provider[IO] {
            override def get: IO[ConsumerMetadata] = IO.raiseError(new AssertionError("noop evaluated metadata"))
          })
          .processBatch(Chunk.singleton(record))(IO.pure("noop-batch"))
        getCount <- gets.get
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(result, "ran")
        assertEquals(emptyResult, "empty")
        assertEquals(noopBatch, "noop-batch")
        assertEquals(getCount, 1)
        assertEquals(
          finished.map(_.getAttributes.get(AttributeKey.stringKey("messaging.client.id"))),
          List("provided-client")
        )
        assertEquals(
          finished.head.getAttributes.get(AttributeKey.stringKey("messaging.consumer.group.name")),
          "provided-group"
        )
      }
    }
  }

  test("suppressed, empty, and noop receive/process calls still evaluate the supplied effect") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      val record = ConsumerRecord("topic", 0, 1L, "key", "value")
      for {
        calls <- Ref[IO].of(0)
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default.withoutProcessSpans)
        disabled = tracer.consumerSpans(provider(ConsumerMetadata(None, None)))
        receiveTracer <- KafkaTracer.create[IO](KafkaTracer.Config.default.withoutReceiveSpans)
        receiveDisabled = receiveTracer.consumerSpans(provider(ConsumerMetadata(None, None)))
        noop = KafkaTracer.noop[IO].consumerSpans(provider(ConsumerMetadata(None, None)))
        disabledResult <- disabled.process(record)(calls.update(_ + 1).as(42))
        disabledBatch <- disabled.processBatch(Chunk.singleton(record))(calls.update(_ + 1).as(47))
        emptyBatch <- disabled.processBatch(Chunk.empty[ConsumerRecord[String, String]])(calls.update(_ + 1).as(48))
        disabledReceive <- receiveDisabled.receive(Chunk.singleton(record))(calls.update(_ + 1).as(44))
        emptyReceive <- disabled.receive(Chunk.empty[ConsumerRecord[String, String]])(calls.update(_ + 1).as(46))
        noopResult <- noop.process(record)(calls.update(_ + 1).as(43))
        noopBatch <- noop.processBatch(Chunk.singleton(record))(calls.update(_ + 1).as(49))
        noopReceive <- noop.receive(Chunk.singleton(record))(calls.update(_ + 1).as(45))
        count <- calls.get
        spans <- testkit.finishedSpans
      } yield {
        assertEquals(disabledResult, 42)
        assertEquals(disabledBatch, 47)
        assertEquals(emptyBatch, 48)
        assertEquals(disabledReceive, 44)
        assertEquals(emptyReceive, 46)
        assertEquals(noopResult, 43)
        assertEquals(noopBatch, 49)
        assertEquals(noopReceive, 45)
        assertEquals(count, 8)
        assertEquals(spans, Nil)
      }
    }
  }

  test("explicit process spans finalize on failure and cancellation") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      val failure = new RuntimeException("process failed")
      val record = ConsumerRecord("topic", 0, 1L, "key", "value")

      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        spans = tracer.consumerSpans(provider(ConsumerMetadata(None, None)))
        error <- spans.process(record)(IO.raiseError[Unit](failure)).attempt
        started <- Deferred[IO, Unit]
        gate <- Deferred[IO, Unit]
        fiber <- spans.process(record)(started.complete(()) *> gate.get).start
        _ <- started.get
        _ <- fiber.cancel
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(error, Left(failure))
        assertEquals(finished.size, 2)
        assertEquals(finished.map(_.getName), List("process topic", "process topic"))
      }
    }
  }

  private def provider(metadata: ConsumerMetadata): ConsumerMetadata.Provider[IO] =
    ConsumerMetadata.Provider.constant[IO](metadata)

}

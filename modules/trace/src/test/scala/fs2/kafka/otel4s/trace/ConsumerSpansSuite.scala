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
import cats.Parallel
import cats.effect.Temporal
import fs2.{Chunk, Pipe, Stream}
import fs2.kafka._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.typelevel.otel4s.oteljava.testkit.AttributesExpectation
import org.typelevel.otel4s.oteljava.testkit.trace.{SpanExpectation, TraceForestExpectation}
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.oteljava.testkit.trace.{
  LinkExpectation,
  LinkSetExpectation,
  SpanContextExpectation,
  StatusExpectation,
  TraceExpectation
}
import org.typelevel.otel4s.trace.Tracer
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration._

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
        _ <- IO(
          assertExpected(
            spans,
            TraceForestExpectation.unordered(
              root(
                SpanExpectation
                  .consumer("process topic")
                  .attributesSubset(
                    MessagingExperimentalAttributes.MessagingClientId("first-client"),
                    MessagingExperimentalAttributes.MessagingConsumerGroupName("group-a")
                  )
              ),
              root(
                SpanExpectation
                  .consumer("process topic")
                  .attributesSubset(
                    MessagingExperimentalAttributes.MessagingClientId("first-client"),
                    MessagingExperimentalAttributes.MessagingConsumerGroupName("group-a")
                  )
              ),
              root(
                SpanExpectation
                  .consumer("process topic")
                  .attributesSubset(
                    MessagingExperimentalAttributes.MessagingClientId("second-client"),
                    MessagingExperimentalAttributes.MessagingConsumerGroupName("group-b")
                  )
              )
            )
          )
        )
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
      } yield assertExpected(
        finished,
        TraceForestExpectation.unordered(
          root(SpanExpectation.client("poll topic")),
          root(SpanExpectation.client("poll topic")),
          root(SpanExpectation.consumer("process topic")),
          root(SpanExpectation.consumer("process topic"))
        )
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
          assertEquals(count, 1)
          assertExpected(
            finished,
            TraceForestExpectation.unordered(
              root(SpanExpectation.internal("source-one").scopeName("fs2.kafka.otel4s.tests")),
              root(SpanExpectation.internal("source-two").scopeName("fs2.kafka.otel4s.tests")),
              root(SpanExpectation.internal("ambient").scopeName("fs2.kafka.otel4s.tests")),
              root(
                SpanExpectation
                  .consumer("process")
                  .scopeName("fs2.kafka")
                  .attributes(
                    AttributesExpectation.where(
                      "mixed batch has count but no single-record destination, partition, or offset"
                    ) { attributes =>
                      AttributesExpectation
                        .subset(
                          Attributes(
                            MessagingExperimentalAttributes.MessagingBatchMessageCount(2L),
                            MessagingExperimentalAttributes.MessagingClientId("client"),
                            MessagingExperimentalAttributes.MessagingConsumerGroupName("group")
                          )
                        )
                        .matches(attributes) &&
                      attributes.get(MessagingExperimentalAttributes.MessagingDestinationName).isEmpty &&
                      attributes.get(MessagingExperimentalAttributes.MessagingDestinationPartitionId).isEmpty &&
                      attributes.get(MessagingExperimentalAttributes.MessagingKafkaOffset).isEmpty
                    }
                  )
                  .links(
                    LinkSetExpectation.exactly(
                      LinkExpectation.any.spanContext(
                        SpanContextExpectation.any
                          .traceIdHex(firstContext.traceIdHex)
                          .spanIdHex(firstContext.spanIdHex)
                          .remote(true)
                      ),
                      LinkExpectation.any.spanContext(
                        SpanContextExpectation.any
                          .traceIdHex(secondContext.traceIdHex)
                          .spanIdHex(secondContext.spanIdHex)
                          .remote(true)
                      )
                    )
                  ),
                TraceExpectation.leaf(SpanExpectation.internal("callback").scopeName("fs2.kafka.otel4s.tests"))
              )
            )
          )
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
        assertExpected(
          finished,
          TraceForestExpectation.unordered(
            root(
              SpanExpectation
                .consumer("batch 1 1 21")
                .scopeName("fs2.kafka")
                .attributesSubset(
                  MessagingExperimentalAttributes.MessagingBatchMessageCount(1L),
                  MessagingExperimentalAttributes.MessagingKafkaOffset(21L),
                  MessagingExperimentalAttributes.MessagingOperationName("setup")
                )
            )
          )
        )
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
      } yield assertExpected(
        finished,
        TraceForestExpectation.unordered(root(SpanExpectation.consumer("process topic").scopeName("fs2.kafka")))
      )
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
        assertExpected(
          finished,
          TraceForestExpectation.unordered(
            root(
              SpanExpectation
                .consumer("process topic")
                .attributesSubset(
                  MessagingExperimentalAttributes.MessagingClientId("provided-client"),
                  MessagingExperimentalAttributes.MessagingConsumerGroupName("provided-group")
                )
            )
          )
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
        val expected = root(SpanExpectation.consumer("process topic").status(StatusExpectation.error))
        assertExpected(finished, TraceForestExpectation.unordered(expected, expected))
      }
    }
  }

  test("commitOffsets describes only supplied topic partitions and preserves ambient parenting") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      import testkit.appTracer

      val offsets = Map(
        new TopicPartition("orders-a", 0) -> new OffsetAndMetadata(25L),
        new TopicPartition("orders-b", 0) -> new OffsetAndMetadata(31L)
      )
      val setupCalls = new AtomicInteger(0)
      val observed = new AtomicReference[CommitSpanContext](null)
      for {
        tracer <- KafkaTracer.create[IO](
          KafkaTracer.Config.default
            .addConstAttributes(MessagingExperimentalAttributes.MessagingOperationName("constant"))
            .withCommitSpanSetup { context =>
              setupCalls.incrementAndGet()
              observed.set(context)
              Some(
                KafkaTracer.Config.SpanSetup.Commit(
                  "offset commit",
                  Attributes(MessagingExperimentalAttributes.MessagingOperationName("setup")),
                  KafkaTracer.Config.Defaults.spanFinalizationStrategy
                )
              )
            }
        )
        spans = tracer.consumerSpans(provider(ConsumerMetadata(Some("client"), Some("group"))))
        committed <- Ref[IO].of(0)
        result <- appTracer.rootSpan("ambient").surround(spans.commitOffsets(offsets)(committed.update(_ + 1).as(42)))
        count <- committed.get
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(count, 1)
        assertEquals(result, 42)
        assertEquals(setupCalls.get(), 1)
        val context = observed.get()
        assertEquals(context.topics, Set("orders-a", "orders-b"))
        assertEquals(context.partitions, Set(0))
        assertEquals(context.recordCount, None)
        assertEquals(context.messageKey, None)
        assertEquals(context.offset, None)
        assertEquals(context.clientId, Some("client"))
        assertEquals(context.groupId, Some("group"))
        assertExpected(
          finished,
          TraceForestExpectation.unordered(
            root(
              SpanExpectation.internal("ambient"),
              TraceExpectation.leaf(
                SpanExpectation
                  .client("offset commit")
                  .linkCount(0)
                  .attributes(
                    AttributesExpectation.exact(
                      Attributes(
                        MessagingExperimentalAttributes.MessagingSystem(
                          MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                        ),
                        MessagingExperimentalAttributes.MessagingOperationName("setup"),
                        MessagingExperimentalAttributes.MessagingOperationType("settle"),
                        MessagingExperimentalAttributes.MessagingClientId("client"),
                        MessagingExperimentalAttributes.MessagingConsumerGroupName("group")
                      )
                    )
                  )
              )
            )
          )
        )
      }
    }
  }

  test("single offset commit may report its partition but never the next offset as a record offset") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      val offsets = Map(new TopicPartition("orders", 3) -> new OffsetAndMetadata(99L))
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        spans = tracer.consumerSpans(provider(ConsumerMetadata(None, None)))
        _ <- spans.commitOffsets(offsets)(IO.pure("done"))
        finished <- testkit.finishedSpans
      } yield {
        assertExpected(
          finished,
          TraceForestExpectation.unordered(
            root(
              SpanExpectation
                .client("commit orders")
                .linkCount(0)
                .attributes(
                  AttributesExpectation.exact(
                    Attributes(
                      MessagingExperimentalAttributes.MessagingSystem(
                        MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                      ),
                      MessagingExperimentalAttributes.MessagingOperationName("commit"),
                      MessagingExperimentalAttributes.MessagingOperationType("settle"),
                      MessagingExperimentalAttributes.MessagingDestinationName("orders"),
                      MessagingExperimentalAttributes.MessagingDestinationPartitionId("3")
                    )
                  )
                )
            )
          )
        )
      }
    }
  }

  test("empty and noop offset commits skip metadata while suppressed commits still evaluate effects") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      val offset = Map(new TopicPartition("orders", 0) -> new OffsetAndMetadata(1L))
      val providerCalls = new AtomicInteger(0)
      val metadata = new ConsumerMetadata.Provider[IO] {
        override def get: IO[ConsumerMetadata] =
          IO(providerCalls.incrementAndGet()).as(ConsumerMetadata(None, None))
      }
      for {
        effectCalls <- Ref[IO].of(0)
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default.withoutCommitSpans)
        disabled = tracer.consumerSpans(metadata)
        noop = KafkaTracer.noop[IO].consumerSpans(metadata)
        enabled <- KafkaTracer.create[IO](
          KafkaTracer.Config.default
            .withCommitSpanSetup(_ => throw new AssertionError("empty offset commits must not evaluate setup"))
        )
        enabledSpans = enabled.consumerSpans(metadata)
        a <- disabled.commitOffsets(offset)(effectCalls.update(_ + 1).as(10))
        b <- disabled.commitOffsets(Map.empty[TopicPartition, OffsetAndMetadata])(effectCalls.update(_ + 1).as(11))
        c <- noop.commitOffsets(offset)(effectCalls.update(_ + 1).as(12))
        d <- enabledSpans.commitOffsets(Map.empty[TopicPartition, OffsetAndMetadata])(effectCalls.update(_ + 1).as(13))
        effectCount <- effectCalls.get
        finished <- testkit.finishedSpans
      } yield {
        assertEquals((a, b, c, d), (10, 11, 12, 13))
        assertEquals(providerCalls.get(), 1)
        assertEquals(effectCount, 4)
        assertEquals(finished, Nil)
      }
    }
  }

  test("commitOffsets finalizes spans after errors and cancellation") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      val offsets = Map(new TopicPartition("orders", 0) -> new OffsetAndMetadata(1L))
      val failure = new RuntimeException("commit failed")
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        spans = tracer.consumerSpans(provider(ConsumerMetadata(None, None)))
        error <- spans.commitOffsets(offsets)(IO.raiseError[Unit](failure)).attempt
        started <- Deferred[IO, Unit]
        gate <- Deferred[IO, Unit]
        fiber <- spans.commitOffsets(offsets)(started.complete(()) *> gate.get).start
        _ <- started.get
        _ <- fiber.cancel
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(error, Left(failure))
        val expected = root(SpanExpectation.client("commit orders").status(StatusExpectation.error))
        assertExpected(finished, TraceForestExpectation.unordered(expected, expected))
      }
    }
  }

  test("commitBatchWithin batches offsets, keeps the highest position, and traces each batch") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        spans = tracer.consumerSpans(provider(ConsumerMetadata(Some("client"), Some("group"))))
        committed <- Ref[IO].of(List.empty[List[Long]])
        committer = KafkaCommitter[IO](
          offsets => committed.update(_ :+ offsets.valuesIterator.map(_.offset()).toList.sorted),
          IO.raiseError(new AssertionError("unexpected metadata"))
        )
        _ <- Stream
          .emits(List(1L, 2L, 3L, 4L, 5L).map(offset(_, committer)))
          .covary[IO]
          .through(batchPipe(spans, 2, 1.second))
          .compile
          .drain
        groups <- committed.get
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(groups, List(List(2L), List(4L), List(5L)))
        val expected = root(
          SpanExpectation
            .client("commit orders")
            .attributes(
              AttributesExpectation.exact(
                Attributes(
                  MessagingExperimentalAttributes.MessagingSystem(
                    MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                  ),
                  MessagingExperimentalAttributes.MessagingOperationName("commit"),
                  MessagingExperimentalAttributes.MessagingOperationType("settle"),
                  MessagingExperimentalAttributes.MessagingDestinationName("orders"),
                  MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                  MessagingExperimentalAttributes.MessagingClientId("client"),
                  MessagingExperimentalAttributes.MessagingConsumerGroupName("group")
                )
              )
            )
        )
        assertExpected(finished, TraceForestExpectation.unordered(expected, expected, expected))
      }
    }
  }

  test("commitBatchWithin retains distinct committers and flushes a partial batch on its timer") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        spans = tracer.consumerSpans(provider(ConsumerMetadata(None, None)))
        seen <- Ref[IO].of(List.empty[(String, List[Long])])
        first = KafkaCommitter[IO](
          offsets => seen.update(_ :+ ("first" -> offsets.valuesIterator.map(_.offset()).toList)),
          IO.raiseError(new AssertionError("metadata"))
        )
        second = KafkaCommitter[IO](
          offsets => seen.update(_ :+ ("second" -> offsets.valuesIterator.map(_.offset()).toList)),
          IO.raiseError(new AssertionError("metadata"))
        )
        firstOffset = offset(3L, first)
        secondOffset = offset(5L, second)
        _ <- Stream
          .emits(List(firstOffset, secondOffset))
          .covary[IO]
          .through(spans.commitBatchWithin(10, 1.second))
          .compile
          .drain
        dispatched <- seen.get
        flushed <- Deferred[IO, Unit]
        timerCommitter = KafkaCommitter[IO](
          _ => flushed.complete(()).void,
          IO.raiseError(new AssertionError("metadata"))
        )
        fiber <- (Stream.emit(offset(8L, timerCommitter)) ++ Stream.eval(IO.never[CommittableOffset[IO]]))
          .covary[IO]
          .through(spans.commitBatchWithin(10, 20.millis))
          .compile
          .drain
          .start
        didFlush <- flushed.get.as(true).timeoutTo(5.seconds, fiber.cancel.as(false))
        _ <- fiber.cancel
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(dispatched.sortBy(_._1), List("first" -> List(3L), "second" -> List(5L)))
        assertEquals(didFlush, true)
        val expected = root(SpanExpectation.client("commit orders"))
        assertExpected(finished, TraceForestExpectation.unordered(expected, expected, expected))
      }
    }
  }

  test("commitBatchWithin commits with noop and disabled spans and skips empty input") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default.withoutCommitSpans)
        disabled = tracer.consumerSpans(provider(ConsumerMetadata(None, None)))
        noop = KafkaTracer.noop[IO].consumerSpans(provider(ConsumerMetadata(None, None)))
        commits <- Ref[IO].of(0)
        committer = KafkaCommitter[IO](_ => commits.update(_ + 1), IO.raiseError(new AssertionError("metadata")))
        _ <- Stream.empty.covary[IO].through(disabled.commitBatchWithin(2, 1.second)).compile.drain
        _ <- Stream
          .emit(offset(1L, committer))
          .covary[IO]
          .through(disabled.commitBatchWithin(2, 1.second))
          .compile
          .drain
        _ <- Stream.emit(offset(2L, committer)).covary[IO].through(noop.commitBatchWithin(2, 1.second)).compile.drain
        count <- commits.get
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(count, 2)
        assertEquals(finished, Nil)
      }
    }
  }

  test("commitBatchWithin stops after failure and finalizes a canceled commit span") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      val failure = new RuntimeException("commit failed")
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        spans = tracer.consumerSpans(provider(ConsumerMetadata(None, None)))
        laterCommits <- Ref[IO].of(0)
        failing = KafkaCommitter[IO](_ => IO.raiseError(failure), IO.raiseError(new AssertionError("metadata")))
        later = KafkaCommitter[IO](_ => laterCommits.update(_ + 1), IO.raiseError(new AssertionError("metadata")))
        failed <- Stream
          .emits(List(offset(1L, failing), offset(2L, later)))
          .covary[IO]
          .through(spans.commitBatchWithin(1, 1.second))
          .compile
          .drain
          .attempt
        laterCount <- laterCommits.get
        started <- Deferred[IO, Unit]
        gate <- Deferred[IO, Unit]
        waiting = KafkaCommitter[IO](
          _ => started.complete(()) *> gate.get,
          IO.raiseError(new AssertionError("metadata"))
        )
        fiber <- Stream
          .emit(offset(3L, waiting))
          .covary[IO]
          .through(spans.commitBatchWithin(1, 1.second))
          .compile
          .drain
          .start
        _ <- started.get
        _ <- fiber.cancel
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(failed, Left(failure))
        assertEquals(laterCount, 0)
        val expected = root(SpanExpectation.client("commit orders").status(StatusExpectation.error))
        assertExpected(finished, TraceForestExpectation.unordered(expected, expected))
      }
    }
  }

  test("KafkaTracer.consumerSpans uses the supplied provider for commit metadata") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        committer = KafkaCommitter[IO](_ => IO.unit, IO.raiseError(new AssertionError("metadata")))
        _ <- Stream
          .emit(offset(1L, committer))
          .covary[IO]
          .through(
            tracer.consumerSpans(provider(ConsumerMetadata(None, None))).commitBatchWithin(1, 1.second)
          )
          .compile
          .drain
        _ <- Stream
          .emit(offset(2L, committer))
          .covary[IO]
          .through(
            tracer
              .consumerSpans(provider(ConsumerMetadata(Some("orders-client"), Some("orders-group"))))
              .commitBatchWithin(1, 1.second)
          )
          .compile
          .drain
        finished <- testkit.finishedSpans
      } yield {
        assertExpected(
          finished,
          TraceForestExpectation.unordered(
            root(
              SpanExpectation
                .client("commit orders")
                .attributes(AttributesExpectation.where("no client or group metadata") { attributes =>
                  attributes.get(MessagingExperimentalAttributes.MessagingClientId).isEmpty &&
                  attributes.get(MessagingExperimentalAttributes.MessagingConsumerGroupName).isEmpty
                })
            ),
            root(
              SpanExpectation
                .client("commit orders")
                .attributesSubset(
                  MessagingExperimentalAttributes.MessagingClientId("orders-client"),
                  MessagingExperimentalAttributes.MessagingConsumerGroupName("orders-group")
                )
            )
          )
        )
      }
    }
  }

  test("KafkaTracer.consumerSpans commit pipe can be reused across stream runs") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val tracerProvider: org.typelevel.otel4s.trace.TracerProvider[IO] = testkit.tracerProvider
      for {
        tracer <- KafkaTracer.create[IO](KafkaTracer.Config.default)
        commits <- Ref[IO].of(0)
        committer = KafkaCommitter[IO](_ => commits.update(_ + 1), IO.raiseError(new AssertionError("metadata")))
        offsets = List(offset(1L, committer), offset(2L, committer), offset(3L, committer))
        pipe = tracer.consumerSpans(provider(ConsumerMetadata(None, None))).commitBatchWithin(1, 1.second)
        stream = Stream.emits(offsets).covary[IO].through(pipe)
        _ <- stream.compile.drain
        _ <- stream.compile.drain
        count <- commits.get
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(count, 6)
        val expected = root(SpanExpectation.client("commit orders"))
        assertExpected(finished, TraceForestExpectation.unordered(expected, List.fill(5)(expected) *))
      }
    }
  }

  private def offset(position: Long, committer: KafkaCommitter[IO]): CommittableOffset[IO] =
    CommittableOffset[IO](
      new TopicPartition("orders", 0),
      new OffsetAndMetadata(position),
      committer
    )

  private def batchPipe[F[_]: Temporal: Parallel](spans: ConsumerSpans[F], n: Int, d: FiniteDuration): Pipe[
    F,
    CommittableOffset[F],
    Unit
  ] =
    spans.commitBatchWithin(n, d)

  private def provider(metadata: ConsumerMetadata): ConsumerMetadata.Provider[IO] =
    ConsumerMetadata.Provider.constant[IO](metadata)

}

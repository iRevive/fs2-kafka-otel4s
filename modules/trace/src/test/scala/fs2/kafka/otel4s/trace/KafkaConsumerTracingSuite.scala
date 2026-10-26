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
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import org.typelevel.otel4s.oteljava.testkit.AttributesExpectation
import org.typelevel.otel4s.Attributes
import org.typelevel.otel4s.oteljava.testkit.trace._
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes
import org.typelevel.otel4s.trace.Tracer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

@annotation.nowarn("cat=deprecation")
final class KafkaConsumerTracingSuite extends KafkaTracingTestSupport {

  test("consumeChunk delegates to the underlying consumer without tracing") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          processed <- Ref[IO].of(Option.empty[Chunk[ConsumerRecord[String, String]]])
          commits <- Ref[IO].of(0)
          record = ConsumerRecord("topic", 0, 1L, "k", "v")
          consumer = StubKafkaConsumer.streaming(
            List(committableRecord(record, commits))
          )
          traced <- testkit.tracedConsumer[String, String](consumer)
          _ <- traced.consumeChunk { chunk =>
            processed.set(Some(chunk)).as(CommitNow)
          }.attempt
          seen <- processed.get
          commitCount <- commits.get
          spans <- testkit.finishedSpans
        } yield {
          assertEquals(seen, Some(Chunk.singleton(record)))
          assertEquals(commitCount, 1)
          assertEquals(spans, Nil)
        }
      }
  }

  test("consumeChunkTraced(chunk) traces delivery, processing, and commit") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          processed <- Ref[IO].of(Option.empty[Chunk[ConsumerRecord[String, String]]])
          commits <- Ref[IO].of(0)
          record = ConsumerRecord("topic", 0, 1L, "k", "v")
          consumer = StubKafkaConsumer.streaming(
            List(committableRecord(record, commits)),
            clientId = "consumer-client",
            groupId = "consumer-group"
          )
          traced <- testkit.tracedConsumer[String, String](consumer)
          _ <- traced.consumeChunkTraced { chunk =>
            processed.set(Some(chunk)).as(CommitNow)
          }.attempt
          seen <- processed.get
          commitCount <- commits.get
          spans <- testkit.finishedSpans
          _ <- IO {
            assertEquals(seen, Some(Chunk.singleton(record)))
            assertEquals(commitCount, 1)
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .client("poll topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingSystem(
                        MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                      ),
                      MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                      MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                      MessagingExperimentalAttributes.MessagingOperationName("poll"),
                      MessagingExperimentalAttributes.MessagingOperationType("receive"),
                      MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                      MessagingExperimentalAttributes.MessagingConsumerGroupName("consumer-group")
                    )
                ),
                root(
                  SpanExpectation
                    .consumer("process topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingOperationName("process"),
                      MessagingExperimentalAttributes.MessagingOperationType("process")
                    )
                ),
                root(
                  SpanExpectation
                    .client("commit topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingSystem(
                        MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                      ),
                      MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                      MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                      MessagingExperimentalAttributes.MessagingOperationName("commit"),
                      MessagingExperimentalAttributes.MessagingOperationType("settle"),
                      MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                      MessagingExperimentalAttributes.MessagingConsumerGroupName("consumer-group")
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("consumeChunkTraced uses one batch process span for the whole callback chunk") {
    KafkaTracerTestkit.create().use { testkit =>
      val records = List(
        ConsumerRecord("topic", 0, 1L, "k1", "v1"),
        ConsumerRecord("topic", 0, 2L, "k2", "v2")
      )
      for {
        callbacks <- Ref[IO].of(0)
        consumer <- IO.pure(
          StubKafkaConsumer.streaming(records.map(StubKafkaConsumer.committableRecord(_)))
        )
        traced <- testkit.tracedConsumer[String, String](consumer)
        _ <- traced.consumeChunkTraced { chunk =>
          callbacks.update(_ + 1) *> IO(assertEquals(chunk.size, 2)).as(CommitNow)
        }.attempt
        callbackCount <- callbacks.get
        spans <- testkit.finishedSpans
      } yield {
        assertEquals(callbackCount, 1)
        assertExpected(
          spans,
          TraceForestExpectation.unordered(
            root(SpanExpectation.client("poll topic").scopeName("fs2.kafka")),
            root(
              SpanExpectation
                .consumer("process topic")
                .scopeName("fs2.kafka")
                .attributes(AttributesExpectation.where("batch count is two and offset is absent") { attributes =>
                  AttributesExpectation
                    .subset(
                      Attributes(
                        MessagingExperimentalAttributes.MessagingBatchMessageCount(2L)
                      )
                    )
                    .matches(attributes) && attributes.get[Long]("messaging.kafka.offset").isEmpty
                })
            ),
            root(SpanExpectation.client("commit topic").scopeName("fs2.kafka"))
          )
        )
      }
    }
  }

  test("consumeChunkTraced skips owned commits when the batch callback errors or is canceled") {
    KafkaTracerTestkit.create().use { testkit =>
      val record = ConsumerRecord("topic", 0, 1L, "key", "value")
      val failure = new RuntimeException("batch failed")
      for {
        errorCommits <- Ref[IO].of(0)
        errorConsumer = StubKafkaConsumer.streaming(List(committableRecord(record, errorCommits)))
        errorTraced <- testkit.tracedConsumer[String, String](errorConsumer)
        error <- errorTraced.consumeChunkTraced(_ => IO.raiseError[CommitNow.type](failure)).attempt
        commitsAfterError <- errorCommits.get
        cancelCommits <- Ref[IO].of(0)
        cancelConsumer = StubKafkaConsumer.streaming(List(committableRecord(record, cancelCommits)))
        cancelTraced <- testkit.tracedConsumer[String, String](cancelConsumer)
        started <- Deferred[IO, Unit]
        gate <- Deferred[IO, Unit]
        fiber <- cancelTraced.consumeChunkTraced(_ => started.complete(()) *> gate.get.as(CommitNow)).start
        _ <- started.get
        _ <- fiber.cancel
        commitsAfterCancel <- cancelCommits.get
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(error, Left(failure))
        assertEquals(commitsAfterError, 0)
        assertEquals(commitsAfterCancel, 0)
        assertExpected(
          finished,
          TraceForestExpectation.unordered(
            root(SpanExpectation.client("poll topic").scopeName("fs2.kafka").status(StatusExpectation.error)),
            root(SpanExpectation.consumer("process topic").scopeName("fs2.kafka").status(StatusExpectation.error)),
            root(SpanExpectation.client("poll topic").scopeName("fs2.kafka").status(StatusExpectation.error)),
            root(SpanExpectation.consumer("process topic").scopeName("fs2.kafka").status(StatusExpectation.error))
          )
        )
      }
    }
  }

  test("consumeChunkTraced(chunk) can disable delivery and commit spans") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val config =
          KafkaTracer.Config.default.withoutReceiveSpans.withoutCommitSpans

        for {
          processed <- Ref[IO].of(Option.empty[Chunk[ConsumerRecord[String, String]]])
          commits <- Ref[IO].of(0)
          record = ConsumerRecord("topic", 0, 1L, "k", "v")
          consumer = StubKafkaConsumer.streaming(
            List(committableRecord(record, commits)),
            clientId = "consumer-client",
            groupId = "consumer-group"
          )
          traced <- testkit.tracedConsumer[String, String](consumer, config)
          _ <- traced.consumeChunkTraced { chunk =>
            processed.set(Some(chunk)).as(CommitNow)
          }.attempt
          seen <- processed.get
          commitCount <- commits.get
          spans <- testkit.finishedSpans
          _ <- IO {
            assertEquals(seen, Some(Chunk.singleton(record)))
            assertEquals(commitCount, 1)
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .consumer("process topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingSystem(
                        MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                      ),
                      MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                      MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                      MessagingExperimentalAttributes.MessagingOperationName("process"),
                      MessagingExperimentalAttributes.MessagingOperationType("process"),
                      MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                      MessagingExperimentalAttributes.MessagingConsumerGroupName("consumer-group")
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("consumeRecordTraced traces delivery, each processed record, and commit") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          processed <- Ref[IO].of(List.empty[ConsumerRecord[String, String]])
          commits <- Ref[IO].of(0)
          record1 = ConsumerRecord("topic", 0, 1L, "k1", "v1")
          record2 = ConsumerRecord("topic", 0, 2L, "k2", "v2")
          consumer = StubKafkaConsumer.streaming(
            List(
              committableRecord(record1, commits),
              committableRecord(record2, commits)
            ),
            clientId = "consumer-client",
            groupId = "consumer-group"
          )
          traced <- testkit.tracedConsumer[String, String](consumer)
          _ <- traced
            .consumeRecordTraced(record => processed.update(_ :+ record))
            .attempt
          seen <- processed.get
          commitCount <- commits.get
          spans <- testkit.finishedSpans
          _ <- IO {
            assertEquals(seen, List(record1, record2))
            assertEquals(commitCount, 2)
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .client("poll topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingOperationName("poll"),
                      MessagingExperimentalAttributes.MessagingOperationType("receive")
                    )
                ),
                root(
                  SpanExpectation
                    .consumer("process topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingSystem(
                        MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                      ),
                      MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                      MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                      MessagingExperimentalAttributes.MessagingOperationName("process"),
                      MessagingExperimentalAttributes.MessagingOperationType("process"),
                      MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                      MessagingExperimentalAttributes.MessagingConsumerGroupName("consumer-group")
                    )
                ),
                root(
                  SpanExpectation
                    .consumer("process topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingSystem(
                        MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                      ),
                      MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                      MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                      MessagingExperimentalAttributes.MessagingOperationName("process"),
                      MessagingExperimentalAttributes.MessagingOperationType("process"),
                      MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                      MessagingExperimentalAttributes.MessagingConsumerGroupName("consumer-group")
                    )
                ),
                root(
                  SpanExpectation
                    .client("commit topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingSystem(
                        MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                      ),
                      MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                      MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                      MessagingExperimentalAttributes.MessagingOperationName("commit"),
                      MessagingExperimentalAttributes.MessagingOperationType("settle"),
                      MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                      MessagingExperimentalAttributes.MessagingConsumerGroupName("consumer-group"),
                      MessagingExperimentalAttributes.MessagingBatchMessageCount(2L)
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("custom commit span setup overrides span name and attributes") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val config =
          KafkaTracer.Config.default.withCommitSpanSetup { ctx =>
            Some(
              KafkaTracer.Config.SpanSetup.Commit(
                spanName = s"finish ${ctx.topics.headOption.getOrElse("unknown")} ${ctx.recordCount}",
                attributes = Attributes(
                  MessagingExperimentalAttributes.MessagingOperationName("finish")
                ),
                finalizationStrategy = KafkaTracer.Config.Defaults.spanFinalizationStrategy
              )
            )
          }

        for {
          commits <- Ref[IO].of(0)
          record = ConsumerRecord("topic", 0, 1L, "k", "v")
          consumer = StubKafkaConsumer.streaming(
            List(committableRecord(record, commits))
          )
          traced <- testkit.tracedConsumer[String, String](consumer, config)
          _ <- traced
            .consumeChunkTraced(_ => IO.pure(CommitNow))
            .attempt
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(SpanExpectation.client("poll topic").scopeName("fs2.kafka")),
                root(SpanExpectation.consumer("process topic").scopeName("fs2.kafka")),
                root(
                  SpanExpectation
                    .client("finish topic 1")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingOperationName("finish"),
                      MessagingExperimentalAttributes.MessagingOperationType("settle")
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("receive and process spans do not inherit ambient context") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        import testkit.appTracer

        for {
          consumerTracer <- testkit.tracedConsumer[String, String](
            StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
          )
          record = ConsumerRecord("topic", 0, 42L, "key", "value")
          _ <- Tracer[IO]
            .rootSpan("ambient-context")
            .surround {
              consumerTracer.spans.receive(Chunk.singleton(record))(IO.unit) >>
                consumerTracer.spans.process(record)(IO.unit)
            }
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(SpanExpectation.internal("ambient-context").scopeName("fs2.kafka.otel4s.tests")),
                root(
                  SpanExpectation
                    .client("poll topic")
                    .scopeName("fs2.kafka")
                    .noParentSpanContext
                ),
                root(
                  SpanExpectation
                    .consumer("process topic")
                    .scopeName("fs2.kafka")
                    .noParentSpanContext
                )
              )
            )
          }
        } yield ()
      }
  }

  test("process links record trace context and emits a process span") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        import testkit.appTracer

        for {
          producerTracer <- testkit.tracedProducer[String, String](
            StubKafkaProducer.metadataOnly("producer-client")
          )
          produced <- Tracer[IO]
            .rootSpan("producer-root")
            .surround {
              for {
                span <- Tracer[IO].currentSpanOrThrow
                propagated <- producerTracer.injectHeaders(
                  ProducerRecord("topic", "key", "value")
                )
              } yield (span.context, propagated)
            }
          (producerRootContext, propagated) = produced
          consumed = ConsumerRecord("topic", 0, 42L, "key", "value").withHeaders(propagated.headers)
          consumerTracer <- testkit.tracedConsumer[String, String](
            StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
          )
          _ <- consumerTracer.spans.process(consumed)(IO.unit)
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(SpanExpectation.internal("producer-root").scopeName("fs2.kafka.otel4s.tests")),
                TraceExpectation.leaf(
                  SpanExpectation
                    .consumer("process topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingSystem(
                        MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                      ),
                      MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                      MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                      MessagingExperimentalAttributes.MessagingOperationName("process"),
                      MessagingExperimentalAttributes.MessagingOperationType("process"),
                      MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                      MessagingExperimentalAttributes.MessagingConsumerGroupName("consumer-group"),
                      MessagingExperimentalAttributes.MessagingKafkaMessageKey("key"),
                      MessagingExperimentalAttributes.MessagingKafkaOffset(42L)
                    )
                    .noParentSpanContext
                    .links(
                      LinkSetExpectation.exactly(
                        LinkExpectation.any.spanContext(
                          SpanContextExpectation.any
                            .traceIdHex(producerRootContext.traceIdHex)
                            .spanIdHex(producerRootContext.spanIdHex)
                        )
                      )
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("disabled consumer spans still process and commit records") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val config =
          KafkaTracer.Config.default.withoutReceiveSpans.withoutProcessSpans.withoutCommitSpans

        for {
          processed <- Ref[IO].of(List.empty[ConsumerRecord[String, String]])
          commits <- Ref[IO].of(0)
          record = ConsumerRecord("topic", 0, 1L, "key", "value")
          consumer = StubKafkaConsumer.streaming(List(committableRecord(record, commits)))
          traced <- testkit.tracedConsumer[String, String](consumer, config)
          _ <- traced
            .consumeRecordTraced(record => processed.update(_ :+ record))
            .attempt
          seen <- processed.get
          commitCount <- commits.get
          spans <- testkit.finishedSpans
        } yield {
          assertEquals(seen, List(record))
          assertEquals(commitCount, 1)
          assertEquals(spans, Nil)
        }
      }
  }

  test("receive emits a poll span and links to record trace contexts") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        import testkit.appTracer

        for {
          producerTracer <- testkit.tracedProducer[String, String](
            StubKafkaProducer.metadataOnly("producer-client")
          )
          consumerTracer <- testkit.tracedConsumer[String, String](
            StubKafkaConsumer.metadataOnly("consumer-client", "consumer-group")
          )
          produced <- Tracer[IO]
            .rootSpan("producer-root")
            .surround {
              for {
                span <- Tracer[IO].currentSpanOrThrow
                record1 <- producerTracer.injectHeaders(ProducerRecord("topic", "k1", "v1"))
                record2 <- producerTracer.injectHeaders(ProducerRecord("topic", "k2", "v2"))
              } yield (span.context, record1, record2)
            }
          (producerRootContext, record1, record2) = produced
          records = Chunk(
            ConsumerRecord("topic", 0, 1L, "k1", "v1").withHeaders(record1.headers),
            ConsumerRecord("topic", 0, 2L, "k2", "v2").withHeaders(record2.headers)
          )
          _ <- consumerTracer.spans.receive(records)(IO.unit)
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(SpanExpectation.internal("producer-root").scopeName("fs2.kafka.otel4s.tests")),
                root(
                  SpanExpectation
                    .client("poll topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingSystem(
                        MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                      ),
                      MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                      MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                      MessagingExperimentalAttributes.MessagingOperationName("poll"),
                      MessagingExperimentalAttributes.MessagingOperationType("receive"),
                      MessagingExperimentalAttributes.MessagingClientId("consumer-client"),
                      MessagingExperimentalAttributes.MessagingConsumerGroupName("consumer-group"),
                      MessagingExperimentalAttributes.MessagingBatchMessageCount(2L)
                    )
                    .noParentSpanContext
                    .links(
                      LinkSetExpectation.exactly(
                        LinkExpectation.any.spanContext(
                          SpanContextExpectation.any
                            .traceIdHex(producerRootContext.traceIdHex)
                            .spanIdHex(producerRootContext.spanIdHex)
                        ),
                        LinkExpectation.any.spanContext(
                          SpanContextExpectation.any
                            .traceIdHex(producerRootContext.traceIdHex)
                            .spanIdHex(producerRootContext.spanIdHex)
                        )
                      )
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  private def committableRecord(
      record: ConsumerRecord[String, String],
      commits: Ref[IO, Int]
  ): CommittableConsumerRecord[IO, String, String] =
    CommittableConsumerRecord(
      record,
      CommittableOffset(
        new TopicPartition(record.topic, record.partition),
        new OffsetAndMetadata(record.offset + 1L),
        KafkaCommitter[IO](
          _ => commits.update(_ + 1),
          IO.raiseError(new AssertionError("unexpected metadata"))
        )
      )
    )

}

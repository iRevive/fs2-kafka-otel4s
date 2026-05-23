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

import cats.effect.{IO, Resource}
import fs2.Chunk
import fs2.kafka._
import fs2.kafka.otel4s.trace.instances._
import org.typelevel.otel4s.oteljava.testkit.AttributesExpectation
import org.typelevel.otel4s.oteljava.testkit.trace.{
  LinkExpectation,
  LinkSetExpectation,
  SpanExpectation,
  TraceForestExpectation
}
import org.typelevel.otel4s.context.propagation.TextMapGetter
import org.typelevel.otel4s.semconv.attributes.ServerAttributes
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes
import org.typelevel.otel4s.trace.SpanFinalizer
import org.typelevel.otel4s.Attributes

import scala.annotation.nowarn

final class KafkaProducerTracingSuite extends KafkaTracingTestSupport {

  final class UnrepresentableKey(val value: String)
  final class WrappedKey(val value: String)

  @nowarn("msg=method withSerializers in trait TracedKafkaProducer is deprecated")
  private def remapWithSerializers(
      producer: TracedKafkaProducer[IO, String, String]
  ): KafkaProducer.WithSettings[IO, WrappedKey, String] =
    producer.withSerializers(
      Serializer[IO, String].contramap[WrappedKey](_.value),
      Serializer[IO, String]
    )

  test("produce(...).flatten injects tracing headers and emits a send span") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          _ <- tracedProducer.produce(ProducerRecords.one(ProducerRecord("topic", "key", "value"))).flatten
          produced <- producer.getCaptured
          spans <- testkit.finishedSpans
          _ <- IO {
            assertEquals(produced.size, 1)
            assertEquals(produced.head.get.topic, "topic")
            assert(produced.head.get.headers.toChain.nonEmpty)
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingSystem(
                        MessagingExperimentalAttributes.MessagingSystemValue.Kafka
                      ),
                      MessagingExperimentalAttributes.MessagingDestinationName("topic"),
                      MessagingExperimentalAttributes.MessagingOperationName("send"),
                      MessagingExperimentalAttributes.MessagingOperationType("send"),
                      MessagingExperimentalAttributes.MessagingClientId("producer-client"),
                      MessagingExperimentalAttributes.MessagingKafkaMessageKey("key")
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("produce(...).flatten completes traced sends in one effect") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          _ <- tracedProducer
            .produce(ProducerRecords.one(ProducerRecord("topic-a", "key-a", "value-a")))
            .flatten
          _ <- tracedProducer
            .produce(ProducerRecords.one(ProducerRecord("topic-b", "key-b", "value-b")))
            .flatten
          completionCount <- producer.getCompletions
          produced <- producer.getCaptured
          spans <- testkit.finishedSpans
          _ <- IO {
            assertEquals(completionCount, 2)
            assertEquals(produced.size, 1)
            assertEquals(produced.head.get.topic, "topic-b")
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(SpanExpectation.producer("send topic-a").scopeName("fs2.kafka")),
                root(SpanExpectation.producer("send topic-b").scopeName("fs2.kafka"))
              )
            )
          }
        } yield ()
      }
  }

  test("single-message send adds broker partition and offset after completion") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          tracedProducer <- testkit.tracedProducer[String, String](
            StubKafkaProducer.metadataOnly("producer-client")
          )
          _ <- tracedProducer.produce(ProducerRecords.one(ProducerRecord("topic", "key", "value"))).flatten
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
                      MessagingExperimentalAttributes.MessagingKafkaOffset(42L)
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("batch produce creates per-message creation contexts and links the batch send span") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          records = Chunk(
            ProducerRecord("topic-a", "key-a", "value-a"),
            ProducerRecord("topic-b", "key-b", null.asInstanceOf[String]).withPartition(3)
          )
          _ <- tracedProducer.produce(records).flatten
          produced <- producer.getCaptured
          spans <- testkit.finishedSpans
          _ <- IO {
            assertEquals(produced.size, 2)

            val traceparents = produced.toList.map { record =>
              TextMapGetter[Headers]
                .get(record.headers, "traceparent")
                .getOrElse(fail("missing produced traceparent"))
            }
            assertEquals(traceparents.distinct.size, 2)

            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(SpanExpectation.producer("create topic-a").scopeName("fs2.kafka")),
                root(SpanExpectation.producer("create topic-b").scopeName("fs2.kafka")),
                root(
                  SpanExpectation
                    .client("send")
                    .scopeName("fs2.kafka")
                    .attributes(
                      AttributesExpectation.where(
                        "must keep tombstone detail off the batch span"
                      )(
                        _.get(MessagingExperimentalAttributes.MessagingKafkaMessageTombstone).isEmpty
                      )
                    )
                    .links(
                      LinkSetExpectation.exactly(
                        LinkExpectation.any
                          .attributesSubset(
                            MessagingExperimentalAttributes.MessagingDestinationName("topic-a")
                          ),
                        LinkExpectation.any
                          .attributesSubset(
                            MessagingExperimentalAttributes.MessagingDestinationName("topic-b"),
                            MessagingExperimentalAttributes.MessagingKafkaMessageTombstone(true)
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

  test("batch send does not emit span-level partition id for mixed-topic same-number partitions") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          records = Chunk(
            ProducerRecord("topic-a", "key-a", "value-a").withPartition(0),
            ProducerRecord("topic-b", "key-b", "value-b").withPartition(0)
          )
          _ <- tracedProducer.produce(records).flatten
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(SpanExpectation.producer("create topic-a").scopeName("fs2.kafka")),
                root(SpanExpectation.producer("create topic-b").scopeName("fs2.kafka")),
                root(
                  SpanExpectation
                    .client("send")
                    .scopeName("fs2.kafka")
                    .attributes(
                      AttributesExpectation.where(
                        "must omit span-level partition id for mixed-topic batches"
                      )(
                        _.get(MessagingExperimentalAttributes.MessagingDestinationPartitionId).isEmpty
                      )
                    )
                    .links(
                      LinkSetExpectation
                        .count(2)
                        .and(
                          LinkSetExpectation.forall(
                            LinkExpectation.any.attributesSubset(
                              MessagingExperimentalAttributes.MessagingDestinationPartitionId("0")
                            )
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

  test("configured server address is emitted on spans") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val config =
          KafkaTracer.Config.default
            .withServerAddress("kafka.internal", Some(9092))

        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer, config)
          _ <- tracedProducer
            .produce(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
            .flatten
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      ServerAttributes.ServerAddress("kafka.internal"),
                      ServerAttributes.ServerPort(9092L)
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("custom send span setup overrides span name, attributes, and finalization behavior") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val config =
          KafkaTracer.Config.default
            .withConstAttributes(
              Attributes(
                MessagingExperimentalAttributes.MessagingOperationType("configured")
              )
            )
            .withSendSpanSetup { ctx =>
              KafkaTracer.Config.SpanSetup(
                spanName = s"publish ${ctx.topics.headOption.getOrElse("unknown")} ${ctx.recordCount}",
                attributes = Attributes(
                  MessagingExperimentalAttributes.MessagingOperationName("publish"),
                  MessagingExperimentalAttributes.MessagingOperationType("publish")
                ),
                finalizationStrategy = { case Resource.ExitCase.Succeeded =>
                  SpanFinalizer.addAttribute(
                    MessagingExperimentalAttributes.MessagingBatchMessageCount(
                      ctx.recordCount.toLong
                    )
                  )
                }
              )
            }

        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer, config)
          _ <- tracedProducer
            .produce(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
            .flatten
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("publish topic 1")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingOperationName("publish"),
                      MessagingExperimentalAttributes.MessagingOperationType("publish"),
                      MessagingExperimentalAttributes.MessagingBatchMessageCount(1L)
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("constant attributes override derived producer attributes on send spans") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val config =
          KafkaTracer.Config.default.withConstAttributes(
            Attributes(
              MessagingExperimentalAttributes.MessagingDestinationName("configured-topic"),
              MessagingExperimentalAttributes.MessagingClientId("configured-client"),
              MessagingExperimentalAttributes.MessagingOperationName("configured-send")
            )
          )

        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer, config)
          _ <- tracedProducer
            .produce(ProducerRecords.one(ProducerRecord("actual-topic", "key", "value")))
            .flatten
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send actual-topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingDestinationName("configured-topic"),
                      MessagingExperimentalAttributes.MessagingClientId("configured-client"),
                      MessagingExperimentalAttributes.MessagingOperationName("configured-send")
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("producer send spans omit endpoint metadata unless configured explicitly") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producerTracer <- testkit.tracedProducer[String, String](
            StubKafkaProducer.metadataOnly("producer-client")
          )
          _ <- producerTracer
            .produce(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
            .flatten
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .attributes(
                      AttributesExpectation.where(
                        "must omit endpoint metadata unless configured explicitly"
                      )(attrs =>
                        attrs.get(ServerAttributes.ServerAddress).isEmpty &&
                          attrs.get(ServerAttributes.ServerPort).isEmpty
                      )
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("single-message send omits messaging.kafka.message.key for null and unrepresentable keys") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        implicit val unrepresentableKey: KafkaMessageKey[UnrepresentableKey] =
          KafkaMessageKey.Noop.noneKafkaMessageKey

        for {
          nullProducer <- StubKafkaProducer.recorder[String, String]()
          badProducer <- StubKafkaProducer.recorder[UnrepresentableKey, String]()
          stringProducerTracer <- testkit.tracedProducer[String, String](nullProducer)
          badProducerTracer <- testkit.tracedProducer[UnrepresentableKey, String](badProducer)
          _ <- stringProducerTracer
            .produce(
              ProducerRecords.one(ProducerRecord("topic", null.asInstanceOf[String], "value"))
            )
            .flatten
          _ <- badProducerTracer
            .produce(
              ProducerRecords.one(
                ProducerRecord("topic", new UnrepresentableKey("secret"), "value")
              )
            )
            .flatten
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .attributes(
                      AttributesExpectation.where("must omit message key for null keys")(
                        _.get(MessagingExperimentalAttributes.MessagingKafkaMessageKey).isEmpty
                      )
                    )
                ),
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .attributes(
                      AttributesExpectation.where(
                        "must omit message key for unrepresentable keys"
                      )(
                        _.get(MessagingExperimentalAttributes.MessagingKafkaMessageKey).isEmpty
                      )
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("single-message send emits messaging.kafka.message.tombstone for null values") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          producerTracer <- testkit.tracedProducer[String, String](producer)
          _ <- producerTracer
            .produce(ProducerRecords.one(ProducerRecord("topic", "key", null.asInstanceOf[String])))
            .flatten
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingKafkaMessageTombstone(true)
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("tracedWithSerializers preserves tracing with a changed key type") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        implicit val wrappedKeyMessageKey: KafkaMessageKey[WrappedKey] =
          KafkaMessageKey.instance(key => Some(s"wrapped:${key.value}"))

        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          remapped = tracedProducer.tracedWithSerializers(
            Serializer[IO, String].contramap[WrappedKey](_.value),
            Serializer[IO, String]
          )
          _ <- remapped
            .produce(
              ProducerRecords.one(
                ProducerRecord("topic", new WrappedKey("key"), "value")
              )
            )
            .flatten
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .attributesSubset(
                      MessagingExperimentalAttributes.MessagingKafkaMessageKey("wrapped:key")
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

  test("withSerializers preserves tracing but drops message-key extraction for a changed key type") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          remapped = remapWithSerializers(tracedProducer)
          _ <- remapped
            .produce(
              ProducerRecords.one(
                ProducerRecord("topic", new WrappedKey("key"), "value")
              )
            )
            .flatten
          spans <- testkit.finishedSpans
          _ <- IO {
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .attributes(
                      AttributesExpectation.where(
                        "must omit message key after withSerializers changes the key type"
                      )(
                        _.get(MessagingExperimentalAttributes.MessagingKafkaMessageKey).isEmpty
                      )
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

}

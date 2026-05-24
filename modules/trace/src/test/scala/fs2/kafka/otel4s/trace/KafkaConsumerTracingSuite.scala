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

import cats.effect.IO
import fs2.Chunk
import fs2.kafka._
import org.typelevel.otel4s.oteljava.testkit.trace._
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes
import org.typelevel.otel4s.trace.Tracer

final class KafkaConsumerTracingSuite extends KafkaTracingTestSupport {

  test("process links propagated context and emits a process span") {
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
          _ <- consumerTracer.process(consumed)(IO.unit)
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

  test("receive emits a poll span and links to message creation contexts") {
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
          _ <- consumerTracer.receive(records)(IO.unit)
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

}

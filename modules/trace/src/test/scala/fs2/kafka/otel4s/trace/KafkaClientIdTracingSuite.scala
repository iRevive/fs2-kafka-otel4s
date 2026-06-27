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

import java.util.Collections

import cats.effect.{IO, Ref}
import fs2.Chunk
import fs2.kafka._
import org.apache.kafka.common.{Metric, MetricName}
import org.typelevel.otel4s.oteljava.testkit.trace.{SpanExpectation, TraceForestExpectation}
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes

final class KafkaClientIdTracingSuite extends KafkaTracingTestSupport {

  test("producer and consumer spans use Kafka-generated client ids from metrics") {
    KafkaTracerTestkit.create().use { testkit =>
      for {
        producerMetricCalls <- Ref[IO].of(0)
        consumerMetricCalls <- Ref[IO].of(0)
        producer = producerWithRuntimeClientId("producer-42", producerMetricCalls)
        consumer = consumerWithRuntimeClientId("consumer-group-42", consumerMetricCalls)
        tracedProducer <- testkit.tracedProducer(producer)
        tracedConsumer <- testkit.tracedConsumer(consumer)
        _ <- tracedProducer
          .produce(ProducerRecords.one(ProducerRecord("topic-a", "key-a", "value-a")))
          .flatten
        _ <- tracedProducer
          .produce(ProducerRecords.one(ProducerRecord("topic-b", "key-b", "value-b")))
          .flatten
        record = ConsumerRecord("topic-c", 0, 1L, "key-c", "value-c")
        _ <- tracedConsumer.receive(Chunk(record))(IO.unit)
        _ <- tracedConsumer.process(record)(IO.unit)
        producerCalls <- producerMetricCalls.get
        consumerCalls <- consumerMetricCalls.get
        spans <- testkit.finishedSpans
        _ <- IO {
          assertEquals(producerCalls, 1)
          assertEquals(consumerCalls, 1)
          assertExpected(
            spans,
            TraceForestExpectation.unordered(
              root(
                SpanExpectation
                  .producer("send topic-a")
                  .attributesSubset(
                    MessagingExperimentalAttributes.MessagingClientId("producer-42")
                  )
              ),
              root(
                SpanExpectation
                  .producer("send topic-b")
                  .attributesSubset(
                    MessagingExperimentalAttributes.MessagingClientId("producer-42")
                  )
              ),
              root(
                SpanExpectation
                  .client("poll topic-c")
                  .attributesSubset(
                    MessagingExperimentalAttributes.MessagingClientId("consumer-group-42")
                  )
              ),
              root(
                SpanExpectation
                  .consumer("process topic-c")
                  .attributesSubset(
                    MessagingExperimentalAttributes.MessagingClientId("consumer-group-42")
                  )
              )
            )
          )
        }
      } yield ()
    }
  }

  private def producerWithRuntimeClientId(
      clientId: String,
      metricCalls: Ref[IO, Int]
  ): KafkaProducer.WithSettings[IO, String, String] = {
    val settings = ProducerSettings[IO, String, String](
      Serializer[IO, String],
      Serializer[IO, String]
    )

    new StubKafkaProducer[String, String](settings) {
      override def produce(
          records: ProducerRecords[String, String]
      ): IO[IO[ProducerResult[String, String]]] =
        IO.pure(IO.pure(Chunk.empty))

      override def metrics: IO[Map[MetricName, Metric]] =
        metricCalls.update(_ + 1).as(clientMetrics(clientId))
    }
  }

  private def consumerWithRuntimeClientId(
      clientId: String,
      metricCalls: Ref[IO, Int]
  ): KafkaConsumer[IO, String, String] = {
    val settings = ConsumerSettings[IO, String, String](
      GenericDeserializer.const[IO, String](null),
      GenericDeserializer.const[IO, String](null)
    ).withGroupId("consumer-group")

    new StubKafkaConsumer[String, String](settings) {
      override def metrics: IO[Map[MetricName, Metric]] =
        metricCalls.update(_ + 1).as(clientMetrics(clientId))
    }
  }

  private def clientMetrics(clientId: String): Map[MetricName, Metric] =
    Map(
      new MetricName(
        "test-metric",
        "test-group",
        "",
        Collections.singletonMap("client-id", clientId)
      ) -> null.asInstanceOf[Metric]
    )

}

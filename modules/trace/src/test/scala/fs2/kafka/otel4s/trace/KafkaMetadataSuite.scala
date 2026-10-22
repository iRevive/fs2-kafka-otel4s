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
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import fs2.kafka.{ConsumerSettings, GenericDeserializer, ProducerSettings, Serializer}
import org.apache.kafka.common.{Metric, MetricName}
import munit.CatsEffectSuite

final class KafkaMetadataSuite extends CatsEffectSuite {

  test("metadata helpers snapshot configured client and consumer group ids") {
    val consumerSettings = ConsumerSettings[IO, String, String](
      GenericDeserializer.const[IO, String](null),
      GenericDeserializer.const[IO, String](null)
    ).withClientId("configured-consumer").withGroupId("orders")
    for {
      consumer <- ConsumerMetadata.Provider.fromConsumer(new StubKafkaConsumer[String, String](consumerSettings)).get
      producer <- ProducerMetadata.Provider
        .fromProducer(StubKafkaProducer.metadataOnly[String, String]("configured-producer"))
        .get
    } yield {
      assertEquals(consumer, ConsumerMetadata(Some("configured-consumer"), Some("orders")))
      assertEquals(producer, ProducerMetadata(Some("configured-producer")))
    }
  }

  test("metadata derivation reads generated client ids from live client metrics") {
    val consumerSettings = ConsumerSettings[IO, String, String](
      GenericDeserializer.const[IO, String](null),
      GenericDeserializer.const[IO, String](null)
    ).withGroupId("orders")
    val producerSettings = ProducerSettings[IO, String, String](Serializer[IO, String], Serializer[IO, String])
    val consumer = new StubKafkaConsumer[String, String](consumerSettings) {
      override def metrics: IO[Map[MetricName, Metric]] = IO.pure(clientMetrics("generated-consumer"))
    }
    val producer = new StubKafkaProducer[String, String](producerSettings) {
      override def metrics: IO[Map[MetricName, Metric]] = IO.pure(clientMetrics("generated-producer"))
    }

    for {
      consumerMetadata <- ConsumerMetadata.Provider.fromConsumer(consumer).get
      producerMetadata <- ProducerMetadata.Provider.fromProducer(producer).get
    } yield {
      assertEquals(consumerMetadata, ConsumerMetadata(Some("generated-consumer"), Some("orders")))
      assertEquals(producerMetadata, ProducerMetadata(Some("generated-producer")))
    }
  }

  test("metadata helper converts metrics failures to missing client ids") {
    val consumerSettings = ConsumerSettings[IO, String, String](
      GenericDeserializer.const[IO, String](null),
      GenericDeserializer.const[IO, String](null)
    ).withGroupId("orders")
    val consumer = new StubKafkaConsumer[String, String](consumerSettings) {
      override def metrics: IO[Map[MetricName, Metric]] = IO.raiseError(new RuntimeException("metrics unavailable"))
    }
    val producerSettings = ProducerSettings[IO, String, String](Serializer[IO, String], Serializer[IO, String])
    val producer = new StubKafkaProducer[String, String](producerSettings) {
      override def metrics: IO[Map[MetricName, Metric]] = IO.raiseError(new RuntimeException("metrics unavailable"))
    }

    for {
      consumerMetadata <- ConsumerMetadata.Provider.fromConsumer(consumer).get
      producerMetadata <- ProducerMetadata.Provider.fromProducer(producer).get
    } yield {
      assertEquals(consumerMetadata, ConsumerMetadata(None, Some("orders")))
      assertEquals(producerMetadata, ProducerMetadata(None))
    }
  }

  test("client-bound providers cache resolved ids for their lifetime") {
    val consumerMetricsReads = new AtomicInteger(0)
    val producerMetricsReads = new AtomicInteger(0)
    val consumerSettings = ConsumerSettings[IO, String, String](
      GenericDeserializer.const[IO, String](null),
      GenericDeserializer.const[IO, String](null)
    ).withGroupId("orders")
    val producerSettings = ProducerSettings[IO, String, String](Serializer[IO, String], Serializer[IO, String])
    val consumer = new StubKafkaConsumer[String, String](consumerSettings) {
      override def metrics: IO[Map[MetricName, Metric]] =
        IO.delay(consumerMetricsReads.incrementAndGet()).map(_ => clientMetrics("generated-consumer"))
    }
    val producer = new StubKafkaProducer[String, String](producerSettings) {
      override def metrics: IO[Map[MetricName, Metric]] =
        IO.delay(producerMetricsReads.incrementAndGet()).map(_ => clientMetrics("generated-producer"))
    }
    val consumerProvider = ConsumerMetadata.Provider.fromConsumer(consumer)
    val producerProvider = ProducerMetadata.Provider.fromProducer(producer)

    for {
      consumerFirst <- consumerProvider.get
      consumerSecond <- consumerProvider.get
      producerFirst <- producerProvider.get
      producerSecond <- producerProvider.get
    } yield {
      assertEquals(consumerFirst, ConsumerMetadata(Some("generated-consumer"), Some("orders")))
      assertEquals(consumerSecond, consumerFirst)
      assertEquals(consumerMetricsReads.get(), 1)
      assertEquals(producerFirst, ProducerMetadata(Some("generated-producer")))
      assertEquals(producerSecond, producerFirst)
      assertEquals(producerMetricsReads.get(), 1)
    }
  }

  test("constant providers return the supplied metadata snapshot") {
    val consumer = ConsumerMetadata(Some("consumer"), Some("orders"))
    val producer = ProducerMetadata(Some("producer"))
    for {
      consumerResult <- ConsumerMetadata.Provider.constant[IO](consumer).get
      producerResult <- ProducerMetadata.Provider.constant[IO](producer).get
    } yield {
      assertEquals(consumerResult, consumer)
      assertEquals(producerResult, producer)
    }
  }

  private def clientMetrics(clientId: String): Map[MetricName, Metric] =
    Map(
      new MetricName("test-metric", "test-group", "", Collections.singletonMap("client-id", clientId)) ->
        null.asInstanceOf[Metric]
    )

}

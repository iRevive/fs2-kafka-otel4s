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
import fs2.Stream
import fs2.kafka._
import fs2.kafka.otel4s.trace.syntax._
import org.typelevel.otel4s.trace.TracerProvider

final class KafkaProducerSyntaxSuite extends KafkaTracingTestSupport {

  test("Stream[KafkaProducer].traced binds tracing and yields traced producers") {
    for {
      expected <- IO(TestTracedProducer(StubKafkaProducer.metadataOnly[String, String]()))
      underlying = StubKafkaProducer.metadataOnly[String, String]()
      tracer = new KafkaTracer[IO] {
        override def producer[K: KafkaMessageKey, V](
            producer: KafkaProducer.WithSettings[IO, K, V]
        ): TracedKafkaProducer[IO, K, V] =
          expected.asInstanceOf[TracedKafkaProducer[IO, K, V]]
      }
      result <- Stream
        .emit(underlying)
        .traced(tracer)
        .compile
        .toList
    } yield assertEquals(result, List(expected))
  }

  test("Stream[KafkaProducer].traced(config) creates a tracer and yields traced producers") {
    KafkaTracerTestkit.create().use { testkit =>
      val underlying = StubKafkaProducer.metadataOnly[String, String]()

      implicit val tracerProvider: TracerProvider[IO] = testkit.tracerProvider

      for {
        traced <- Stream
          .emit(underlying)
          .traced(KafkaTracer.Config.default)
          .compile
          .onlyOrError
        produced <- traced
          .produce(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
          .flatten
      } yield assertEquals(produced.size, 1)
    }
  }

  private final case class TestTracedProducer[K, V](
      underlying: KafkaProducer.WithSettings[IO, K, V]
  ) extends TracedKafkaProducer[IO, K, V] {

    override def settings: ProducerSettings[IO, K, V] =
      underlying.settings

    override def injectHeaders(
        record: ProducerRecord[K, V]
    ): IO[ProducerRecord[K, V]] =
      IO.raiseError(new AssertionError("unexpected injectHeaders(record)"))

    override def injectHeaders(
        records: ProducerRecords[K, V]
    ): IO[ProducerRecords[K, V]] =
      IO.raiseError(new AssertionError("unexpected injectHeaders(records)"))

    override def produce(
        records: ProducerRecords[K, V]
    ): IO[IO[ProducerResult[K, V]]] =
      IO.raiseError(new AssertionError("unexpected produce"))

    override def initTransactions: IO[Unit] =
      IO.raiseError(new AssertionError("unexpected initTransactions"))

    override def transaction =
      underlying.transaction

    override def sendOffsetsToTransaction(
        offsets: Map[org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata],
        groupMetadata: org.apache.kafka.clients.consumer.ConsumerGroupMetadata
    ): IO[Unit] =
      IO.raiseError(new AssertionError("unexpected sendOffsetsToTransaction"))

    override def produceAndCommitTransactionally(
        records: TransactionalProducerRecords[IO, K, V]
    ): IO[ProducerResult[K, V]] =
      IO.raiseError(new AssertionError("unexpected produceAndCommitTransactionally"))

    override def produceTransactionally(
        records: ProducerRecords[K, V]
    ): IO[ProducerResult[K, V]] =
      IO.raiseError(new AssertionError("unexpected produceTransactionally"))

    override def metrics =
      underlying.metrics

    override def partitionsFor(topic: String) =
      underlying.partitionsFor(topic)

    override def withSerializers[K2, V2](
        keySerializer: KeySerializer[IO, K2],
        valueSerializer: ValueSerializer[IO, V2]
    ): KafkaProducer.WithSettings[IO, K2, V2] =
      underlying.withSerializers(keySerializer, valueSerializer)

    override def tracedWithSerializers[K2: KafkaMessageKey, V2](
        keySerializer: KeySerializer[IO, K2],
        valueSerializer: ValueSerializer[IO, V2]
    ): TracedKafkaProducer[IO, K2, V2] =
      TestTracedProducer(underlying.withSerializers(keySerializer, valueSerializer))
        .asInstanceOf[TracedKafkaProducer[IO, K2, V2]]

  }

}

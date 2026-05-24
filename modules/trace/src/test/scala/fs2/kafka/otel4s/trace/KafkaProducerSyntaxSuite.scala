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
    KafkaTracerTestkit.create().use { testkit =>
      val underlying = StubKafkaProducer.metadataOnly[String, String]()

      for {
        expected <- testkit.tracedProducer(underlying)
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
          .onlyOrError
      } yield assert(result eq expected)
    }
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
}

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
import fs2.kafka.{ProducerRecord, ProducerRecords}

final class KafkaTracerSuite extends KafkaTracingTestSupport {

  test("noop producer delegates without injecting tracing headers") {
    for {
      underlying <- StubKafkaProducer.recorder[String, String]()
      producer = KafkaTracer.noop[IO].producer(underlying)
      _ <- producer
        .produce(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
        .flatten
      captured <- underlying.getCaptured
    } yield {
      assertEquals(captured.size, 1)
      assertEquals(captured.head.get.headers.toChain.toList, Nil)
    }
  }

  test("TracedKafkaProducer.noop creates a no-op producer directly") {
    for {
      underlying <- StubKafkaProducer.recorder[String, String]()
      producer = TracedKafkaProducer.noop(underlying)
      _ <- producer
        .produce(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
        .flatten
      captured <- underlying.getCaptured
    } yield assertEquals(captured.head.get.headers.toChain.toList, Nil)
  }

  test("noop consumer binds the underlying consumer") {
    val underlying = StubKafkaConsumer.metadataOnly[String, String]()
    val consumer = KafkaTracer.noop[IO].consumer(underlying)

    assert(consumer.underlying eq underlying)
  }

  test("TracedKafkaConsumer.noop creates a no-op consumer directly") {
    val underlying = StubKafkaConsumer.metadataOnly[String, String]()
    val consumer = TracedKafkaConsumer.noop(underlying)
    val record = fs2.kafka.ConsumerRecord("topic", 0, 0L, "key", "value")

    consumer.process(record)(IO.pure("result")).map { result =>
      assert(consumer.underlying eq underlying)
      assertEquals(result, "result")
    }
  }

}

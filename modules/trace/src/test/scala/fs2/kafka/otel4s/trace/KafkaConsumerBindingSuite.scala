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

import fs2.kafka.ConsumerRecord

final class KafkaConsumerBindingSuite extends KafkaTracingTestSupport {

  test("traced consumer records delegates to underlying.records") {
    KafkaTracerTestkit.create().use { testkit =>
      val records = List(StubKafkaConsumer.committableRecord(ConsumerRecord("topic", 0, 1L, "k", "v")))
      val consumer = StubKafkaConsumer.streaming(records)

      for {
        traced <- testkit.tracedConsumer[String, String](consumer)
        got <- traced.records.compile.toList
      } yield assertEquals(got.map(_.record), records.map(_.record))
    }
  }

  test("traced consumer partitionedRecords delegates to underlying.partitionedRecords") {
    KafkaTracerTestkit.create().use { testkit =>
      val records = List(StubKafkaConsumer.committableRecord(ConsumerRecord("topic", 0, 1L, "k", "v")))
      val consumer = StubKafkaConsumer.streaming(records)

      for {
        traced <- testkit.tracedConsumer[String, String](consumer)
        got <- traced.partitionedRecords.evalMap(_.compile.toList).compile.toList
      } yield assertEquals(got.map(_.map(_.record)), List(records.map(_.record)))
    }
  }

  test("traced consumer partitionedStream delegates to underlying.partitionedStream") {
    KafkaTracerTestkit.create().use { testkit =>
      val records = List(StubKafkaConsumer.committableRecord(ConsumerRecord("topic", 0, 1L, "k", "v")))
      val consumer = StubKafkaConsumer.streaming(records)

      for {
        traced <- testkit.tracedConsumer[String, String](consumer)
        got <- traced.partitionedStream.evalMap(_.compile.toList).compile.toList
      } yield assertEquals(got.map(_.map(_.record)), List(records.map(_.record)))
    }
  }

}

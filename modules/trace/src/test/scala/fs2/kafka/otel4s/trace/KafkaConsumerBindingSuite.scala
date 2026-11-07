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

final class KafkaConsumerBindingSuite extends KafkaTracingTestSupport {

  test("traced consumer exposes its underlying consumer") {
    KafkaTracerTestkit.create().use { testkit =>
      val consumer = StubKafkaConsumer.metadataOnly[String, String]()
      testkit.tracedConsumer[String, String](consumer).map { traced =>
        assert(traced.underlying eq consumer)
      }
    }
  }

}

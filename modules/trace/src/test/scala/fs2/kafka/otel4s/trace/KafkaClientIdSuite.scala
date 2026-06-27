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
import fs2.kafka.otel4s.trace.internal.KafkaClientId
import munit.CatsEffectSuite
import org.apache.kafka.common.{Metric, MetricName}

final class KafkaClientIdSuite extends CatsEffectSuite {

  test("configured client id takes precedence without reading metrics") {
    for {
      metricCalls <- Ref[IO].of(0)
      clientId = KafkaClientId[IO](
        Some("configured-client"),
        metricCalls.update(_ + 1).as(clientMetrics("runtime-client"))
      )
      first <- clientId.get
      second <- clientId.get
      calls <- metricCalls.get
    } yield {
      assertEquals(first, Some("configured-client"))
      assertEquals(second, Some("configured-client"))
      assertEquals(calls, 0)
    }
  }

  test("client id is resolved from metrics and cached") {
    for {
      metricCalls <- Ref[IO].of(0)
      clientId = KafkaClientId[IO](
        None,
        metricCalls.update(_ + 1).as(clientMetrics("runtime-client"))
      )
      first <- clientId.get
      second <- clientId.get
      calls <- metricCalls.get
    } yield {
      assertEquals(first, Some("runtime-client"))
      assertEquals(second, Some("runtime-client"))
      assertEquals(calls, 1)
    }
  }

  test("metrics failure is treated as a cached missing client id") {
    val failure = new RuntimeException("metrics unavailable")

    for {
      metricCalls <- Ref[IO].of(0)
      clientId = KafkaClientId[IO](
        None,
        metricCalls.update(_ + 1) *> IO.raiseError(failure)
      )
      first <- clientId.get
      second <- clientId.get
      calls <- metricCalls.get
    } yield {
      assertEquals(first, None)
      assertEquals(second, None)
      assertEquals(calls, 1)
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

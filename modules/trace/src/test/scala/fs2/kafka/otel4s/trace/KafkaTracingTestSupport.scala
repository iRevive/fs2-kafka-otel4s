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

import io.opentelemetry.sdk.trace.data.SpanData
import munit.Location
import munit.CatsEffectSuite
import org.typelevel.otel4s.oteljava.testkit.trace.{
  SpanExpectation,
  TraceExpectation,
  TraceExpectations,
  TraceForestExpectation
}

trait KafkaTracingTestSupport extends CatsEffectSuite {

  protected def root(
      span: SpanExpectation,
      children: TraceExpectation*
  ): TraceExpectation =
    TraceExpectation.ordered(
      span.noParentSpanContext,
      children *
    )

  protected def assertExpected(
      spans: List[SpanData],
      expected: TraceForestExpectation
  )(implicit loc: Location): Unit =
    TraceExpectations.check(spans, expected) match {
      case Right(_) =>
        ()
      case Left(mismatches) =>
        fail(TraceExpectations.format(mismatches))
    }

}

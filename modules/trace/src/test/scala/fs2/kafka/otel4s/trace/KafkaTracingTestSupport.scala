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
      children*
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

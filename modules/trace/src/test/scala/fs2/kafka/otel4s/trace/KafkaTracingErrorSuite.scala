package fs2.kafka.otel4s.trace

import cats.effect.IO
import fs2.kafka.{ProducerRecord, ProducerRecords}
import org.typelevel.otel4s.oteljava.testkit.trace.{
  SpanExpectation,
  StatusExpectation,
  TraceForestExpectation
}
import org.typelevel.otel4s.semconv.attributes.ErrorAttributes

final class KafkaTracingErrorSuite extends KafkaTracingTestSupport {

  test("failed send emits error.type and error status") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          tracedProducer <- testkit.tracedProducer(
            StubKafkaProducer.failingAwait[String, String](
              new RuntimeException("send failed")
            )
          )
          result <- tracedProducer
            .produceAwaited(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
            .attempt
          spans <- testkit.finishedSpans
          _ <- IO {
            assert(result.isLeft)
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .status(StatusExpectation.error)
                    .attributesSubset(
                      ErrorAttributes.ErrorType("java.lang.RuntimeException")
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

}

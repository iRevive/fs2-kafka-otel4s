package fs2.kafka.otel4s.trace

import cats.effect.{IO, Outcome}
import fs2.kafka.{ProducerRecord, ProducerRecords}
import org.typelevel.otel4s.oteljava.testkit.trace.{
  SpanExpectation,
  StatusExpectation,
  TraceForestExpectation
}
import org.typelevel.otel4s.semconv.attributes.ErrorAttributes

final class KafkaTracingErrorSuite extends KafkaTracingTestSupport {

  test("outer produce-stage failure emits error.type and error status") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          tracedProducer <- testkit.tracedProducer(
                              StubKafkaProducer.failingOuter[String, String](
                                new RuntimeException("stage failed")
                              )
                            )
          result <- tracedProducer
            .produce(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
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

  test("inner await-stage failure emits error.type and error status") {
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
            .produce(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
            .flatten
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

  test("canceling while waiting on the inner await effect finalizes the send span as canceled") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        for {
          producer <- StubKafkaProducer.gatedAwait[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          fiber <- tracedProducer
            .produce(ProducerRecords.one(ProducerRecord("topic", "key", "value")))
            .flatten
            .start
          _ <- producer.awaitStarted
          produced <- producer.getCaptured
          spansBeforeCancel <- testkit.finishedSpans
          _ <- fiber.cancel
          _ <- producer.awaitCanceled
          outcome <- fiber.join
          completions <- producer.getCompletions
          spans <- testkit.finishedSpans
          _ <- IO {
            assertEquals(produced.size, 1)
            assert(produced.head.get.headers.toChain.nonEmpty)
            assertEquals(spansBeforeCancel, Nil)
            outcome match {
              case Outcome.Canceled() =>
                ()
              case other =>
                fail(s"expected canceled outcome, got: $other")
            }
            assertEquals(completions, 0)
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("send topic")
                    .scopeName("fs2.kafka")
                    .status(StatusExpectation.error)
                    .attributesSubset(
                      ErrorAttributes.ErrorType("canceled")
                    )
                )
              )
            )
          }
        } yield ()
      }
  }

}

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

import cats.effect.{IO, Outcome}
import cats.mtl.LiftKind
import cats.{Applicative, ~>}
import fs2.Chunk
import fs2.kafka.{ProducerRecord, ProducerRecords}
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.{TextMapGetter => JTextMapGetter, TextMapPropagator, TextMapSetter}
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.{ReadWriteSpan, ReadableSpan, SpanProcessor}
import org.typelevel.otel4s.oteljava.testkit.trace.{SpanExpectation, StatusExpectation, TraceForestExpectation}
import org.typelevel.otel4s.semconv.attributes.ErrorAttributes
import org.typelevel.otel4s.trace.Tracer

import java.util.{Collection, Collections}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

final class KafkaTracingErrorSuite extends KafkaTracingTestSupport {
  import KafkaTracingErrorSuite._

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

  test("later preparation failure only marks the failing create span and releases each span once") {
    val spanTracker = new SpanLifecycleTracker
    val failingPropagator = new FailingOnNthInjectPropagator(2)

    KafkaTracerTestkit
      .create(
        propagators = Seq(W3CTraceContextPropagator.getInstance(), failingPropagator),
        tracerProviderCustomizer = _.addSpanProcessor(spanTracker.processor)
      )
      .use { testkit =>
        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          result <- tracedProducer
            .produce(
              ProducerRecords(
                Chunk(
                  ProducerRecord("topic-a", "key-a", "value-a"),
                  ProducerRecord("topic-b", "key-b", "value-b")
                )
              )
            )
            .attempt
          produced <- producer.getCaptured
          spans <- testkit.finishedSpans
          startedNames <- spanTracker.startedNames
          endedNames <- spanTracker.endedNames
          _ <- IO {
            assert(result.isLeft)
            assertEquals(produced, Chunk.empty)
            assertEquals(startedNames.sorted, List("create topic-a", "create topic-b"))
            assertEquals(endedNames.sorted, startedNames.sorted)
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .producer("create topic-a")
                    .status(StatusExpectation.unset)
                    .eventCount(0)
                    .where("must end before the next create span starts") { first =>
                      spans
                        .filter(_.getName == "create topic-b")
                        .forall(second => first.getEndEpochNanos <= second.getStartEpochNanos)
                    }
                ),
                root(
                  SpanExpectation
                    .producer("create topic-b")
                    .eventCount(1)
                    .status(StatusExpectation.error)
                    .attributesSubset(ErrorAttributes.ErrorType("java.lang.RuntimeException"))
                )
              )
            )
          }
        } yield ()
      }
  }

  test("cancellation during later preparation finalizes only the active create as canceled") {
    val cancelAfterInjection = new AtomicBoolean(false)
    val injections = new AtomicInteger(0)
    val spanTracker = new SpanLifecycleTracker
    val propagator = new TextMapPropagator {
      def fields(): Collection[String] = Collections.emptyList[String]()
      def extract[C](context: Context, carrier: C, getter: JTextMapGetter[C]): Context = context
      def inject[C](context: Context, carrier: C, setter: TextMapSetter[C]): Unit = {
        if (injections.incrementAndGet() == 2) cancelAfterInjection.set(true)
      }
    }

    KafkaTracerTestkit
      .create(
        propagators = Seq(W3CTraceContextPropagator.getInstance(), propagator),
        tracerProviderCustomizer = _.addSpanProcessor(spanTracker.processor)
      )
      .use { testkit =>
        // Make the next tracing effect cancel at the end of the second injection, inside its create scope.
        val cancelOnInjection = new LiftKind[IO, IO] {
          val applicativeF: Applicative[IO] = IO.asyncForIO
          val applicativeG: Applicative[IO] = IO.asyncForIO
          def apply[A](fa: IO[A]): IO[A] = fa.flatTap { _ =>
            IO(cancelAfterInjection.getAndSet(false)).flatMap { cancel =>
              if (cancel) IO.canceled *> IO.never[Unit] else IO.unit
            }
          }
          def limitedMapK[A](fa: IO[A])(scope: IO ~> IO): IO[A] = scope(fa)
        }
        implicit val tracer: Tracer[IO] = testkit.appTracer.liftTo[IO](
          IO.asyncForIO,
          IO.asyncForIO,
          cancelOnInjection
        )
        val spans = ProducerSpans[IO](
          ProducerMetadata.Provider.constant[IO](ProducerMetadata(None)),
          KafkaTracer.Config.default
        )
        for {
          fiber <- spans
            .produce(
              ProducerRecords(
                Chunk(
                  ProducerRecord("topic-a", "a", "value"),
                  ProducerRecord("topic-b", "b", "value")
                )
              )
            )(_ => IO.raiseError(new AssertionError("submission must not run")))
            .start
          outcome <- fiber.join
          finished <- testkit.finishedSpans
          started <- spanTracker.startedNames
          ended <- spanTracker.endedNames
        } yield {
          assert(outcome.isCanceled)
          assertEquals(started.sorted, List("create topic-a", "create topic-b"))
          assertEquals(ended.sorted, started.sorted)
          assertExpected(
            finished,
            TraceForestExpectation.unordered(
              root(SpanExpectation.producer("create topic-a").status(StatusExpectation.unset)),
              root(
                SpanExpectation
                  .producer("create topic-b")
                  .status(StatusExpectation.error)
                  .attributesSubset(ErrorAttributes.ErrorType("canceled"))
              )
            )
          )
        }
      }
  }

}

object KafkaTracingErrorSuite {

  final class FailingOnNthInjectPropagator(failOn: Int) extends TextMapPropagator {

    private val counter = new AtomicInteger(0)

    override def fields(): Collection[String] =
      Collections.singletonList("x-failing-propagation")

    override def inject[C](context: Context, carrier: C, setter: TextMapSetter[C]): Unit = {
      val invocation = counter.incrementAndGet()

      if (invocation == failOn) {
        throw new RuntimeException(s"inject failed on invocation $invocation")
      }

      setter.set(carrier, "x-failing-propagation", s"value-$invocation")
    }

    override def extract[C](context: Context, carrier: C, getter: JTextMapGetter[C]): Context =
      context

  }

  final class SpanLifecycleTracker {

    private val started = new ConcurrentLinkedQueue[String]()
    private val ended = new ConcurrentLinkedQueue[String]()

    val processor: SpanProcessor = new SpanProcessor {
      override def onStart(parentContext: Context, span: ReadWriteSpan): Unit = {
        started.add(span.getName)
        ()
      }

      override def isStartRequired(): Boolean =
        true

      override def onEnd(span: ReadableSpan): Unit = {
        ended.add(span.getName)
        ()
      }

      override def isEndRequired(): Boolean =
        true

      override def shutdown(): CompletableResultCode =
        CompletableResultCode.ofSuccess()

      override def forceFlush(): CompletableResultCode =
        CompletableResultCode.ofSuccess()
    }

    def startedNames: IO[List[String]] =
      IO(started.iterator().asScala.toList)

    def endedNames: IO[List[String]] =
      IO(ended.iterator().asScala.toList)

  }

}

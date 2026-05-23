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
import fs2.kafka._
import fs2.kafka.otel4s.trace.instances._
import org.typelevel.otel4s.context.propagation.{TextMapGetter, TextMapUpdater}
import org.typelevel.otel4s.oteljava.testkit.trace.{SpanExpectation, TraceForestExpectation}

final class KafkaPropagationSuite extends KafkaTracingTestSupport {

  test("headers text map updater replaces existing values for the same key") {
    val headers =
      TextMapUpdater[Headers].updated(
        TextMapUpdater[Headers].updated(
          Headers.empty,
          "traceparent",
          "old"
        ),
        "traceparent",
        "new"
      )

    assertEquals(
      TextMapGetter[Headers].get(headers, "traceparent"),
      Some("new")
    )
    assertEquals(headers.toChain.iterator.count(_.key == "traceparent"), 1)
  }

  test("headers text map getter prefers the last matching value") {
    val headers = Headers.fromSeq(
      Seq(
        Header("traceparent", "first"),
        Header("other", "value"),
        Header("traceparent", "second")
      )
    )

    assertEquals(
      TextMapGetter[Headers].get(headers, "traceparent"),
      Some("second")
    )
  }

  test("headers text map getter returns None when the last matching value is null") {
    val headers = Headers.fromSeq(
      Seq(
        Header("traceparent", "first"),
        Header("traceparent", null.asInstanceOf[Array[Byte]])
      )
    )

    assertEquals(
      TextMapGetter[Headers].get(headers, "traceparent"),
      None
    )
  }

  test("headers text map updater keeps unrelated duplicate headers intact") {
    val headers =
      TextMapUpdater[Headers].updated(
        Headers.fromSeq(
          Seq(
            Header("traceparent", "old"),
            Header("baggage", "k1=v1"),
            Header("baggage", "k2=v2")
          )
        ),
        "traceparent",
        "new"
      )

    assertEquals(
      TextMapGetter[Headers].get(headers, "traceparent"),
      Some("new")
    )
    assertEquals(headers.toChain.iterator.count(_.key == "traceparent"), 1)
    assertEquals(headers.toChain.iterator.count(_.key == "baggage"), 2)
  }

  test("headers text map updater collapses duplicate values for the updated key to one latest value") {
    val headers =
      TextMapUpdater[Headers].updated(
        Headers.fromSeq(
          Seq(
            Header("traceparent", "first"),
            Header("traceparent", "second"),
            Header("other", "value")
          )
        ),
        "traceparent",
        "final"
      )

    assertEquals(
      TextMapGetter[Headers].get(headers, "traceparent"),
      Some("final")
    )
    assertEquals(headers.toChain.iterator.count(_.key == "traceparent"), 1)
    assertEquals(headers.toChain.iterator.count(_.key == "other"), 1)
  }

  test("produce preserves an existing message creation context and links the send span to it") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        import testkit.appTracer

        for {
          producer <- StubKafkaProducer.recorder[String, String]()
          tracedProducer <- testkit.tracedProducer(producer)
          prepared <- appTracer.rootSpan("upstream-create-context").surround {
            tracedProducer.injectHeaders(ProducerRecord("topic", "key", "value"))
          }
          originalTraceparent = TextMapGetter[Headers]
            .get(prepared.headers, "traceparent")
            .getOrElse(fail("missing prepared traceparent"))
          _ <- tracedProducer.produce(ProducerRecords.one(prepared)).flatten
          produced <- producer.getCaptured
          spans <- testkit.finishedSpans
          _ <- IO {
            val producedRecord = produced.head.getOrElse(fail("missing produced record"))
            val producedTraceparent = TextMapGetter[Headers]
              .get(producedRecord.headers, "traceparent")
              .getOrElse(fail("missing produced traceparent"))

            assertEquals(producedTraceparent, originalTraceparent)
            assertExpected(
              spans,
              TraceForestExpectation.unordered(
                root(
                  SpanExpectation
                    .internal("upstream-create-context")
                    .scopeName("fs2.kafka.otel4s.tests")
                ),
                root(
                  SpanExpectation.client("send topic").scopeName("fs2.kafka").linkCount(1)
                )
              )
            )
          }
        } yield ()
      }
  }

  test("injectHeaders preserves an existing message creation context") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val producerTracer =
          testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer-client"))

        for {
          tracedProducer <- producerTracer
          prepared <- testkit.appTracer.rootSpan("upstream-create-context").surround {
            tracedProducer.injectHeaders(ProducerRecord("topic", "key", "value"))
          }
          originalTraceparent = TextMapGetter[Headers]
            .get(prepared.headers, "traceparent")
            .getOrElse(fail("missing prepared traceparent"))
          reinjected <- testkit.appTracer.rootSpan("different-current-context").surround {
            tracedProducer.injectHeaders(prepared)
          }
          _ <- IO {
            val reinjectedTraceparent = TextMapGetter[Headers]
              .get(reinjected.headers, "traceparent")
              .getOrElse(fail("missing reinjected traceparent"))

            assertEquals(reinjectedTraceparent, originalTraceparent)
            assertEquals(reinjected.headers.toChain.iterator.count(_.key == "traceparent"), 1)
          }
        } yield ()
      }
  }

  test("injectHeaders preserves the last matching propagated context when duplicate headers exist") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val producerTracer =
          testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer-client"))

        for {
          tracedProducer <- producerTracer
          firstPrepared <- testkit.appTracer.rootSpan("first-context").surround {
            tracedProducer.injectHeaders(ProducerRecord("topic", "key", "value"))
          }
          secondPrepared <- testkit.appTracer.rootSpan("second-context").surround {
            tracedProducer.injectHeaders(ProducerRecord("topic", "key", "value"))
          }
          duplicated = ProducerRecord("topic", "key", "value").withHeaders(
            Headers.fromSeq(
              Seq(
                Header(
                  "traceparent",
                  TextMapGetter[Headers]
                    .get(firstPrepared.headers, "traceparent")
                    .getOrElse(fail("missing first traceparent"))
                ),
                Header(
                  "traceparent",
                  TextMapGetter[Headers]
                    .get(secondPrepared.headers, "traceparent")
                    .getOrElse(fail("missing second traceparent"))
                )
              )
            )
          )
          reinjected <- testkit.appTracer.rootSpan("different-current-context").surround {
            tracedProducer.injectHeaders(duplicated)
          }
          _ <- IO {
            val reinjectedTraceparent = TextMapGetter[Headers]
              .get(reinjected.headers, "traceparent")
              .getOrElse(fail("missing reinjected traceparent"))

            assertEquals(
              reinjectedTraceparent,
              TextMapGetter[Headers]
                .get(secondPrepared.headers, "traceparent")
                .getOrElse(fail("missing second traceparent"))
            )
            assertEquals(reinjected.headers.toChain.iterator.count(_.key == "traceparent"), 2)
          }
        } yield ()
      }
  }

  test("injectHeaders preserves an existing non-W3C creation context recognized by the configured propagator") {
    KafkaTracerTestkit
      .create(propagators = Seq(CustomTraceContextPropagator))
      .use { testkit =>
        val producerTracer =
          testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer-client"))

        for {
          tracedProducer <- producerTracer
          prepared <- testkit.appTracer.rootSpan("upstream-custom-context").surround {
            tracedProducer.injectHeaders(ProducerRecord("topic", "key", "value"))
          }
          originalCustomHeader = TextMapGetter[Headers]
            .get(prepared.headers, CustomTraceContextPropagator.customPropagationHeader)
            .getOrElse(fail("missing prepared custom propagation header"))
          reinjected <- testkit.appTracer.rootSpan("different-current-context").surround {
            tracedProducer.injectHeaders(prepared)
          }
          _ <- IO {
            val reinjectedCustomHeader = TextMapGetter[Headers]
              .get(reinjected.headers, CustomTraceContextPropagator.customPropagationHeader)
              .getOrElse(fail("missing reinjected custom propagation header"))

            assertEquals(reinjectedCustomHeader, originalCustomHeader)
            assertEquals(
              reinjected.headers.toChain.iterator.count(
                _.key == CustomTraceContextPropagator.customPropagationHeader
              ),
              1
            )
            assertEquals(TextMapGetter[Headers].get(reinjected.headers, "traceparent"), None)
          }
        } yield ()
      }
  }

  test("injectHeaders replaces malformed propagation input with the current context") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val producerTracer =
          testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer-client"))

        val malformed = ProducerRecord("topic", "key", "value").withHeaders(
          Headers(Header("traceparent", "00-not-a-valid-traceparent"))
        )

        for {
          tracedProducer <- producerTracer
          propagated <- testkit.appTracer.rootSpan("replacement-context").surround {
            tracedProducer.injectHeaders(malformed)
          }
          _ <- IO {
            val traceparent = TextMapGetter[Headers]
              .get(propagated.headers, "traceparent")
              .getOrElse(fail("missing propagated traceparent"))

            assertNotEquals(traceparent, "00-not-a-valid-traceparent")
            assertEquals(propagated.headers.toChain.iterator.count(_.key == "traceparent"), 1)
          }
        } yield ()
      }
  }

  test("injectHeaders treats the last malformed duplicate as authoritative and replaces it") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val producerTracer =
          testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer-client"))

        for {
          tracedProducer <- producerTracer
          validPrepared <- testkit.appTracer.rootSpan("valid-earlier-context").surround {
            tracedProducer.injectHeaders(ProducerRecord("topic", "key", "value"))
          }
          validTraceparent = TextMapGetter[Headers]
            .get(validPrepared.headers, "traceparent")
            .getOrElse(fail("missing valid traceparent"))
          duplicated = ProducerRecord("topic", "key", "value").withHeaders(
            Headers.fromSeq(
              Seq(
                Header("traceparent", validTraceparent),
                Header("traceparent", "00-not-a-valid-traceparent")
              )
            )
          )
          propagated <- testkit.appTracer.rootSpan("replacement-context").surround {
            tracedProducer.injectHeaders(duplicated)
          }
          _ <- IO {
            val traceparent = TextMapGetter[Headers]
              .get(propagated.headers, "traceparent")
              .getOrElse(fail("missing propagated traceparent"))

            assertNotEquals(traceparent, validTraceparent)
            assertNotEquals(traceparent, "00-not-a-valid-traceparent")
            assertEquals(propagated.headers.toChain.iterator.count(_.key == "traceparent"), 1)
          }
        } yield ()
      }
  }

  test("injectHeaders treats a trailing null duplicate as missing and injects the current context") {
    KafkaTracerTestkit
      .create()
      .use { testkit =>
        val producerTracer =
          testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("producer-client"))

        for {
          tracedProducer <- producerTracer
          validPrepared <- testkit.appTracer.rootSpan("valid-earlier-context").surround {
            tracedProducer.injectHeaders(ProducerRecord("topic", "key", "value"))
          }
          validTraceparent = TextMapGetter[Headers]
            .get(validPrepared.headers, "traceparent")
            .getOrElse(fail("missing valid traceparent"))
          duplicated = ProducerRecord("topic", "key", "value").withHeaders(
            Headers.fromSeq(
              Seq(
                Header("traceparent", validTraceparent),
                Header("traceparent", null.asInstanceOf[Array[Byte]])
              )
            )
          )
          propagated <- testkit.appTracer.rootSpan("replacement-context").surround {
            tracedProducer.injectHeaders(duplicated)
          }
          _ <- IO {
            val traceparent = TextMapGetter[Headers]
              .get(propagated.headers, "traceparent")
              .getOrElse(fail("missing propagated traceparent"))

            assertNotEquals(traceparent, validTraceparent)
            assertEquals(propagated.headers.toChain.iterator.count(_.key == "traceparent"), 1)
          }
        } yield ()
      }
  }

}

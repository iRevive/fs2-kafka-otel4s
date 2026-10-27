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
import fs2.Chunk
import fs2.kafka.{ConsumerRecord, Header, Headers, ProducerRecord}
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.TextMapPropagator
import org.typelevel.otel4s.{Attribute, Attributes}
import org.typelevel.otel4s.oteljava.testkit.trace.{
  LinkExpectation,
  LinkSetExpectation,
  SpanContextExpectation,
  SpanExpectation,
  TraceExpectation,
  TraceForestExpectation
}
import org.typelevel.otel4s.semconv.experimental.attributes.MessagingExperimentalAttributes
import org.typelevel.otel4s.trace.{SpanContext, TracerProvider}

final class ProcessSpanRelationshipSuite extends KafkaTracingTestSupport {

  private val traceId = "12345678901234567890123456789012"
  private val spanId = "1234567890123456"
  private def incomingContext = SpanContextExpectation.any.traceIdHex(traceId)

  private def parentedProcess(topic: String) = SpanExpectation
    .consumer(s"process $topic")
    .spanContext(incomingContext)
    .parentSpanContext(incomingContext.spanIdHex(spanId).remote(true))

  private def messageLink(topic: String) = LinkExpectation.any
    .spanContext(incomingContext.spanIdHex(spanId).remote(true))
    .attributesSubset(
      MessagingExperimentalAttributes.MessagingDestinationName(topic),
      MessagingExperimentalAttributes.MessagingDestinationPartitionId("0"),
      MessagingExperimentalAttributes.MessagingKafkaMessageKey("key"),
      MessagingExperimentalAttributes.MessagingKafkaOffset(1L)
    )

  private val record = ConsumerRecord("orders", 0, 1L, "key", "value")
  private val config = KafkaTracer.Config.default
    .withProcessSpanSetup { ctx =>
      KafkaTracer.Config.Defaults
        .processSpanSetup(ctx)
        .map(_.withRelationship(ProcessSpanRelationship.ParentFromRecord))
    }
    .withBatchProcessSpanSetup { ctx =>
      KafkaTracer.Config.Defaults
        .batchProcessSpanSetup(ctx)
        .map(_.withRelationship(ProcessSpanRelationship.ParentFromRecord))
    }

  private val propagators: List[(String, TextMapPropagator, Headers)] = List(
    ("w3c", W3CTraceContextPropagator.getInstance(), Headers(Header("traceparent", s"00-$traceId-$spanId-01"))),
    ("custom", CustomTraceContextPropagator, Headers(Header("x-custom-trace", s"$traceId->$spanId")))
  )

  for {
    (propagatorName, propagator, headers) <- propagators
    operation <- List("record", "committable", "singleton-batch")
    ambient <- List(false, true)
  } test(s"$operation uses $propagatorName record parent and retains links, ambient=$ambient") {
    KafkaTracerTestkit.create(propagators = Seq(propagator)).use { testkit =>
      val incoming = record.withHeaders(headers)
      for {
        consumer <- testkit.tracedConsumer[String, String](StubKafkaConsumer.metadataOnly("client", "group"), config)
        spans = consumer.spans
        effect = operation match {
          case "record"      => spans.process(incoming)(testkit.appTracer.span("work").surround(IO.pure(42)))
          case "committable" =>
            spans.process(StubKafkaConsumer.committableRecord(incoming))(
              testkit.appTracer.span("work").surround(IO.pure(42))
            )
          case _ => spans.processBatch(Chunk.singleton(incoming))(testkit.appTracer.span("work").surround(IO.pure(42)))
        }
        resultAndAmbient <-
          if (ambient) testkit.appTracer.rootSpan("ambient").surround {
            for {
              span <- testkit.appTracer.currentSpanOrThrow
              result <- effect
            } yield (result, Some(span.context): Option[SpanContext])
          }
          else effect.map(result => (result, Option.empty[SpanContext]))
        (result, ambientContext) = resultAndAmbient
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(result, 42)
        val ambientLinks = ambientContext.toList.map { context =>
          LinkExpectation.any.spanContext(SpanContextExpectation.exact(context))
        }
        val process = TraceExpectation.ordered(
          parentedProcess("orders").links(LinkSetExpectation.exactly(messageLink("orders"), ambientLinks *)),
          TraceExpectation.leaf(SpanExpectation.internal("work").spanContext(incomingContext))
        )
        val expected = ambientContext.fold(TraceForestExpectation.unordered(process)) { context =>
          TraceForestExpectation.unordered(process, root(SpanExpectation.internal("ambient").spanContextExact(context)))
        }
        assertExpected(finished, expected)
      }
    }
  }

  for {
    headers <- List(Headers.empty, Headers(Header("traceparent", "invalid")))
    singletonBatch <- List(false, true)
  } test(s"missing or invalid context stays root under ambient span: $headers, batch=$singletonBatch") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val provider: TracerProvider[IO] = testkit.tracerProvider
      for {
        tracer <- KafkaTracer.create[IO](config)
        spans = tracer.consumerSpans(ConsumerMetadata.Provider.constant[IO](ConsumerMetadata(None, None)))
        incoming = record.withHeaders(headers)
        _ <- testkit.appTracer.rootSpan("ambient").surround {
          if (singletonBatch) spans.processBatch(Chunk.singleton(incoming))(IO.unit)
          else spans.process(incoming)(IO.unit)
        }
        finished <- testkit.finishedSpans
      } yield {
        assertExpected(
          finished,
          TraceForestExpectation.unordered(
            root(SpanExpectation.internal("ambient")),
            root(SpanExpectation.consumer("process orders").linkCount(0))
          )
        )
      }
    }
  }

  test("multiple-record batches and receives stay linked roots, including repeated message contexts") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val provider: TracerProvider[IO] = testkit.tracerProvider
      val incoming = record.withHeaders(propagators.head._3)
      val records = Chunk(incoming, incoming)
      for {
        tracer <- KafkaTracer.create[IO](config)
        spans = tracer.consumerSpans(ConsumerMetadata.Provider.constant[IO](ConsumerMetadata(None, None)))
        _ <- testkit.appTracer.rootSpan("ambient").surround {
          spans.receive(records)(spans.processBatch(records)(IO.unit))
        }
        finished <- testkit.finishedSpans
      } yield {
        val links = LinkSetExpectation.exactly(messageLink("orders"), messageLink("orders"))
        assertExpected(
          finished,
          TraceForestExpectation.unordered(
            root(SpanExpectation.internal("ambient")),
            root(SpanExpectation.consumer("process orders").links(links)),
            root(SpanExpectation.client("poll orders").links(links))
          )
        )
      }
    }
  }

  test("record context matching ambient context does not add a duplicate ambient link") {
    KafkaTracerTestkit.create().use { testkit =>
      for {
        producer <- testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("client"))
        consumer <- testkit.tracedConsumer[String, String](StubKafkaConsumer.metadataOnly("client", "group"), config)
        ambientContext <- testkit.appTracer.rootSpan("ambient").surround {
          producer.injectHeaders(ProducerRecord("orders", "key", "value")).flatMap { propagated =>
            consumer.spans.process(record.withHeaders(propagated.headers))(IO.unit)
          } >> testkit.appTracer.currentSpanOrThrow.map(_.context)
        }
        finished <- testkit.finishedSpans
      } yield {
        val extractedContext = SpanContextExpectation.any
          .traceIdHex(ambientContext.traceIdHex)
          .spanIdHex(ambientContext.spanIdHex)
          .remote(true)
        assertExpected(
          finished,
          TraceForestExpectation.unordered(
            root(
              SpanExpectation.internal("ambient").spanContextExact(ambientContext),
              TraceExpectation.leaf(
                SpanExpectation
                  .consumer("process orders")
                  .parentSpanContext(extractedContext)
                  .links(LinkSetExpectation.exactly(LinkExpectation.any.spanContext(extractedContext)))
              )
            )
          )
        )
      }
    }
  }

  test("explicit Link restores root processing and disabled spans preserve ambient context") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val provider: TracerProvider[IO] = testkit.tracerProvider
      val incoming = record.withHeaders(propagators.head._3)
      val metadata = ConsumerMetadata.Provider.constant[IO](ConsumerMetadata(None, None))
      for {
        linked <- KafkaTracer.create[IO](config.withProcessSpanSetup { ctx =>
          KafkaTracer.Config.Defaults.processSpanSetup(ctx).map(_.withRelationship(ProcessSpanRelationship.Link))
        })
        disabled <- KafkaTracer.create[IO](config.withoutProcessSpans)
        _ <- testkit.appTracer.rootSpan("ambient").surround {
          linked.consumerSpans(metadata).process(incoming)(IO.unit) >>
            disabled
              .consumerSpans(metadata)
              .process(incoming)(testkit.appTracer.span("single-work").surround(IO.unit)) >>
            disabled
              .consumerSpans(metadata)
              .processBatch(Chunk.singleton(incoming))(
                testkit.appTracer.span("batch-work").surround(IO.unit)
              )
        }
        finished <- testkit.finishedSpans
      } yield {
        assertExpected(
          finished,
          TraceForestExpectation.unordered(
            root(
              SpanExpectation.internal("ambient"),
              TraceExpectation.leaf(SpanExpectation.internal("single-work")),
              TraceExpectation.leaf(SpanExpectation.internal("batch-work"))
            ),
            root(
              SpanExpectation
                .consumer("process orders")
                .links(LinkSetExpectation.exactly(messageLink("orders")))
            )
          )
        )
      }
    }
  }

  test("relationship can vary by topic independently for record and batch setup") {
    KafkaTracerTestkit.create().use { testkit =>
      implicit val provider: TracerProvider[IO] = testkit.tracerProvider
      val perTopic = KafkaTracer.Config.default
        .withProcessSpanSetup { ctx =>
          KafkaTracer.Config.Defaults.processSpanSetup(ctx).map { setup =>
            if (ctx.topic == "orders") setup.withRelationship(ProcessSpanRelationship.ParentFromRecord)
            else setup
          }
        }
        .withBatchProcessSpanSetup { ctx =>
          KafkaTracer.Config.Defaults.batchProcessSpanSetup(ctx).map { setup =>
            if (ctx.topics == Set("payments")) setup.withRelationship(ProcessSpanRelationship.ParentFromRecord)
            else setup
          }
        }
      val orders = record.withHeaders(propagators.head._3)
      val payments = ConsumerRecord("payments", 0, 1L, "key", "value").withHeaders(propagators.head._3)
      for {
        tracer <- KafkaTracer.create[IO](perTopic)
        spans = tracer.consumerSpans(ConsumerMetadata.Provider.constant[IO](ConsumerMetadata(None, None)))
        _ <- spans.process(orders)(IO.unit)
        _ <- spans.process(payments)(IO.unit)
        _ <- spans.processBatch(Chunk.singleton(orders))(IO.unit)
        _ <- spans.processBatch(Chunk.singleton(payments))(IO.unit)
        finished <- testkit.finishedSpans
      } yield {
        assertExpected(
          finished,
          TraceForestExpectation.ordered(
            TraceExpectation.leaf(parentedProcess("orders").links(LinkSetExpectation.exactly(messageLink("orders")))),
            root(
              SpanExpectation.consumer("process payments").links(LinkSetExpectation.exactly(messageLink("payments")))
            ),
            root(SpanExpectation.consumer("process orders").links(LinkSetExpectation.exactly(messageLink("orders")))),
            TraceExpectation
              .leaf(parentedProcess("payments").links(LinkSetExpectation.exactly(messageLink("payments"))))
          )
        )
      }
    }
  }

  test("changing relationship preserves custom span setup") {
    val setup = KafkaTracer.Config.SpanSetup.Process(
      "custom process",
      Attributes(Attribute("app.name", "orders")),
      KafkaTracer.Config.Defaults.spanFinalizationStrategy
    )
    val parented = setup.withRelationship(ProcessSpanRelationship.ParentFromRecord)
    assertEquals(setup.relationship, ProcessSpanRelationship.Link)
    assertEquals(parented.relationship, ProcessSpanRelationship.ParentFromRecord)
    assertEquals(parented.spanName, setup.spanName)
    assertEquals(parented.attributes, setup.attributes)
    assertEquals(parented.finalizationStrategy, setup.finalizationStrategy)
  }

}

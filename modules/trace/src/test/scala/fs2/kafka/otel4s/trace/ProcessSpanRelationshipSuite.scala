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
import fs2.kafka.{ConsumerRecord, Header, Headers}
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.TextMapPropagator
import org.typelevel.otel4s.trace.TracerProvider

import scala.jdk.CollectionConverters._

final class ProcessSpanRelationshipSuite extends KafkaTracingTestSupport {

  private val traceId = "12345678901234567890123456789012"
  private val spanId = "1234567890123456"
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
        result <- if (ambient) testkit.appTracer.rootSpan("ambient").surround(effect) else effect
        finished <- testkit.finishedSpans
      } yield {
        assertEquals(result, 42)
        val process = finished.find(_.getName == "process orders").get
        assertEquals(process.getTraceId, traceId)
        assertEquals(process.getParentSpanId, spanId)
        assert(process.getParentSpanContext.isRemote)
        val links = process.getLinks.asScala.toList.map(l => (l.getSpanContext.getTraceId, l.getSpanContext.getSpanId))
        val ambientLinks = finished.filter(_.getName == "ambient").map(s => (s.getTraceId, s.getSpanId))
        assertEquals(links, (traceId, spanId) :: ambientLinks)
        val messageLink = process.getLinks.get(0)
        assert(!messageLink.getAttributes.isEmpty)
        val work = finished.find(_.getName == "work").get
        assertEquals(work.getTraceId, traceId)
        assertEquals(work.getParentSpanId, process.getSpanId)
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
        val process = finished.find(_.getName == "process orders").get
        assert(!process.getParentSpanContext.isValid)
        assert(process.getLinks.isEmpty)
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
        val operations = finished.filter(s => s.getName == "process orders" || s.getName == "poll orders")
        assertEquals(operations.size, 2)
        operations.foreach { span =>
          assert(!span.getParentSpanContext.isValid)
          assertEquals(span.getLinks.size(), 2)
          assert(span.getLinks.asScala.forall(_.getSpanContext.getSpanId == spanId))
        }
      }
    }
  }

  test("record context matching ambient context does not add a duplicate ambient link") {
    KafkaTracerTestkit.create().use { testkit =>
      for {
        producer <- testkit.tracedProducer[String, String](StubKafkaProducer.metadataOnly("client"))
        consumer <- testkit.tracedConsumer[String, String](StubKafkaConsumer.metadataOnly("client", "group"), config)
        _ <- testkit.appTracer.rootSpan("ambient").surround {
          producer.injectHeaders(fs2.kafka.ProducerRecord("orders", "key", "value")).flatMap { propagated =>
            consumer.spans.process(record.withHeaders(propagated.headers))(IO.unit)
          }
        }
        finished <- testkit.finishedSpans
      } yield {
        val ambient = finished.find(_.getName == "ambient").get
        val process = finished.find(_.getName == "process orders").get
        assertEquals(process.getParentSpanId, ambient.getSpanId)
        assertEquals(process.getLinks.size(), 1)
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
        val ambient = finished.find(_.getName == "ambient").get
        val processes = finished.filter(_.getName == "process orders")
        assertEquals(processes.size, 1)
        assert(!processes.head.getParentSpanContext.isValid)
        assertEquals(processes.head.getLinks.size(), 1)
        assertEquals(processes.head.getLinks.get(0).getSpanContext.getSpanId, spanId)
        finished.filter(_.getName.endsWith("-work")).foreach { span =>
          assertEquals(span.getParentSpanId, ambient.getSpanId)
        }
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
        assertEquals(
          finished.map(_.getName),
          List("process orders", "process payments", "process orders", "process payments")
        )
        assertEquals(finished.map(_.getParentSpanContext.isValid), List(true, false, false, true))
        assert(finished.forall(_.getLinks.size() == 1))
      }
    }
  }

  test("changing relationship preserves custom span setup") {
    val setup = KafkaTracer.Config.SpanSetup.Process(
      "custom process",
      org.typelevel.otel4s.Attributes(org.typelevel.otel4s.Attribute("app.name", "orders")),
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

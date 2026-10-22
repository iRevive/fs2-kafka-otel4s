# fs2-kafka-otel4s

## Setup

Add the trace module dependency:

```scala
libraryDependencies += "io.github.irevive" %% "fs2-kafka-otel4s-trace" % "0.2-3c52470-20260916T140549Z-SNAPSHOT"
```

Create normal `fs2-kafka` settings first. If you want stable broker endpoint attributes on spans, configure them explicitly through `KafkaTracer.Config`.

```scala
import cats.effect.IO
import fs2.kafka.{ConsumerSettings, Deserializer, ProducerSettings, Serializer}
import fs2.kafka.otel4s.trace.{BatchSpanMode, KafkaTracer}
import org.typelevel.otel4s.trace.TracerProvider

val producerSettings: ProducerSettings[IO, String, String] =
  ProducerSettings[IO, String, String](
    Serializer[IO, String],
    Serializer[IO, String]
  )
    .withBootstrapServers("localhost:9092")
    .withClientId("orders-producer")

val consumerSettings: ConsumerSettings[IO, String, String] =
  ConsumerSettings[IO, String, String](
    Deserializer[IO, String],
    Deserializer[IO, String]
  )
    .withBootstrapServers("localhost:9092")
    .withClientId("orders-consumer")
    .withGroupId("orders-group")

val tracerConfig: KafkaTracer.Config =
  KafkaTracer.Config.default
    .withServerAddress("kafka.internal", Some(9092))

val lowerVolumeTracerConfig: KafkaTracer.Config =
  KafkaTracer.Config.default
    .withBatchSpanMode(BatchSpanMode.SharedSendSpan)
```

Create `KafkaTracer` once from the `TracerProvider`, then bind traced handles from normal `fs2-kafka` resources:

```scala
import cats.effect.Resource
import fs2.kafka.{KafkaConsumer, KafkaProducer}
import fs2.kafka.otel4s.trace.{TracedKafkaConsumer, TracedKafkaProducer}

def createTracedProducer(
    implicit tracerProvider: TracerProvider[IO]
): Resource[IO, TracedKafkaProducer[IO, String, String]] =
  for {
    kafkaTracer <- KafkaTracer.resource[IO](tracerConfig)
    producer <- KafkaProducer.resource[IO, String, String](producerSettings)
  } yield kafkaTracer.producer(producer)

def createTracedConsumer(
    implicit tracerProvider: TracerProvider[IO]
): Resource[IO, TracedKafkaConsumer[IO, String, String]] =
  for {
    kafkaTracer <- KafkaTracer.resource[IO](tracerConfig)
    consumer <- KafkaConsumer.resource[IO, String, String](consumerSettings)
  } yield kafkaTracer.consumer(consumer)
```

## Producer Usage

Bind a `KafkaTracer` to a concrete `KafkaProducer.WithSettings`, then call the traced producer like the normal fs2-kafka producer.

If you want the most concise producer binding, import `fs2.kafka.otel4s.trace.syntax._`. That gives you:

- `.traced(...)` on `Stream[F, KafkaProducer.WithSettings[...]]`

The important part is that `produce` keeps the original fs2-kafka two-stage contract:

- the outer effect stages the send
- the inner effect waits for Kafka completion

In the common case, use `produce(...).flatten`. If the inner effect is never run, Kafka completion is not awaited and send spans do not finish.

```scala
import fs2.Chunk
import fs2.kafka.{ProducerRecord, ProducerRecords, ProducerResult}
import fs2.kafka.otel4s.trace.TracedKafkaProducer

def sendOne(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerResult[String, String]] =
  producer
    .produce(
      ProducerRecords.one(
        ProducerRecord("orders", "order-1", """{"status":"created"}""")
      )
    )
    .flatten

def sendBatch(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerResult[String, String]] =
  producer
    .produce(
      ProducerRecords(
        Chunk(
          ProducerRecord("orders", "order-1", """{"status":"created"}"""),
          ProducerRecord("orders", "order-2", """{"status":"created"}""")
        )
      )
    )
    .flatten
```

### Producer span model

Producer spans depend on the number of records, whether each record already has trace context in its Kafka headers, and the configured `BatchSpanMode`.

In the table below, **record trace context** means a valid span context encoded in the record's Kafka headers and recognized by the configured propagator. It may have been injected earlier by application code or received from another component. An ambient current span by itself is not record trace context until its context has been injected into the headers.

OpenTelemetry calls the trace context propagated with a message its _message creation context_. This documentation uses _record trace context_ to make clear that it is serialized trace information in the Kafka record headers, not an active context or necessarily a parent span.

| Records and trace headers | `PerRecordSpans` (default) | `SharedSendSpan` |
| --- | --- | --- |
| Empty | No spans | No spans |
| One, no trace context | One `PRODUCER` `send <topic>`; inject its context | Same |
| One, trace context present | One linked `CLIENT` `send <topic>`; preserve the context | Same |
| Batch, no record has trace context | One `PRODUCER` create span per record plus one `CLIENT` send with one link per record | One `PRODUCER` send; inject the same send context into every record; no send links |
| Batch, trace context on some records | One create span per record without trace context plus one `CLIENT` send with one link per record | One `PRODUCER` send; preserve and link existing record trace context, and inject the send context into other records |
| Batch, trace context on every record | One `CLIENT` send with one link per record and no create spans | Same |

Record trace context is recognized when the configured OpenTelemetry propagator can extract a usable span context from the Kafka headers. For example, when W3C Trace Context propagation is configured, this means a structurally valid `traceparent`. An unsampled context is still valid. Missing, malformed, or null authoritative headers are treated as no trace context. For duplicate propagation headers, the last matching header is authoritative. Other configured propagators, such as B3, may recognize different headers.

`PerRecordSpans` follows OpenTelemetry's higher-fidelity recommendation for batch-oriented APIs. Every record without trace context gets a distinct producer span and trace context, and the send span retains record-specific link attributes, at the cost of up to one extra span per record.

`SharedSendSpan` reduces span volume. Records without trace context share the send span's context and do not create producer-side links, so they also lose distinct producer spans and their per-record producer-side attributes. Existing record trace context remains authoritative and is still represented by links.

All producer spans use the ambient current span as their normal parent. A context extracted from record headers is represented by a link rather than used as the send span's parent.

See [Producer instrumentation](docs/producer-instrumentation.md) for the complete span matrix, attributes, lifecycle, error behavior, and transactional details.

The syntax import lets you bind tracing at the stream boundary and keep the rest of the producer API unchanged:

```scala
import fs2.Stream
import fs2.kafka.otel4s.trace.syntax._

def sendWithSyntax(
    implicit tracerProvider: TracerProvider[IO]
): IO[ProducerResult[String, String]] =
  Stream
    .resource(KafkaProducer.resource[IO, String, String](producerSettings))
    .traced(KafkaTracer.Config.default)
    .evalMap { producer =>
      producer.produce(
        ProducerRecords.one(
        ProducerRecord("orders", "order-1", """{"status":"created"}""")
        )
      ).flatten
    }
    .compile
    .onlyOrError
```

Transactional methods such as `produceTransactionally` remain available and traced:

```scala
def sendTransactionally(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerResult[String, String]] =
  producer.produceTransactionally(
    ProducerRecords.one(
      ProducerRecord("orders", "order-1", """{"status":"created"}""")
    )
  )
```

`produce` injects propagation headers automatically. In normal use, pass records directly to `produce`; do not call `injectHeaders` yourself.

Use `injectHeaders` only when a specific current span's trace context should deliberately be written to the record headers before later publication, such as when record construction and publication are decoupled. Explicit injection changes the telemetry when the record does not already have trace context.

If an application span is current, `injectHeaders(record)` followed by `produce(ProducerRecords.one(record))` preserves that application context and creates a linked `CLIENT` send span. Direct `produce(ProducerRecords.one(record))` instead creates a `PRODUCER` send span and injects the send span's own context. Consumers therefore correlate with the application span in the first flow and with the producer send span in the second.

If the record already has valid trace context, `injectHeaders` preserves it and both flows have the same topology. If no span context is current during explicit injection, no new usable trace context can be propagated and the subsequent `produce` follows the direct-production behavior.

For a batch, explicitly injecting the same current application context into every record suppresses the per-record create spans: the batch send span links once per record to that shared context. With the default `PerRecordSpans` mode, directly producing an uninstrumented batch creates a distinct create span and trace context for every record. With `SharedSendSpan`, direct production instead injects one shared send-span context.

```scala
def prepareRecord(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerRecord[String, String]] =
  producer.injectHeaders(
    ProducerRecord("orders", "order-1", """{"status":"created"}""")
  )
```

For domain key types, define `KafkaMessageKey` so `messaging.kafka.message.key` can be populated. Return `None` when the key should not be exposed. If you change the key type via serializers, use `tracedWithSerializers`; plain `withSerializers` still emits spans, but it drops key-attribute derivation for the new key type.

```scala
import fs2.kafka.otel4s.trace.KafkaMessageKey

final class OrderId(val value: String)

implicit val orderIdKafkaMessageKey: KafkaMessageKey[OrderId] =
  KafkaMessageKey.instance(id => Some(id.value))

def remapSerializers(
    producer: TracedKafkaProducer[IO, String, String]
): TracedKafkaProducer[IO, OrderId, String] =
  producer.tracedWithSerializers(
    Serializer[IO, String].contramap[OrderId](_.value),
    Serializer[IO, String]
  )
```

## Consumer Usage

Consumer tracing is explicit. The library does not try to transparently instrument every `KafkaConsumer` method.

`records`, `partitionedRecords`, `partitionedStream`, and `consumeChunk` remain available on `TracedKafkaConsumer`, but `consumeChunk` is a raw passthrough and does not add tracing. Spans are emitted only for explicit traced operations such as `consumeChunkTraced`, `recordsTraced`, `receive`, `process`, the commit spans owned by traced chunk helpers, and the syntax helpers built on top of them.

Import `fs2.kafka.otel4s.trace.syntax._` once and then choose the shape that matches your consumer:

- `.consumeChunk(...)` for raw, untraced chunk-oriented flows
- `.consumeChunkTraced(...)` for chunk-callback flows with `receive`, per-record `process`, and owned `commit` spans
- `.consumeRecordTraced(...)` for per-record callback flows with `receive`, per-record `process`, and owned `commit` spans
- `.recordsTraced(...)` for `.records.evalMap(...)`-style flows with `receive` and per-record `process` spans
- `receiveTraced` and `processTraced` when you need explicit boundaries inside chunked or partitioned streams

`.traced(...)` accepts either a bound `KafkaTracer` or a `KafkaTracer.Config`.

### Chunk-Oriented Syntax

`consumeChunkTraced` and `consumeRecordTraced` consume chunks from all assigned partitions concurrently. They create a
chunk-level `receive` span, add a `process` span for every record, and commit offsets after the callback has completed
successfully. The owned final offset commit is wrapped in a `commit` span with `messaging.operation.type=settle`.

The chunk callback overload wraps the entire callback in the `process` span for every record in its chunk. The
per-record callback overload evaluates each callback in its corresponding `process` span.

```scala
import fs2.kafka.KafkaConsumer
import fs2.kafka.otel4s.trace.syntax._

def consumeChunks(
    implicit kafkaTracer: KafkaTracer[IO]
): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(kafkaTracer)
    .consumeRecordTraced { record =>
      IO.println(s"Processed record: $record")
    }
```

```scala
import cats.syntax.all._
import fs2.kafka.KafkaConsumer
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.otel4s.trace.syntax._

def consumeChunkBatches(
    implicit kafkaTracer: KafkaTracer[IO]
): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(kafkaTracer)
    .consumeChunkTraced { chunk =>
      chunk.traverse_(record => IO.println(s"Received record: $record")).as(CommitNow)
    }
```

By default, both traced chunk helpers create all three span types. Use `KafkaTracer.Config.withoutReceiveSpans`,
`withoutProcessSpans`, or `withoutCommitSpans` to suppress individual span types without changing processing or commit
behavior. Use `consumeChunk` only when you intentionally want raw `fs2-kafka` behavior without tracing.

### Record-Oriented Syntax

For `.records.evalMap(...)`-style consumers, prefer `recordsTraced` together with `commitBatchWithin`. It creates
`receive` and per-record `process` spans but intentionally does not commit offsets or emit `commit` spans: the
downstream commit pipe owns commits.

```scala
import fs2.Stream
import fs2.kafka.commitBatchWithin
import fs2.kafka.otel4s.trace.syntax._

import scala.concurrent.duration._

def consumeRecords(
    implicit kafkaTracer: KafkaTracer[IO]
): Stream[IO, Unit] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(kafkaTracer)
    .recordsTraced { committable =>
      IO.println(s"Consumed record: $committable").as(committable.offset)
    }
    .through(commitBatchWithin[IO](500, 15.seconds))
```

### Local Syntax For Explicit Boundaries

When you are already working with chunked or partitioned consumer streams, local syntax makes the explicit tracing calls much lighter.

```scala
import fs2.kafka.otel4s.trace.TracedKafkaConsumer
import fs2.kafka.otel4s.trace.syntax._

def consumePartitioned(
    implicit kafkaTracer: KafkaTracer[IO]
): Stream[IO, Unit] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(kafkaTracer)
    .flatMap { tracedConsumer =>
      implicit val tc: TracedKafkaConsumer[IO, String, String] = tracedConsumer

      tracedConsumer.partitionedStream.flatMap(
        _.chunks.evalMap { chunk =>
          chunk.receiveTraced {
            chunk.traverse_(record =>
              record.processTraced {
                IO.println(s"Processed record: $record")
              }
            )
          }
        }
      )
    }
```

## Notes

- Reuse one `KafkaTracer` and one bound traced handle per producer or consumer resource.
- `commitBatchWithin` remains standard `fs2-kafka`; use it normally after `recordsTraced`. Generic downstream commit pipes are not traced because they only receive offsets, not the consumed records needed for Kafka span attributes.
- Duplicate propagation headers use last-match extraction, matching OpenTelemetry Java Kafka instrumentation rather than the generic first-value propagator rule.
- `injectHeaders` does not overwrite recognized record trace context. If a record already carries valid trace headers, those headers are preserved.
- The default `PerRecordSpans` mode may emit one producer `create` span per record plus a batch `send` span. Use `SharedSendSpan` when lower telemetry volume is more important than distinct per-record trace contexts and producer-side details.

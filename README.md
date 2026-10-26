# fs2-kafka-otel4s

OpenTelemetry tracing for fs2-kafka producers and consumers, powered by otel4s. Add `.traced(...)` to your existing
streams to create messaging spans and propagate trace context through Kafka headers.

- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Consumer syntax](#consumer-syntax)
- [Producer syntax](#producer-syntax)
- [Low-level operations](#low-level-operations)
- [Advanced usage](#advanced-usage)

## Quick start

Add the trace module dependency:

```scala
libraryDependencies += "io.github.irevive" %% "fs2-kafka-otel4s-trace" % "0.3-1962ce3-20261026T101356Z-SNAPSHOT"
```

Your application supplies an otel4s `TracerProvider[IO]` backed by its configured telemetry backend and exporter.
This library uses that provider; it does not configure an exporter. The examples below assume Kafka is available at
`localhost:9092` and the `orders` topic exists.

Start with per-record consumption. Import the tracing syntax, subscribe using normal fs2-kafka syntax, and add
`.traced(KafkaTracer.Config.default)` before the consume operation:

```scala
import cats.effect.IO
import fs2.kafka.{ConsumerSettings, Deserializer, KafkaConsumer}
import fs2.kafka.KafkaConsumer._
import fs2.kafka.otel4s.trace.KafkaTracer
import fs2.kafka.otel4s.trace.syntax._
import org.typelevel.otel4s.trace.TracerProvider

val consumerSettings: ConsumerSettings[IO, String, String] =
  ConsumerSettings[IO, String, String](
    Deserializer[IO, String],
    Deserializer[IO, String]
  )
    .withBootstrapServers("localhost:9092")
    .withClientId("orders-consumer")
    .withGroupId("orders-group")

def consumeOrders(implicit tracerProvider: TracerProvider[IO]): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(KafkaTracer.Config.default)
    .consumeRecordTraced(record => IO.println(s"Order: ${record.value}"))
```

This runs the handler once per record and commits each consumed chunk after all its handlers succeed. You get a
chunk-level `receive` span, a `process` span for each handler, and a `commit` span. A failed or canceled handler skips
the owned commit. The consumer keeps running until it fails or is canceled.

The same syntax import supports producers; see [Producer syntax](#producer-syntax) for a complete send example.

## Configuration

Start with `KafkaTracer.Config.default`. Pass your configuration directly to `.traced(config)`; the syntax creates a
tracer once per stream evaluation. If you already share a `KafkaTracer`, use `.traced(kafkaTracer)` instead.

```scala
import fs2.kafka.otel4s.trace.BatchSpanMode

val tracerConfig: KafkaTracer.Config =
  KafkaTracer.Config.default
    .withServerAddress("localhost", Some(9092))

val lowerVolumeTracerConfig: KafkaTracer.Config =
  tracerConfig.withBatchSpanMode(BatchSpanMode.SharedSendSpan)

val processingOnlyConfig: KafkaTracer.Config =
  tracerConfig.withoutReceiveSpans.withoutCommitSpans
```

| Option | Use it to |
| --- | --- |
| `withServerAddress(host, port)` | Add explicit broker endpoint attributes; bootstrap servers are configured separately in Kafka settings |
| `withBatchSpanMode(SharedSendSpan)` | Reduce producer batch span volume; the default is `PerRecordSpans` |
| `withoutReceiveSpans`, `withoutProcessSpans`, `withoutCommitSpans` | Suppress consumer span types while retaining processing and commits |
| `withoutSendSpans` | Suppress producer send spans while retaining propagation and any batch create spans |
| `withConstAttributes`, `addConstAttributes` | Add attributes to library spans |
| `with*SpanSetup` | Customize a span's name, attributes, and finalization, or return `None` to suppress it |

`withProcessSpanSetup` controls single-record processing; `withBatchProcessSpanSetup` controls chunk processing.
`withoutProcessSpans` suppresses both. Span suppression still runs your effects. `KafkaTracer.noop` is the explicit
complete opt-out: it runs the operations without spans or header propagation.

Client and consumer-group identity come from Kafka settings and metadata. Configure those on the Kafka client;
configure tracing policy on `KafkaTracer.Config`.

## Consumer syntax

Choose the operation that matches your handler:

| Your handler | Operation | Who commits? |
| --- | --- | --- |
| One record at a time | `consumeRecordTraced(record => effect)` | `consumeRecordTraced`, after every record callback in the chunk succeeds |
| One whole chunk at a time | `consumeChunkTraced(chunk => effect.as(CommitNow))` | `consumeChunkTraced`, after the chunk callback succeeds |
| A stream of callback results or offsets | `recordsTraced(record => effect)` | You, through a downstream commit pipe such as `consumer.spans.commitBatchWithin` |

Both consume helpers process assigned partitions concurrently and records within each partition sequentially.
Use the same `syntax._` import and `consumerSettings` from the quick start.

The diagrams show one chunk with all spans enabled and no ambient application span. Bars show lifetimes, not measured
durations. The library-created receive, process, and commit spans have no parent-child relationship with each other
in these examples. Receive and process are independent roots, even though their lifetimes overlap; nesting the work
in time does not make process a child of receive. Each root starts its own trace. Recognized incoming message contexts
connect to receive and process through links, as shown below. Commit uses an active application span as its parent
when one exists. The receive span's
default name is `poll <topic>`; the diagrams use the operation name `receive`.

### Per-record handler

`consumeOrders` in the quick start uses `consumeRecordTraced`. Choose this for ordinary per-message business logic.

`consumeRecordTraced`: Invokes the handler once per record and commits after all handlers succeed.

```text
Input chunk: [m1, m2, m3]

                         time -->
                    +--------+--------+--------+--------+
handler             |handle  |handle  |handle  |commit  |
                    |  m1    |  m2    |  m3    |offsets |
                    +--------+--------+--------+--------+

Span lifetime                                             Controlled by
--------------------------------------------------------  -------------------------
> receive           [=========================]           withReceiveSpanSetup
> process (m1)      [=======]                             withProcessSpanSetup
> process (m2)               [=======]                    withProcessSpanSetup
> process (m3)                        [=======]           withProcessSpanSetup
> commit                                       [=======]  withCommitSpanSetup
```

### Chunk handler

Choose `consumeChunkTraced` when your handler batches writes, deduplicates records, or processes an entire chunk at
once. It creates one batch process span, including for a singleton chunk. That span records the batch message count.

```scala
import cats.syntax.all._
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow

def consumeChunkBatches(implicit tracerProvider: TracerProvider[IO]): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(tracerConfig)
    .consumeChunkTraced { chunk =>
      chunk.traverse_(record => IO.println(s"Order: ${record.value}")).as(CommitNow)
    }
```

`consumeChunkTraced`: Invokes one handler for the entire chunk and commits after it succeeds.

```text
Input chunk: [m1, m2, m3]

                         time -->
                    +--------------------------+--------+
handler             | handleChunk([m1,m2,m3])  |commit  |
                    |       called once        |offsets |
                    +--------------------------+--------+

Span lifetime                                             Controlled by
--------------------------------------------------------  -------------------------
> receive           [=========================]           withReceiveSpanSetup
> process batch     [=========================]           withBatchProcessSpanSetup
> commit                                       [=======]  withCommitSpanSetup
```

#### Add a child span for each processed message

The batch process span is current while the chunk callback runs. Use an ordinary otel4s span inside that callback
when you also want a child span around each message's work:

```scala
import fs2.kafka.ConsumerRecord
import org.typelevel.otel4s.trace.Tracer

def consumeWithChildSpans(
    handle: ConsumerRecord[String, String] => IO[Unit]
)(implicit tracerProvider: TracerProvider[IO], tracer: Tracer[IO]): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(tracerConfig)
    .consumeChunkTraced { chunk =>
      chunk.traverse_ { record =>
        Tracer[IO].span("process").surround(handle(record))
      }.as(CommitNow)
    }
```

Here, each application-created `process` span is a child of the library's batch process span. The receive and commit
spans remain separate roots when there is no ambient application span:

```text
> receive
> process batch
  > process (m1)    child of process batch
  > process (m2)    child of process batch
  > process (m3)    child of process batch
> commit

Indentation means parent-child; aligned rows are independent roots.
```

Only messages passed to `handle` get these child spans. The batch span still describes the original chunk. If you
suppress the batch process span, it cannot parent these spans; they use whichever context is active in the callback.

### Stream results and batch commits

Choose `recordsTraced` when you want to return values or offsets to downstream stream stages. It traces delivery and
each record's handler; you choose when to commit. Use the bound consumer's commit pipe to include commit spans and
reuse its metadata:

```scala
import fs2.Stream
import scala.concurrent.duration._

def consumeRecords(implicit tracerProvider: TracerProvider[IO]): Stream[IO, Unit] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(tracerConfig)
    .flatMap { consumer =>
      consumer
        .recordsTraced { committable =>
          IO.println(s"Order: ${committable.record.value}").as(committable.offset)
        }
        .through(consumer.spans.commitBatchWithin(500, 15.seconds))
    }
```

`recordsTraced`: Finishes delivery tracing before invoking the per-record handlers. Committing belongs to the caller.

```text
Input chunk: [m1, m2, m3]

                         time -->
                    +--------+--------+--------+--------+
work                |deliver |handle  |handle  |handle  |
                    | chunk  |  m1    |  m2    |  m3    |
                    +--------+--------+--------+--------+

Span lifetime                                             Controlled by
--------------------------------------------------------  -------------------------
> receive           [=======]                             withReceiveSpanSetup
> process (m1)               [=======]                    withProcessSpanSetup
> process (m2)                        [=======]           withProcessSpanSetup
> process (m3)                                 [=======]  withProcessSpanSetup

Results are emitted after each handler; no automatic commit.
```

The traced pipe adds one commit span per committer call, using the context active when that call runs. Its batches
may differ from consumed chunks. The standard `fs2.kafka.commitBatchWithin` also works, but adds no commit spans.
Offset-only commits report topics and partitions; they cannot infer processed record counts or record details.

### Incoming trace headers: links, not parents

When a message carries a recognized trace context in its Kafka headers, the consumer records a **span link** to that
context. For example, a producer's send span may have injected its context into the message. The consumer's receive
and process spans remain roots; they do not become children of that producer span.

For a chunk containing one message `m1` with incoming context `S`, and no ambient application span:

```text
Consumer spans                         Context carried by m1

> receive         - - - link - - - ->  S (for example, a producer send span)
> process (m1)    - - - link - - - ->  S
> commit                               no message link

Each > row is a separate root. Dashed arrows are links, not parent-child edges.
```

With `consumeRecordTraced` or `recordsTraced`, each process span links to its own message's context. With
`consumeChunkTraced`, the one batch process span links to each recognized context in the chunk; receive does the same.
The application-created child spans above inherit the batch span as parent; the library attaches the message links
to the batch span, rather than to those children.

Without recognized trace headers, receive and process still create their root spans, with no incoming message links.
Links preserve correlation across traces without choosing one message as the parent of a batch. The links a tracing
backend displays depend on its support for span links.

## Producer syntax

Bind tracing with `.traced(config)` and use the normal producer API. Headers are injected automatically; ordinary
sends need no manual header preparation.

```scala
import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords, ProducerResult, ProducerSettings, Serializer}

val producerSettings: ProducerSettings[IO, String, String] =
  ProducerSettings[IO, String, String](
    Serializer[IO, String],
    Serializer[IO, String]
  )
    .withBootstrapServers("localhost:9092")
    .withClientId("orders-producer")

def sendOrder(implicit tracerProvider: TracerProvider[IO]): IO[ProducerResult[String, String]] =
  Stream
    .resource(KafkaProducer.resource[IO, String, String](producerSettings))
    .traced(tracerConfig)
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

Always evaluate both stages of `produce`: the outer effect submits records; the inner effect awaits Kafka's
acknowledgment and finalizes the send span. `.flatten` runs both.

For one record without existing trace headers:

```text
                         time -->
                    +----------------+--------------------+
work                | submit records | await Kafka ack    |
                    +----------------+--------------------+

Span lifetime
-----------------------------------------------------------
> send orders       [======================================]

The send span's context is injected into the record headers.
```

For batches without existing trace headers, the default `PerRecordSpans` mode gives each record a distinct context.
`SharedSendSpan` trades that detail for fewer spans:

```text
PerRecordSpans (default)            SharedSendSpan

> create orders (m1)               > send orders
> create orders (m2)                 context injected into m1, m2
> send orders
  links to both create contexts

Rows show relationships, not timing. Create and send spans use the ambient
application span as parent when present; links do not establish parentage.
```

Existing recognized record trace context is preserved and linked from the send span. A single record with existing
context produces a `CLIENT` send span; without it, a `PRODUCER` send span. See
[Producer instrumentation](docs/producer-instrumentation.md) for the full batch matrix, attributes, errors, and
transactional behavior.

Transactional APIs remain available on the traced producer:

```scala
import fs2.kafka.otel4s.trace.TracedKafkaProducer

def sendTransactionally(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerResult[String, String]] =
  producer.produceTransactionally(
    ProducerRecords.one(ProducerRecord("orders", "order-1", """{"status":"created"}"""))
  )
```

Use producer settings configured for transactions when calling transactional APIs.

## Low-level operations

Use these when your framework owns the Kafka loop or you need to choose exact tracing boundaries. Ordinary
applications can stay with the syntax above. Explicit handles apply the same configured tracing policy as the
high-level adapters; they require no tracing syntax import.

### Consumer operations without a client

Client-free callers can use the same batch process operation with an explicit metadata snapshot:

```scala
import fs2.Chunk
import fs2.kafka.otel4s.trace.ConsumerMetadata

def processBatch(
    records: Chunk[ConsumerRecord[String, String]],
    callback: IO[Unit]
)(implicit kafkaTracer: KafkaTracer[IO]): IO[Unit] =
  kafkaTracer
    .consumerSpans(ConsumerMetadata.Provider.constant[IO](ConsumerMetadata(None, None)))
    .processBatch(records)(callback)
```

Explicit `ConsumerSpans` operations create only their own span. The caller chooses execution boundaries and commits:

```text
Operation                                   Span created       Controlled by
------------------------------------------  -----------------  -------------------------
spans.process(record)(effect)               > process          withProcessSpanSetup
spans.process(committableRecord)(effect)    > process          withProcessSpanSetup
spans.processBatch(records)(effect)         > process batch    withBatchProcessSpanSetup
spans.receive(records)(effect)              > receive          withReceiveSpanSetup
spans.receiveCommittable(records)(effect)    > receive          withReceiveSpanSetup
```

Each span surrounds the supplied effect; none of these operations commits offsets. Empty batch operations run the
supplied effect without a span.

### Bound consumer operations

When you are already working with chunked or partitioned consumer streams, call the configured operations on
`tracedConsumer.spans`:

```scala
import fs2.kafka.otel4s.trace.TracedKafkaConsumer

def processWithSelectedConsumer(
    records: Chunk[ConsumerRecord[String, String]],
    tracedConsumer: TracedKafkaConsumer[IO, String, String]
): IO[Unit] =
  tracedConsumer.spans.processBatch(records)(IO.println(records))
```

For per-record boundaries, use `tracedConsumer.spans.process(record)(effect)`; choose the effect passed to
`tracedConsumer.spans.receiveCommittable(chunk)(effect)` to define exactly how much work runs inside the receive span.
The batch operation is also available without a client-bound adapter through
`KafkaTracer.consumerSpans(metadataProvider)` as shown above.

### Offset-only commits

If your app or library commits offsets directly, wrap the commit effect with `commitOffsets` to trace it:

```scala
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

def commitOffsets(
    offsets: Map[TopicPartition, OffsetAndMetadata],
    underlyingCommit: IO[Unit]
)(implicit kafkaTracer: KafkaTracer[IO]): IO[Unit] =
  kafkaTracer
    .consumerSpans(ConsumerMetadata.Provider.constant[IO](ConsumerMetadata(None, None)))
    .commitOffsets(offsets)(underlyingCommit)
```

`commitOffsets` runs `underlyingCommit` inside a commit span. You supply the actual commit effect. An empty map runs
the effect without a span.

The span reports the topics being committed. It reports a partition only when the map has one topic-partition entry.
Commit positions do not tell us how many records were processed, or their keys, offsets, or tombstone values, so those
attributes are omitted. The span has no message links. Commit spans use the context active when each commit call runs;
they do not recover the context of earlier per-record `process` spans.

If a span is active, it becomes the commit span's parent. Otherwise the commit span is a root.
The custom `kafkaTracer.consumerSpans(metadata).commitBatchWithin` pipe traces each committer call. The upstream
`fs2.kafka.commitBatchWithin` does not automatically create commit spans.

### Producer operations without a client

For frameworks that own their producer, use `ProducerSpans` with explicit metadata and a send callback:

```scala
import fs2.kafka.otel4s.trace.ProducerMetadata

def sendWithTracing(
    kafkaTracer: KafkaTracer[IO],
    records: ProducerRecords[String, String],
    send: ProducerRecords[String, String] => IO[IO[ProducerResult[String, String]]]
): IO[ProducerResult[String, String]] = {
  val spans = kafkaTracer.producerSpans(
    ProducerMetadata.Provider.constant[IO](ProducerMetadata(Some("orders-producer")))
  )
  spans.produce(records)(send).flatten
}
```

The callback must publish the prepared records it receives, including their injected headers. The outer effect submits
records; the returned acknowledgment effect waits for completion and finalizes the send span. Always evaluate both
stages. Abandoning the acknowledgment effect leaves the span open. A single handle accepts different key and value
types at each call. Configuration applies equally to this API and the producer adapter.

`tracedProducer.spans` exposes operations using the bound producer's metadata provider. Use
`ProducerMetadata.Provider.fromProducer` to derive metadata from another live producer, or `constant` for a snapshot
that retains no client. Generated client IDs are resolved lazily from metrics and cached, including a missing value
after lookup failure. Transactions remain the responsibility of the adapter or the calling framework.

### Resource binding and tracer reuse

If you allocate clients through `Resource`, create a `KafkaTracer` once and bind your handles explicitly. The same
tracer can also be passed to `.traced(kafkaTracer)` in stream syntax.

```scala
import cats.effect.Resource

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

Reuse bound handles for the lifetime of their clients. A metadata provider derived with
`ConsumerMetadata.Provider.fromConsumer` or `ProducerMetadata.Provider.fromProducer` retains its client and lazily
caches its identity, including missing identity after a failed metrics lookup. A `Provider.constant` holds an explicit
snapshot without retaining a client. Use metadata from the consumer that produced the offsets when tracing commits;
use `ConsumerMetadata(None, None)` if identity is unavailable.

The traced batching pipe needs `Temporal` for timed flushes and `Parallel` for distinct committers. These requirements
apply to the batching method rather than every operation handle.

## Advanced usage

### Trace only selected work in a chunk

Selecting records inside a chunk callback still commits the original chunk's offsets after success. Explicit
`consumer.spans.process(record)(effect)` calls create root process spans, unlike the ordinary otel4s child spans
shown in the chunk-handler example. To emit only those selected root spans, suppress the surrounding batch span
with `withBatchProcessSpanSetup(_ => None)`.

### Manual header injection

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

### Custom message keys

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

### Raw APIs and migration

Use `tracedConsumer.underlying` for lifecycle, assignment, and raw stream operations. Raw `records`,
`partitionedRecords`, `partitionedStream`, and `consumeChunk` do not add tracing. Older forwarding methods and
record/chunk syntax are deprecated; use explicit handles instead:

| Older call | Recommended call |
| --- | --- |
| `record.processTraced(effect)` | `consumer.spans.process(record)(effect)` |
| `chunk.receiveTraced(effect)` | `consumer.spans.receive(chunk)(effect)` |
| `consumer.process(record)(effect)` | `consumer.spans.process(record)(effect)` |
| `consumer.consumeChunk(callback)` | `consumer.underlying.consumeChunk(callback)` for raw consumption |

Duplicate propagation headers use last-match extraction. A recognized context can be unsampled; malformed or null
authoritative headers are treated as missing. See [Producer instrumentation](docs/producer-instrumentation.md) for
header preservation and injection details.

# Consumers

Add `.traced(config)` to trace record processing and offset commits.
Use the [interactive guide](../interactive-guides/consumer-tracing.md) to inspect spans and try configuration options.

## Choose a consumer API

Choose the operation that matches your handler:

| Your handler | Operation | Who commits? |
| --- | --- | --- |
| One record at a time | `consumeRecordTraced` | `consumeRecordTraced`, after every record callback in the chunk succeeds |
| One whole chunk at a time | `consumeChunkTraced` | `consumeChunkTraced`, after the chunk callback succeeds |
| A stream of callback results or offsets | `recordsTraced` | You, through a downstream commit pipe such as `consumer.spans.commitBatchWithin` |

Both consume helpers process partitions concurrently. Records within each partition run in order.

## Set up the consumer

The examples use the same settings as the [quick start](../index.md#quick-start). Supply your configured
`TracerProvider[IO]`; Kafka must be available at `localhost:9092` with an `orders` topic.

```scala mdoc:silent
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

val tracerConfig: KafkaTracer.Config =
  KafkaTracer.Config.default
```

## Understand the spans

The diagrams show one chunk with all spans enabled and no active application span.

- Receive, process, and commit are separate root spans. Each starts a trace.
- Incoming message contexts are attached as span links.
- Commit spans are always roots, independent of any active application span.
- Bars show span lifetimes. The receive span is named `poll <topic>`.

### Per-record handler

Use `consumeRecordTraced` to handle each record separately. It commits the chunk after all handlers succeed.
See the [quick start](../index.md#quick-start) for code.

```text
Input chunk: [m1, m2, m3]

                         time -->
                    +--------+--------+--------+--------+
handler             |handle  |handle  |handle  |commit  |
                    |  m1    |  m2    |  m3    |offsets |
                    +--------+--------+--------+--------+

Span lifetime                                             Controlled by
--------------------------------------------------------  -------------------------
  receive           [=========================]           withReceiveSpanSetup
  process (m1)      [=======]                             withProcessSpanSetup
  process (m2)               [=======]                    withProcessSpanSetup
  process (m3)                        [=======]           withProcessSpanSetup
  commit                                       [=======]  withCommitSpanSetup
```

### Chunk handler

Use `consumeChunkTraced` to handle a whole chunk, for example to batch writes or deduplicate records.
It creates one process span with the message count, even for a chunk of one record.

```scala mdoc:silent
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

```text
Input chunk: [m1, m2, m3]

                         time -->
                    +--------------------------+--------+
handler             | handleChunk([m1,m2,m3])  |commit  |
                    |       called once        |offsets |
                    +--------------------------+--------+

Span lifetime                                             Controlled by
--------------------------------------------------------  -------------------------
  receive           [=========================]           withReceiveSpanSetup
  process batch     [=========================]           withBatchProcessSpanSetup
  commit                                       [=======]  withCommitSpanSetup
```

#### Add a child span for each processed message

Use an otel4s span inside the chunk callback to add a child span for each message:

```scala mdoc:silent
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
        Tracer[IO].span("handle order").surround(handle(record))
      }.as(CommitNow)
    }
```

Each application `handle order` span is a child of the batch process span:

```text
  receive
  process batch
    handle order (m1)    child of process batch
    handle order (m2)    child of process batch
    handle order (m3)    child of process batch
  commit

Indentation means parent-child; aligned rows are independent roots.
```

Only messages passed to `handle` get child spans. The batch span covers the original chunk.
If you disable the batch span, child spans use the context active in the callback.

### Stream results and batch commits

Use `recordsTraced` to pass results or offsets downstream. You choose when to commit.
Use `consumer.spans.commitBatchWithin` to trace commits:

```scala mdoc:silent
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

The receive span ends before record handlers run. Each handler gets a process span.

```text
Input chunk: [m1, m2, m3]

                         time -->
                    +--------+--------+--------+--------+
work                |deliver |handle  |handle  |handle  |
                    | chunk  |  m1    |  m2    |  m3    |
                    +--------+--------+--------+--------+

Span lifetime                                             Controlled by
--------------------------------------------------------  -------------------------
  receive           [=======]                             withReceiveSpanSetup
  process (m1)               [=======]                    withProcessSpanSetup
  process (m2)                        [=======]           withProcessSpanSetup
  process (m3)                                 [=======]  withProcessSpanSetup

Results are emitted after each handler; no automatic commit.
```

The pipe creates one span per commit call. Commit batches may differ from consumed chunks.
The standard `fs2.kafka.commitBatchWithin` pipe also works but adds no commit spans.

Offset-only commits report topics and partitions, without record counts or details.

### Incoming trace headers

By default, receive and process spans link to trace contexts found in message headers. They remain separate roots.
For one message `m1` carrying context `S`:

```text
Consumer spans                         Context carried by m1

  receive         - - - link - - - ->  S (for example, a producer send span)
  process (m1)    - - - link - - - ->  S
  commit                               no message link

Each span shown here is a separate root. Dashed arrows are links, not parent-child edges.
```

- Per-record process spans link to their own message context.
- Receive and batch process spans link to all recognized contexts in the chunk.
- Application child spans inherit their parent. They get no automatic message links.

Without recognized headers, receive and process spans still start new traces.
Your tracing backend must support span links to display these connections.

To continue the producer trace during processing, return a process setup with
`.withRelationship(ProcessSpanRelationship.ParentFromRecord)` from `withProcessSpanSetup` or
`withBatchProcessSpanSetup`. Single-record processing and singleton
process batches then use the message context as parent and retain the message link. A distinct ambient context is
also linked. Missing or invalid headers still produce a root; batches with multiple records remain linked roots.
Receive and commit behavior is unchanged. See [Configuration](configuration.md#process-span-relationships).

## Trace only selected work in a chunk

Selecting records in a chunk callback still commits the original chunk's offsets after success.

To trace selected records with the configured process relationship, call `consumer.spans.process(record)(effect)`.
Disable the batch process span with `withBatchProcessSpanSetup(_ => None)` if you only want those record spans.

See [Configuration](configuration.md) for span setup options and the [Low-level API](../reference/low-level-api.md) for explicit operation handles.

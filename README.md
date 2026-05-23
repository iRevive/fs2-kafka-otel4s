# fs2-kafka-otel4s

## Setup

Add the trace module dependency:

```scala
libraryDependencies += "io.github.irevive" %% "fs2-kafka-otel4s-trace" % "0.1-d44d3ca-20260523T094655Z-SNAPSHOT"
```

Create normal `fs2-kafka` settings first, then create a library-managed `KafkaTracer`.

```scala
import cats.effect.{IO, Resource}
import cats.syntax.all._
import fs2.Stream
import fs2.kafka.KafkaConsumer._
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.otel4s.trace.{KafkaMessageKey, KafkaTracer, TracedKafkaConsumer, TracedKafkaProducer}
import fs2.kafka._
import org.typelevel.otel4s.trace.TracerProvider

import scala.concurrent.duration._

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
```

Create bound traced handles from normal `fs2-kafka` resources:

```scala
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

Producer tracing is direct: bind a `KafkaTracer` to a concrete `KafkaProducer.WithSettings`, then call the traced producer like the normal producer.

The main caveat is unchanged from `fs2-kafka`: `produce` is two-stage.

- the outer effect stages the send
- the inner effect waits for Kafka completion

In the common case, use `produce(...).flatten`.

```scala
import fs2.Chunk

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

You can also inject trace headers without sending yet:

```scala
import fs2.kafka.ProducerRecord

def prepareRecord(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerRecord[String, String]] =
  producer.injectHeaders(
    ProducerRecord("orders", "order-1", """{"status":"created"}""")
  )
```

If the key type changes, prefer `tracedWithSerializers` over `withSerializers`.

```scala
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

The two main paths are:

- chunk-oriented tracing with `consumeChunk`
- record-oriented tracing with `recordsWithProcess`

`records`, `partitionedRecords`, and `partitionedStream` remain available on `TracedKafkaConsumer`, but they are passthrough accessors. Spans are emitted only when you call explicit traced operations such as `consumeChunk`, `receive`, `process`, or the syntax helpers built on top of them.

## Syntax First

The recommended ergonomic path is to import `fs2.kafka.otel4s.trace.syntax._` once in consumer code.

Then:

1. bind tracing on `Stream[F, KafkaConsumer[F, K, V]]` with either `.traced(kafkaTracer)` or `.traced(config)`
2. use traced stream helpers like `.consumeChunk(...)` or `.recordsWithProcess(...)`
3. use local helpers like `chunk.traceReceive(...)` and `record.traceProcess(...)` when you need more control

### Chunk-Oriented Syntax

This is the closest shape to existing `consumeChunk` usage.

```scala
import fs2.kafka.otel4s.trace.syntax._

def consumeChunks(
    implicit kafkaTracer: KafkaTracer[IO]
): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(kafkaTracer)
    .consumeChunk { chunk =>
      chunk.traverse_(record => IO.println(s"Consumed chunk record: $record")).as(CommitNow)
    }
```

This syntax is shorthand for:

```scala
KafkaConsumer
  .stream[IO, String, String](consumerSettings)
  .subscribeTo("orders")
  .map(kafkaTracer.consumer(_))
  .evalMap(_.consumeChunk { chunk =>
    chunk.traverse_(record => IO.println(s"Consumed chunk record: $record")).as(CommitNow)
  })
  .compile
  .onlyOrError
```

To construct the implicit tracer once and then reuse it:

```scala
def consumeChunksProgram(
    implicit tracerProvider: TracerProvider[IO]
): Resource[IO, IO[Nothing]] =
  KafkaTracer.resource[IO](tracerConfig).map { implicit kafkaTracer =>
    consumeChunks
  }
```

`consumeChunk` traces chunk delivery. If you want explicit per-record `process` spans, use `recordsWithProcess` or local `traceProcess` helpers.

If you want to keep the dependency explicit at the call site, the same syntax also accepts a bound `KafkaTracer`:

```scala
import fs2.kafka.otel4s.trace.syntax._

def consumeChunksExplicit(
    kafkaTracer: KafkaTracer[IO]
): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(kafkaTracer)
    .consumeChunk { chunk =>
      chunk.traverse_(record => IO.println(s"Consumed chunk record: $record")).as(CommitNow)
    }
```

If you do not want to construct `KafkaTracer` yourself first, the same stream syntax also accepts `KafkaTracer.Config`:

```scala
import fs2.kafka.otel4s.trace.syntax._

def consumeChunksWithConfig(
    implicit tracerProvider: TracerProvider[IO]
): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(tracerConfig)
    .consumeChunk { chunk =>
      chunk.traverse_(record => IO.println(s"Consumed chunk record: $record")).as(CommitNow)
    }
```

### Record-Oriented Syntax

For `.records.evalMap(...)`-style consumers, prefer `recordsWithProcess`.

```scala
import fs2.kafka.otel4s.trace.syntax._

def consumeRecords(
    implicit kafkaTracer: KafkaTracer[IO]
): Stream[IO, Unit] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(kafkaTracer)
    .recordsWithProcess { committable =>
      IO.println(s"Consumed record: $committable").as(committable.offset)
    }
    .through(commitBatchWithin[IO](500, 15.seconds))
```

This syntax is shorthand for:

```scala
KafkaConsumer
  .stream[IO, String, String](consumerSettings)
  .subscribeTo("orders")
  .map(kafkaTracer.consumer(_))
  .flatMap(_.recordsWithProcess { committable =>
    IO.println(s"Consumed record: $committable").as(committable.offset)
  })
  .through(commitBatchWithin(500, 15.seconds))
```

### Local Syntax For Explicit Boundaries

When you are already working with chunked or partitioned consumer streams, local syntax makes the explicit tracing calls much lighter.

```scala
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
          chunk.traceReceive {
            chunk.traverse_(record =>
              record.traceProcess {
                IO.println(s"Processed record: $record")
              }
            )
          }
        }
      )
    }
```

This syntax is shorthand for:

```scala
tracedConsumer.partitionedStream.flatMap(
  _.chunks.evalMap { chunk =>
    tracedConsumer.receiveCommittable(chunk) {
      chunk.traverse_(record =>
        tracedConsumer.process(record) {
          IO.println(s"Processed record: $record")
        }
      )
    }
  }
)
```

## Guidance

### Prefer syntax imports for consumer code

The syntax layer is the easiest way to keep consumer tracing readable:

- `.traced(kafkaTracer)` makes the bind step obvious
- keeping `KafkaTracer[F]` implicit in your surrounding code still works well with `.traced(kafkaTracer)`
- `.traced(config)` is the shortest path when you only need a default library-managed tracer
- `.consumeChunk(...)` and `.recordsWithProcess(...)` keep common flows compact
- `traceReceive` and `traceProcess` keep explicit boundaries visible without repeating `tracedConsumer`

### Reuse one `KafkaTracer` and one bound traced handle per client

Create `KafkaTracer` once from the `TracerProvider`, then bind it once per producer or consumer resource.

### Keep consumer processing boundaries explicit

Plain stream emission is not automatically treated as message processing. If the business step matters, wrap it with:

- `recordsWithProcess`
- `process(record)(fa)`
- `record.traceProcess(fa)`

### Configure stable endpoint attributes explicitly

`server.address` and `server.port` are not inferred from Kafka client internals. If you want them on spans, configure them through `KafkaTracer.Config`.

## Caveats

### `produce` is intentionally two-stage

If you only evaluate the outer effect, Kafka completion is not awaited and send-span finalization is delayed.

### Consumer tracing is explicit, not transparent

This module does not try to make plain `.records`, `.partitionedStream`, or arbitrary downstream `Pipe`s automatically traced.

### Generic `commitBatchWithin` remains just `fs2-kafka`

You can use it normally after `recordsWithProcess`, but commit batching itself is not turned into a dedicated traced commit API by this module.

### Duplicate propagation headers use last-match extraction

When multiple Kafka headers share the same propagation key, extraction prefers the last matching value. This matches OpenTelemetry Java Kafka instrumentation rather than the generic first-value propagator rule.

### Existing propagated context is preserved

`injectHeaders` does not overwrite a recognized existing propagation context. If a record already carries trace headers, those headers continue to define the message creation context.

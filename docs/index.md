# fs2-kafka-otel4s

## Setup

Add the trace module dependency:

```scala
libraryDependencies += "io.github.irevive" %% "fs2-kafka-otel4s-trace" % "@VERSION@"
```

Create normal `fs2-kafka` settings first. If you want stable broker endpoint attributes on spans, configure them explicitly through `KafkaTracer.Config`.

```scala mdoc:silent
import cats.effect.IO
import fs2.kafka.{ConsumerSettings, Deserializer, ProducerSettings, Serializer}
import fs2.kafka.otel4s.trace.KafkaTracer
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
```

Create `KafkaTracer` once from the `TracerProvider`, then bind traced handles from normal `fs2-kafka` resources:

```scala mdoc:silent
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

`produce` keeps the original fs2-kafka two-stage contract:

- the outer effect stages the send
- the inner effect waits for Kafka completion

In the common case, use `produce(...).flatten`. If the inner effect is never run, Kafka completion is not awaited and send spans do not finish.

```scala mdoc:silent
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

Transactional methods such as `produceTransactionally` remain available and traced:

```scala mdoc:silent
def sendTransactionally(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerResult[String, String]] =
  producer.produceTransactionally(
    ProducerRecords.one(
      ProducerRecord("orders", "order-1", """{"status":"created"}""")
    )
  )
```

`produce` injects propagation headers automatically. Use `injectHeaders` only when record construction and publication are decoupled and you need to preserve the current tracing context across that gap.

```scala mdoc:silent
def prepareRecord(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerRecord[String, String]] =
  producer.injectHeaders(
    ProducerRecord("orders", "order-1", """{"status":"created"}""")
  )
```

For domain key types, define `KafkaMessageKey` so `messaging.kafka.message.key` can be populated. Return `None` when the key should not be exposed. If you change the key type via serializers, use `tracedWithSerializers`; plain `withSerializers` still emits spans, but it drops key-attribute derivation for the new key type.

```scala mdoc:silent
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

`records`, `partitionedRecords`, and `partitionedStream` remain available on `TracedKafkaConsumer`, but spans are emitted only for explicit traced operations such as `consumeChunk`, `receive`, `process`, and the syntax helpers built on top of them.

Import `fs2.kafka.otel4s.trace.syntax._` once and then choose the shape that matches your consumer:

- `.consumeChunkTraced(...)` for chunk-oriented flows
- `.recordsWithProcessTraced(...)` for `.records.evalMap(...)`-style flows
- `receiveTraced` and `processTraced` when you need explicit boundaries inside chunked or partitioned streams

`.traced(...)` accepts either a bound `KafkaTracer` or a `KafkaTracer.Config`.

### Chunk-Oriented Syntax

This is the closest shape to existing `consumeChunk` usage.

```scala mdoc:silent
import cats.syntax.all._
import fs2.kafka.KafkaConsumer
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.otel4s.trace.syntax._

def consumeChunks(
    implicit kafkaTracer: KafkaTracer[IO]
): IO[Nothing] =
  KafkaConsumer
    .stream[IO, String, String](consumerSettings)
    .subscribeTo("orders")
    .traced(kafkaTracer)
    .consumeChunkTraced { chunk =>
      chunk.traverse_(record => IO.println(s"Consumed chunk record: $record")).as(CommitNow)
    }
```

`consumeChunkTraced` traces chunk delivery. If the business step needs per-record `process` spans, use `recordsWithProcessTraced` or local `processTraced` helpers.

### Record-Oriented Syntax

For `.records.evalMap(...)`-style consumers, prefer `recordsWithProcessTraced`.

```scala mdoc:silent
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
    .recordsWithProcessTraced { committable =>
      IO.println(s"Consumed record: $committable").as(committable.offset)
    }
    .through(commitBatchWithin[IO](500, 15.seconds))
```

### Local Syntax For Explicit Boundaries

When you are already working with chunked or partitioned consumer streams, local syntax makes the explicit tracing calls much lighter.

```scala mdoc:silent
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
- `commitBatchWithin` remains standard `fs2-kafka`; use it normally after `recordsWithProcessTraced`.
- Duplicate propagation headers use last-match extraction, matching OpenTelemetry Java Kafka instrumentation rather than the generic first-value propagator rule.
- `injectHeaders` does not overwrite a recognized existing propagation context. If a record already carries trace headers, those headers continue to define the message creation context.
- Real batch sends may emit per-record producer `create` spans plus a batch `send` span, with record-specific details attached as links rather than collapsed onto the batch span.

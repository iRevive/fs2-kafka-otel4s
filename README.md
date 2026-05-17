# fs2-kafka-otel4s

## Setup

Add the trace module dependency:

```scala
libraryDependencies += "io.github.irevive" %% "fs2-kafka-otel4s-trace" % "0.1-a460256-20260517T073136Z-SNAPSHOT"
```

Create normal `fs2-kafka` producer settings first:

```scala
import cats.effect.{IO, Resource}
import fs2.kafka.{KafkaProducer, ProducerSettings, Serializer}
import fs2.kafka.otel4s.trace.{KafkaTracer, TracedKafkaProducer}
import org.typelevel.otel4s.trace.TracerProvider

val producerSettings: ProducerSettings[IO, String, String] =
  ProducerSettings[IO, String, String](
    Serializer[IO, String],
    Serializer[IO, String]
  )
    .withBootstrapServers("localhost:9092")
    .withClientId("orders-producer")

def createTracedProducer(
    implicit tracerProvider: TracerProvider[IO]
): Resource[IO, TracedKafkaProducer[IO, String, String]] =
  for {
    tracer <- Resource.eval(KafkaTracer.create[IO](KafkaTracer.Config.default))
    // create normal producer
    producer <- KafkaProducer.resource[IO, String, String](producerSettings)
    // create traced producer
  } yield tracer.producer(producer)
```

If you want broker-level endpoint attributes on emitted spans, configure them explicitly:

```scala
import fs2.kafka.otel4s.trace.KafkaTracer

val tracerConfig: KafkaTracer.Config =
  KafkaTracer.Config.default
    .withServerAddress("kafka.internal", Some(9092))
```

## How To Use It

Bind a `KafkaTracer` to a concrete `KafkaProducer.WithSettings`, then call the traced producer exactly like the normal
fs2-kafka producer.

The important part is that `produce` keeps the original fs2-kafka two-stage contract:

- the outer effect stages the send
- the inner effect waits for Kafka completion

So in the common case you want `produce(...).flatten`.

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

Transactional APIs remain available and traced:

```scala
import fs2.kafka.{ProducerRecords, ProducerResult}

def sendTransactionally(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerResult[String, String]] =
  producer.produceTransactionally(
    ProducerRecords.one(
      ProducerRecord("orders", "order-1", """{"status":"created"}""")
    )
  )
```

## General Patterns

### Prefer `produce(...).flatten` at the call site

That is the simplest way to keep span lifetime aligned with Kafka acknowledgement.

### Reuse one traced producer per bound Kafka producer

Create the `TracedKafkaProducer` once from the real producer resource and pass it through your application, instead of
rebuilding it for every send.

### Define `KafkaMessageKey` for domain keys

If your producer key type is not already covered, define a canonical string representation for semantic attributes:

```scala
import fs2.kafka.otel4s.trace.KafkaMessageKey

final class OrderId(val value: String)

implicit val orderIdKafkaMessageKey: KafkaMessageKey[OrderId] =
  KafkaMessageKey.instance(id => Some(id.value))
```

Return `None` when the key should not be exposed as telemetry.

### Prefer `tracedWithSerializers` over `withSerializers`

If you need to change serializers on a traced producer and the key type changes, use `tracedWithSerializers`.

```scala
import fs2.kafka.otel4s.trace.{KafkaMessageKey, TracedKafkaProducer}

final class RemappedKey(val value: String)

implicit val remappedKeyMessageKey: KafkaMessageKey[RemappedKey] =
  KafkaMessageKey.instance(key => Some(s"remapped:${key.value}"))

def remapSerializers(
    producer: TracedKafkaProducer[IO, String, String]
): TracedKafkaProducer[IO, RemappedKey, String] =
  producer.tracedWithSerializers(
    Serializer[IO, String].contramap[RemappedKey](_.value),
    Serializer[IO, String]
  )
```

That keeps tracing semantics for the new key type, including `messaging.kafka.message.key`.

### Use `injectHeaders` when send and publish are decoupled

If you send records through a `TracedKafkaProducer`, trace headers are injected automatically during traced `produce`
calls. You do not need to call `injectHeaders` in the normal send path.

Use `injectHeaders` only when record construction and record publication are decoupled, for example when you need to
prepare a record now and hand it off to some later send path while preserving the current tracing context.

### Configure stable endpoint attributes explicitly

`server.address` and `server.port` are not inferred from Kafka client internals. If you want them on spans, configure
them through `KafkaTracer.Config`.

## Pitfalls And Caveats

### `produce` is intentionally two-stage

This is the most important caveat in the current API.

```scala
import fs2.kafka.{ProducerRecord, ProducerRecords, ProducerResult}
import fs2.kafka.otel4s.trace.TracedKafkaProducer

def stagedOnly(
    producer: TracedKafkaProducer[IO, String, String]
): IO[IO[ProducerResult[String, String]]] =
  producer.produce(
    ProducerRecords.one(
      ProducerRecord("orders", "order-1", """{"status":"created"}""")
    )
  )

def fullyEvaluated(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerResult[String, String]] =
  stagedOnly(producer).flatten
```

If you drop the inner await effect:

- Kafka completion is never awaited
- send-span finalization is delayed
- the span/resource lifecycle effectively leaks until that inner effect is run or discarded with the whole effect graph

If you want normal traced-send behavior, always evaluate the returned inner effect.

### This module is producer-only right now

There is no consumer tracing API in this repository yet.

### Duplicate propagation headers use last-match extraction

When multiple Kafka headers share the same propagation key, extraction prefers the last matching value. This matches
OpenTelemetry Java Kafka instrumentation rather than the generic first-value propagator rule.

### Existing propagated context is preserved

`injectHeaders` does not overwrite a recognized existing propagation context. If the record already carries trace
headers, those headers continue to define the message creation context.

### `withSerializers` is deprecated on traced producers

`withSerializers` is still available because `TracedKafkaProducer` extends the fs2-kafka producer API, but it is not
the safe traced path when the key type changes.

- tracing still works
- spans are still emitted
- `messaging.kafka.message.key` derivation is dropped for the new key type

Use `tracedWithSerializers` instead if you want to preserve key-aware tracing behavior.

### Batch sends change span shape

Single-message sends usually produce one producer `send` span.

For real batches, the instrumentation may:

- create per-record producer `create` spans
- emit a client `send` span for the batch
- attach per-record details on links instead of collapsing conflicting values onto the batch span

That is expected and is how the implementation stays closer to messaging semantic-convention guidance.

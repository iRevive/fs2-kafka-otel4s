# Producers

Add `.traced(config)` and use the normal producer API. Trace headers are added automatically.

```scala mdoc:silent
import cats.effect.IO
import fs2.Stream
import fs2.kafka.otel4s.trace.KafkaTracer
import fs2.kafka.otel4s.trace.syntax._
import org.typelevel.otel4s.trace.TracerProvider
import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords, ProducerResult, ProducerSettings, Serializer}

val tracerConfig: KafkaTracer.Config =
  KafkaTracer.Config.default

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

Use `.produce(records).flatten` to submit records and wait for Kafka to acknowledge them.
The send span ends after acknowledgment.

Use the [interactive guide](../interactive-guides/producer-tracing.md) to compare spans and outgoing headers.

## Single-record spans

For one record without existing trace headers:

```text
                         time -->
                    +----------------+--------------------+
work                | submit records | await Kafka ack    |
                    +----------------+--------------------+

Span lifetime
-----------------------------------------------------------
  send orders       [======================================]

The send span's context is injected into the record headers.
```

## Batch modes

For batches without trace headers:

- `PerRecordSpans` (default) gives each record its own context.
- `SharedSendSpan` uses one send context for all records, creating fewer spans.

```text
PerRecordSpans (default)            SharedSendSpan

  create orders (m1)                 send orders
  create orders (m2)                 context injected into m1, m2
  send orders
  links to both create contexts

Create and send use the active application span as parent, if present.
Rows show relationships. They do not show timing.
```

Existing recognized record trace context is preserved and linked from the send span. A single record with existing
context produces a `CLIENT` send span; without it, a `PRODUCER` send span. See
[Producer instrumentation](../reference/producer-instrumentation.md) for the full batch matrix, attributes, errors, and
transactional behavior.

## Transactions

Transactional APIs remain available on the traced producer:

```scala mdoc:silent
import fs2.kafka.otel4s.trace.TracedKafkaProducer

def sendTransactionally(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerResult[String, String]] =
  producer.produceTransactionally(
    ProducerRecords.one(ProducerRecord("orders", "order-1", """{"status":"created"}"""))
  )
```

Use producer settings configured for transactions when calling transactional APIs.

For tracing policy, see [Configuration](configuration.md). For explicit operation handles, see the [Low-level API](../reference/low-level-api.md).

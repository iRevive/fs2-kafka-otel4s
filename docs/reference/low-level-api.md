# Low-level API

Use operation handles to choose where tracing starts and ends in your own Kafka loop.
They use the same configuration as the consumer and producer adapters. No tracing syntax import is needed.

## Consumer operations without a client

Supply metadata directly to trace a batch without a Kafka client:

```scala mdoc:silent
import cats.effect.IO
import fs2.Chunk
import fs2.kafka.{ConsumerRecord, ProducerRecords, ProducerResult}
import fs2.kafka.otel4s.trace.KafkaTracer
import fs2.kafka.otel4s.trace.ConsumerMetadata

def processBatch(
    records: Chunk[ConsumerRecord[String, String]],
    callback: IO[Unit]
)(implicit kafkaTracer: KafkaTracer[IO]): IO[Unit] =
  kafkaTracer
    .consumerSpans(ConsumerMetadata.Provider.constant[IO](ConsumerMetadata(None, None)))
    .processBatch(records)(callback)
```

Each `ConsumerSpans` operation creates one span around your effect. You handle commits:

```text
Operation                                   Span created       Controlled by
------------------------------------------  -----------------  -------------------------
spans.process(record)(effect)                 process          withProcessSpanSetup
spans.process(committableRecord)(effect)      process          withProcessSpanSetup
spans.processBatch(records)(effect)           process batch    withBatchProcessSpanSetup
spans.receive(records)(effect)                receive          withReceiveSpanSetup
spans.receiveCommittable(records)(effect)     receive          withReceiveSpanSetup
```

Each span surrounds the supplied effect; none of these operations commits offsets. Empty batch operations run the
supplied effect without a span.

## Bound consumer operations

When you are already working with chunked or partitioned consumer streams, call the configured operations on
`tracedConsumer.spans`:

```scala mdoc:silent
import fs2.kafka.otel4s.trace.TracedKafkaConsumer

def processWithSelectedConsumer(
    records: Chunk[ConsumerRecord[String, String]],
    tracedConsumer: TracedKafkaConsumer[IO, String, String]
): IO[Unit] =
  tracedConsumer.spans.processBatch(records)(IO.println(records))
```

Use `spans.process(record)(effect)` for one record. Use `spans.receiveCommittable(chunk)(effect)`
to wrap an effect in a receive span.

## Offset-only commits

If your app or library commits offsets directly, wrap the commit effect with `commitOffsets` to trace it:

```scala mdoc:silent
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

Commit spans include:

- Topics being committed.
- A partition when the map has exactly one topic-partition entry.
- The active span as parent, if one exists. Otherwise, the commit span is a root.

Offset-only commits have no record counts, record details, or message links.
They use the context active at commit time.

`kafkaTracer.consumerSpans(metadata).commitBatchWithin` traces each commit call.
The standard `fs2.kafka.commitBatchWithin` adds no spans.

## Producer operations without a client

For frameworks that own their producer, use `ProducerSpans` with explicit metadata and a send callback:

```scala mdoc:silent
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

The callback must publish the records it receives, including injected headers.
Use `.flatten` to submit records and wait for acknowledgment. Skipping acknowledgment leaves the send span open.

A handle accepts different key and value types at each call. It uses the same configuration as the producer adapter.
You handle transactions.

`tracedProducer.spans` uses the bound producer’s metadata. To supply metadata separately, use:

- `Provider.fromProducer` to read and cache a live client’s ID. Failed lookups are also cached.
- `Provider.constant` to store metadata without retaining a client.

See [Configuration](../getting-started/configuration.md#resource-binding-and-tracer-reuse) for resource binding, tracer reuse, and metadata-provider lifetimes.

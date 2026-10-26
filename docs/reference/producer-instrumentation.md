# Producer instrumentation

This page describes producer spans, headers, attributes, and errors.
For setup, see [Producers](../getting-started/producers.md).
Try the options in the [interactive guide](../interactive-guides/producer-tracing.md).

## Span creation matrix

**Record trace context** is a valid span context extracted from Kafka headers by your configured propagator.
OpenTelemetry calls it the *message creation context*.

| Records and trace headers | `PerRecordSpans` (default) | `SharedSendSpan` |
| --- | --- | --- |
| Empty records | No spans | No spans |
| One record without trace context | One `PRODUCER` `send <topic>`; inject its context; no links | Same |
| One record with trace context | One `CLIENT` `send <topic>`; preserve and link the context | Same |
| Batch where no record has trace context | One `PRODUCER` create span per record plus one `CLIENT` send with one link per record | One `PRODUCER` send; inject the same send context into every record; no send links |
| Batch with trace context on some records | One create span per record without trace context plus one `CLIENT` send with one link per record | One `PRODUCER` send; preserve and link existing record trace context, and inject the send context into other records |
| Batch with trace context on every record | One `CLIENT` send with one link per record and no create spans | Same |
| Batch preparation fails | Previously allocated create spans are released; no send span | No create or send span |
| Transactional produce | The same spans as the corresponding non-transactional produce | Same |
| `injectHeaders` by itself | No spans | No spans |

For a batch of `N` records with `M` missing contexts:

| Mode | Create spans | Send kind | Send links |
| --- | --- | --- | --- |
| `PerRecordSpans` | `M` | `CLIENT` | `N` |
| `SharedSendSpan`, `M > 0` | None | `PRODUCER` | `N - M` |
| `SharedSendSpan`, `M = 0` | None | `CLIENT` | `N` |

The default, `PerRecordSpans`, gives each record its own context. To reduce span count, use `SharedSendSpan`:

```scala
KafkaTracer.Config
  .default
  .withBatchSpanMode(BatchSpanMode.SharedSendSpan)
```

## Recognized record trace context

Your configured propagator must extract a valid `SpanContext`. With W3C Trace Context, a valid header looks like this:

```text
traceparent: 00-80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-01
```

A record is treated as having no trace context when:

- no recognized propagation header exists;
- `traceparent` is malformed;
- its trace ID or span ID is invalid, for example because it is all zeros;
- only baggage or `tracestate` exists without a span context;
- the last value of a duplicate propagation header is malformed or null.

Unsampled contexts and contexts from ended spans are valid. The referenced span need not be exported.
Other configured propagators, such as B3, can use different headers.

For duplicate headers, the last value wins. A malformed or null last value makes the context missing,
even if an earlier value was valid. Production then injects a new context.

Extraction uses `joinOrRoot`:

```scala
Tracer[F].joinOrRoot(record.headers)(Tracer[F].currentSpanContext)
```

The result is `Some(context)` for a valid context, or `None`.

## Explicit header injection versus direct production

Use `produce` for normal sends. It adds trace headers automatically.
Use `injectHeaders` when you want to save the current application context for later publication.

For a record without headers, with an application span active:

| Call | Send kind | Context in headers | Send links |
| --- | --- | --- | --- |
| `produce(records).flatten` | `PRODUCER` | Send span | None |
| `injectHeaders`, then `produce` | `CLIENT` | Application span | Application span |

Both flows preserve existing recognized contexts. If no context is active, `injectHeaders` has nothing to add.

### Batch consequences

Injecting the same application context into every record creates one `CLIENT` send span with one link per record.
All links point to that application context and carry their record’s attributes. No create spans are needed.

Direct production of a batch without headers uses the configured mode:

- `PerRecordSpans` creates and injects a context per record, then links send to each.
- `SharedSendSpan` injects one send context into every record, with no send links.

### Prepare a record for later publication

Use this when the current application span should supply the record context:

```scala mdoc:silent
import cats.effect.IO
import fs2.kafka.ProducerRecord
import fs2.kafka.otel4s.trace.TracedKafkaProducer

def prepareRecord(
    producer: TracedKafkaProducer[IO, String, String]
): IO[ProducerRecord[String, String]] =
  producer.injectHeaders(
    ProducerRecord("orders", "order-1", """{"status":"created"}""")
  )
```

## Single-record sends

### Without trace context in the record headers

The instrumentation creates one span:

- kind: `PRODUCER`;
- name: `send <topic>`;
- links: none.

The send span's context is injected into the Kafka record headers before the record is passed to the underlying producer.

This is the only case where a send span has kind `PRODUCER` in `PerRecordSpans`. In `SharedSendSpan`, a batch send is also `PRODUCER` whenever at least one record receives its context.

### With trace context in the record headers

The instrumentation creates one span:

- kind: `CLIENT`;
- name: `send <topic>`;
- links: one link to the context extracted from the record headers.

Existing headers are preserved. The send span links to their context.

## Batch sends

Any send containing more than one record is treated as a batch.

### `PerRecordSpans`

The send span always has kind `CLIENT`.

For each record without valid trace context in its headers, the instrumentation creates a dedicated span:

- kind: `PRODUCER`;
- name: `create <record-topic>`;
- links: none.

The create span's context is injected into that record. Records that already contain valid contexts retain their existing propagation headers.

The batch send span contains one link for every record. Each link targets either the record trace context extracted from its headers or its generated create span.

### `SharedSendSpan`

No create spans are generated. Records with recognized trace context in their headers retain those headers and contribute links to the send span. Other records receive the send span's context and do not contribute self-links.

The send span is `PRODUCER` when its context is injected into at least one record. If every record already has a context, the send span is `CLIENT` and contains one link per record.

## Link attributes

Every link from a send span describes one produced record and may contain:

- `messaging.destination.name`;
- `messaging.destination.partition.id`, when the partition was explicitly set on the producer record;
- `messaging.kafka.message.key`, when the key can be represented by its `KafkaMessageKey` instance;
- `messaging.kafka.message.tombstone = true`, when the record value is null.

Links do not contain the producer client ID, messaging operation attributes, Kafka offset, or configured constant attributes.

In `SharedSendSpan`, records given the send context have no send links. Their individual attributes
are omitted from the batch span.

## Send-span attributes

Every send span initially contains:

- `messaging.system = "kafka"`;
- `messaging.operation.name = "send"`;
- `messaging.operation.type = "send"`;
- `messaging.client.id`, when configured on the producer;
- `messaging.destination.name`, only when all records target one topic;
- `messaging.destination.partition.id`, only when every record has an explicit partition and all records target the same topic-partition;
- `messaging.batch.message_count`, only when the operation contains more than one record;
- `messaging.kafka.message.key`, only for a single record with a representable key;
- `messaging.kafka.message.tombstone = true`, when a single record is a tombstone or every record in a batch is a tombstone.

After a successful single-record send completes, its Kafka result contributes:

- the actual `messaging.destination.partition.id`;
- `messaging.kafka.offset`.

Batch results do not add result-level partition or offset attributes.

`withSendSpanSetup` attributes override constant attributes, which override derived attributes.
After acknowledgment, single-record result attributes replace matching partition and offset values.

## Create-span attributes

Every generated create span contains:

- `messaging.system = "kafka"`;
- `messaging.operation.name = "create"`;
- `messaging.operation.type = "create"`;
- `messaging.destination.name`;
- `messaging.client.id`, when configured on the producer;
- `messaging.destination.partition.id`, when explicitly set on the producer record;
- `messaging.kafka.message.key`, when representable;
- `messaging.kafka.message.tombstone = true`, when the value is null;
- configured constant attributes.

Create-span naming and setup are fixed. `withSendSpanSetup` only affects send spans.

## Parenting and links

Create and send spans share the active application span as parent. Without one, they are separate roots.
Contexts from record headers become links. They do not become parents.

## Span lifetime and errors

The send span covers both stages of fs2-kafka's producer API:

1. submitting records through the outer `produce` effect;
2. waiting for the Kafka result through the returned effect.

Run both stages with `.flatten`:

```scala
producer.produce(records).flatten
```

Running only the outer effect leaves the send span open. Create spans end after submission,
before Kafka acknowledgment.

With the default send-span finalization strategy:

- an outer submission failure ends the send span with error status;
- an await-stage failure ends the send span with error status;
- the exception is recorded;
- `error.type` contains the exception class name;
- the status description contains the exception message when one is available;
- cancellation sets `error.type = "canceled"` and error status with description `canceled`.

`withSendSpanSetup` can replace the send span's name, additional attributes, and finalization strategy.
Returning `None` suppresses the send span for that batch. It does not change the span kind, link selection, or create spans.

Disabling send spans keeps publishing and create spans enabled. Records missing context use their create span
or the active application context, if available.

If header preparation fails, any create spans are released. No send span is created.

## Transactional APIs

`produceTransactionally` wraps the regular traced produce operation in a Kafka transaction. It does not create a separate transaction span.

`produceAndCommitTransactionally` groups records by `KafkaCommitter`. Each group gets a traced produce call
using the span rules above.

The following operations do not create producer spans by themselves:

- `initTransactions`;
- `transaction`;
- `sendOffsetsToTransaction`;
- `metrics`;
- `partitionsFor`;
- `injectHeaders`.

Empty regular and transactional produce operations delegate directly to the underlying producer and create no spans.

## Client-free operations

`KafkaTracer.producerSpans(ProducerMetadata.Provider.constant(metadata))` creates a handle without a Kafka client.
The producer adapter exposes its handle through `.spans`. Both use the same implementation and tracing rules.

Call `spans.produce(records)(send)`. The callback must publish the prepared records it receives
and return the two-stage fs2-kafka effect.

Empty input calls `send` without metadata lookup or spans. A noop handle also skips header propagation.

Run the acknowledgment effect to end the send span. You handle transactions when using this API directly.

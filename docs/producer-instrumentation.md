# Producer instrumentation

Producer instrumentation decides which spans to create from three inputs:

- the number of records being produced;
- whether each record already contains valid trace context in its Kafka headers;
- the configured `BatchSpanMode`.

## Span creation matrix

In this document, **record trace context** means valid trace information in a record's Kafka headers that the configured propagator can extract. OpenTelemetry calls this the _message creation context_; the shorter wording emphasizes where the context exists and avoids implying that it must be an active parent span.

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

For a batch of `N` records where `M` records do not have valid record trace context, `PerRecordSpans` creates:

- `M` create spans;
- one send span;
- `N` links on the send span;
- `M + 1` spans in total.

`SharedSendSpan` creates one send span and no create spans. When `M > 0`, the send span is `PRODUCER`, its context is injected into those `M` records, and it has `N - M` links to preserved record trace contexts. When `M = 0`, it is a `CLIENT` span with `N` links.

`PerRecordSpans` is the default because it follows OpenTelemetry's higher-fidelity recommendation for batch-oriented APIs. `SharedSendSpan` trades distinct per-record spans, trace contexts, and producer-side details for lower span volume:

```scala
KafkaTracer.Config.default
  .withBatchSpanMode(BatchSpanMode.SharedSendSpan)
```

## Recognized record trace context

A record has recognized trace context when the configured OpenTelemetry propagator can extract a usable `SpanContext` from its Kafka headers.

For example, when W3C Trace Context propagation is configured, this means that the record contains a structurally valid `traceparent`:

```text
traceparent: 00-80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-01
```

A record is treated as having no trace context when:

- no recognized propagation header exists;
- `traceparent` is malformed;
- its trace ID or span ID is invalid, for example because it is all zeros;
- only baggage or `tracestate` exists without a span context;
- the authoritative duplicate propagation header is malformed or null.

An unsampled context is still valid. The referenced span may also have already ended: validity concerns whether a structurally usable span context can be extracted, not whether the referenced span is currently active or exported.

A context does not have to use W3C headers. Other configured propagators, such as B3, may recognize different headers. Any header format from which the configured propagator can extract a span context is accepted.

When duplicate Kafka headers exist for a propagation key, the last matching header is authoritative. For example, a valid `traceparent` followed by a malformed or null `traceparent` is treated as missing. A new context is then injected when the record is prepared for production.

The implementation checks this by evaluating the current span context inside the scope established by `joinOrRoot`:

```scala
Tracer[F].joinOrRoot(record.headers)(Tracer[F].currentSpanContext)
```

The result is `Some(context)` when extraction establishes a span context and `None` otherwise.

## Explicit header injection versus direct production

In normal use, pass records directly to `produce`. It automatically injects the appropriate tracing context, so callers generally should not call `injectHeaders` themselves.

Use `injectHeaders` only when a specific current context should deliberately be written to the record headers before later publication, such as when record construction and publication are decoupled. Calling it before `produce` intentionally changes the producer telemetry.

Assume that an application span is current while both operations run. For a single record without existing propagation headers, these two flows are different:

```scala
tracedProducer
  .injectHeaders(record)
  .flatMap(injected => tracedProducer.produce(ProducerRecords.one(injected)).flatten)
```

creates:

- one `CLIENT` send span;
- a link from the send span to the current application span;
- message headers containing the application span's context.

The explicit injection writes the application span's trace context into the record headers. When `produce` subsequently inspects the record, it preserves that context and represents the Kafka send operation separately as a linked `CLIENT` span.

In contrast, direct production:

```scala
tracedProducer.produce(ProducerRecords.one(record)).flatten
```

creates:

- one `PRODUCER` send span;
- no links on that send span;
- message headers containing the send span's context.

In this flow the instrumentation finds no trace context in the record headers, so it injects the send span's context.

The resulting consumer-side correlation also differs. After explicit injection, consumers correlate with the application span whose context was injected. After direct single-record production, consumers correlate with the producer send span.

The two flows may have the same result when:

- the record headers already contained recognized trace context, because `injectHeaders` preserves it;
- no span context is current while `injectHeaders` runs, because there is no usable current context to propagate.

### Batch consequences

The distinction is larger for batches. If `injectHeaders` runs once for every record while the same application span is current, every previously uninstrumented record receives that same application-span context. The subsequent batch `produce` operation creates:

- no per-record create spans;
- one `CLIENT` send span;
- one link per record, with all links targeting the same application-span context but carrying their own record-specific link attributes.

With `PerRecordSpans`, directly producing the same uninstrumented batch instead creates:

- one distinct `PRODUCER` create span per record;
- one distinct propagated trace context per record;
- one `CLIENT` send span;
- one link from the send span to each create span.

With `SharedSendSpan`, direct production creates one `PRODUCER` send span, injects its context into every record, and creates no producer-side links.

Use explicit `injectHeaders` only when another specific span's trace context should deliberately remain associated with the record. In ordinary production flows, use direct `produce` and let the producer instrumentation inject the appropriate context.

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

The original propagation headers are preserved. The extracted context is represented by a link; it does not become the parent of the send span.

A malformed context is treated as missing. If duplicate propagation headers exist, the last matching value determines whether a valid context is present. A malformed or null last value therefore causes a new context to be injected.

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

In `SharedSendSpan`, records that receive the send span's own context do not have producer-side links. Their varying per-record attributes therefore cannot be represented on the batch send span; this is part of the mode's telemetry-volume trade-off.

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

Configured constant attributes are added after derived semantic-convention attributes and therefore take precedence for duplicate keys. Attributes returned by `withSendSpanSetup` are added last and take precedence over both derived and constant attributes.

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

Create and send spans use the ambient current tracing context as their normal parent. When no ambient span exists, they are normally root spans.

A context extracted from Kafka record headers is never selected as the send span's parent. It is represented by a span link instead. Consequently, generated create spans and the batch send span are siblings under the ambient span, or separate roots when no ambient span exists, connected by links from the send span.

## Span lifetime and errors

The send span covers both stages of fs2-kafka's producer API:

1. submitting records through the outer `produce` effect;
2. waiting for the Kafka result through the returned effect.

The send span remains open until the returned await effect is evaluated. Callers should normally use:

```scala
producer.produce(records).flatten
```

Evaluating only `producer.produce(records)` submits the records but leaves successful send-span finalization pending. Generated create spans in `PerRecordSpans` are released after the outer submission stage completes; they do not remain open while waiting for Kafka acknowledgement.

With the default send-span finalization strategy:

- an outer submission failure ends the send span with error status;
- an await-stage failure ends the send span with error status;
- the exception is recorded;
- `error.type` contains the exception class name;
- the status description contains the exception message when one is available;
- cancellation sets `error.type = "canceled"` and error status with description `canceled`.

`withSendSpanSetup` can replace the send span's name, additional attributes, and finalization strategy. 
Returning `None` suppresses the send span for that batch. It does not change the span kind, link selection, or create spans.

When the send span is suppressed, producing the records and any configured create-span preparation still proceed. 
Records without an existing propagated context can inherit the ambient context, but there is no send-span context to inject.

If propagation fails while preparing a batch, all create spans allocated before the failure are released. The send span is not created because batch preparation did not complete. `SharedSendSpan` has no create spans to release.

## Transactional APIs

`produceTransactionally` wraps the regular traced produce operation in a Kafka transaction. It does not create a separate transaction span.

`produceAndCommitTransactionally` groups records by `KafkaCommitter` and performs one traced produce operation per committer group. Each group independently follows the span creation matrix above. Multiple committer groups can therefore create multiple send spans.

The following operations do not create producer spans by themselves:

- `initTransactions`;
- `transaction`;
- `sendOffsetsToTransaction`;
- `metrics`;
- `partitionsFor`;
- `injectHeaders`.

Empty regular and transactional produce operations delegate directly to the underlying producer and create no spans.

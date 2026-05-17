# Compliance Matrix

This is a living producer-side matrix for the current implementation.

It is intentionally scoped:

- it compares this repository against the OpenTelemetry messaging/propagation spec points that are relevant to the current code
- it compares against the Kafka-specific OpenTelemetry Java instrumentation behaviors we explicitly use as references
- it is not a claim of full parity with the whole `opentelemetry-java-instrumentation` Kafka stack, which also covers consumer flows and internal behaviors outside this module

Legend:

- `Match`: current behavior aligns with the reference
- `Partial`: current behavior aligns in spirit but not in total scope
- `N/A`: the reference does not expose a directly comparable behavior for this case
- `Intentional divergence`: we knowingly chose a different behavior

| Area | OTel spec | otel-java-instrumentation Kafka | This repo | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| Producer scope | Messaging conventions cover producer and consumer spans | Kafka instrumentation covers producer and consumer | Producer only | Partial | Consumer tracing is not implemented here yet. |
| Injection on traced send | Message creation context should be injected into the message | Kafka instrumentation injects via Kafka header setter | Traced `produce` injects automatically | Match | Normal traced sends do not require manual `injectHeaders`. |
| Manual `injectHeaders` helper | Not required, but compatible with propagation model | No equivalent public helper in the compared Kafka helper classes | Explicit helper exists | N/A | Convenience API for decoupled record preparation/publish flows. |
| Overwrite policy for injected propagation headers | Not prescribed generically | `KafkaHeadersSetter` removes matching key then adds one new value | Same behavior | Match | Existing values for the injected key are replaced when we inject. |
| Duplicate propagation header extraction | Generic propagator contract says `Get` returns the first value; spec also allows instrumentations to hardcode extraction logic | `KafkaConsumerRecordGetter.get` uses the last matching header; `getAll` preserves all values in order | Last matching value wins | Match Java / Intentional divergence from generic default | Chosen to match Kafka Java instrumentation semantics. |
| Null-valued propagation headers | Not specifically prescribed | Kafka getter skips null-valued headers | Getter skips null-valued headers | Match Java | Defensive interoperability behavior for externally produced Kafka headers. |
| Single-message send attributes | `messaging.system`, operation attrs, destination name, and canonical key are required/recommended on applicable spans | Producer attributes getter exposes system, destination, client id | Emitted for single-message sends | Match | `messaging.kafka.message.key` is only emitted when the key has a canonical string form. |
| Batch message count | Should be set for batch operations and not for single-message operations | Kafka producer helper returns no batch count because the Java producer API being instrumented is single-record | Emitted only for actual batches | Match spec / N/A for Java | This repo exposes batch APIs directly via `ProducerRecords`. |
| Batch create/send structure | Batch-oriented APIs are recommended to create per-message `create` spans plus a linked `send` span | Not directly comparable in the Kafka Java producer helper classes we use as reference | Implemented | Match spec / N/A for Java | For batch sends we create per-record `create` spans and a linked batch `send` span. |
| Existing custom/propagated creation context preservation | Existing message creation context should be preserved | Not directly compared from the referenced helper classes | Implemented | Match spec | If a record already carries recognized propagation headers, we preserve them. |
| Server endpoint attributes | `server.address` / `server.port` should describe the logical broker/service when available | Not inferred by the compared helper classes | Explicit opt-in via config | Partial | We require the user to provide logical endpoint metadata explicitly. |
| Failed send error reporting | Span error information should be recorded, including `error.type` | Comparable behavior exists in full instrumentation, but is not part of the specific helper classes compared here | `error.type` and error status are emitted | Match spec | Covered by tests for failed send completion. |
| Two-stage `produce` contract | Spec does not discuss fs2-kafka’s outer/inner effect shape | Java producer API does not expose an equivalent staged outer/inner effect | Traced `produce` preserves the two-stage contract | N/A | If the returned inner await effect is dropped, send-span finalization is delayed; this is a repo-specific caveat. |

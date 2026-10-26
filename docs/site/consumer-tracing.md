# Consumer tracing

Build a consumer tracing configuration, inspect the resulting spans, and copy the complete Scala example. Add constant
attributes to see them appear immediately on library spans and in the generated code. Work stays the same when a span
is disabled: handlers still run, and the owned-commit helpers still commit the original chunk's offsets after success.

The example assumes successful, sequential processing within one partition, recognized incoming message contexts,
and no ambient app span. Durations are illustrative. Rows at the same indentation are independent root spans;
indented rows are actual child spans. Incoming message contexts become links rather than parents.

<div class="consumer-explorer" data-consumer-explorer hidden>
  <div class="ce-picker">
    <fieldset class="ce-controls">
      <legend>Consumer and tracing</legend>
      <label>Consumer API <select data-option="method"><option value="chunk">consumeChunkTraced</option><option value="record">consumeRecordTraced</option><option value="stream">recordsTraced</option></select></label>
      <label>Topic <input data-option="topic" value="orders"></label>
      <label>Client ID <input data-option="clientId" value="orders-consumer"></label>
      <label>Group ID <input data-option="groupId" value="orders-group"></label>
      <div class="ce-toggles">
        <label><input type="checkbox" data-option="receive" checked> Receive span</label>
        <label><input type="checkbox" data-option="process" checked> <span data-process-label>Batch process span</span></label>
        <label data-commit-control><input type="checkbox" data-option="commit" checked> Commit span</label>
        <label data-children-control><input type="checkbox" data-option="children"> App processor span per message</label>
      </div>
    </fieldset>
    <fieldset class="ce-constants">
      <legend>Constant span attributes</legend>
      <p>Attached to every library span. App spans keep their own attributes.</p>
      <div data-attribute-editor></div>
      <button type="button" data-add-attribute>+ Add attribute</button>
      <p data-errors role="status"></p>
    </fieldset>
  </div>
  <section class="ce-result" aria-label="Example trace timeline">
    <h2>Trace timeline</h2>
    <p><strong>Input:</strong> <code data-input></code></p>
    <div class="ce-work" data-work></div>
    <p class="ce-muted" data-completion></p>
    <div class="ce-span-row ce-heading"><strong>Span</strong><div class="ce-axis"><span>0</span><span>10</span><span>20</span><span>30</span><span>40 ms</span></div><strong>Duration</strong></div>
    <div data-spans></div>
    <p class="ce-muted" data-parentage></p>
    <p class="ce-muted">Select a span name or bar to inspect its attributes. Times and incoming links are illustrative, not live telemetry.</p>
    <span class="ce-sr-only" aria-live="polite" data-selection-status></span>
    <section class="ce-inspector" data-inspector aria-label="Selected span details"></section>
    <section class="ce-generated" aria-label="Generated Scala example">
      <div class="ce-code-heading"><h2>Generated Scala</h2><button type="button" data-copy>Copy code</button><span data-copy-status role="status"></span></div>
      <p class="ce-muted">Complete consumer allocation, subscription, tracing configuration, and processing. Supply your configured otel4s TracerProvider and Kafka broker.</p>
      <pre><code class="language-scala" data-code></code></pre>
    </section>
  </section>
</div>

<noscript><p>The interactive selector requires JavaScript. The span behavior is described below.</p></noscript>

## Which helper should I use?

| Helper | Handler calls | Process spans | Commit ownership |
| --- | --- | --- | --- |
| `consumeRecordTraced` | Once per record | One per record | Adapter commits after all records in the chunk succeed |
| `consumeChunkTraced` | Once per chunk | One for the entire chunk | Adapter commits after the chunk callback succeeds |
| `recordsTraced` | Once per record | One per record | App owns downstream committing |

For both owned-commit helpers, receive stays open while the callback runs; commit occurs afterward, outside receive.
For `recordsTraced`, receive ends after chunk delivery, before per-record callbacks. The standard fs2-kafka
`commitBatchWithin` pipe does not add tracing by itself. Failure or cancellation of an owned-commit callback skips
its commit.

## Configure spans independently

Return `None` from a setup function to suppress that operation's span. To disable the automatic batch span while
retaining explicit single-record operations:

```scala mdoc:silent
import fs2.kafka.otel4s.trace.KafkaTracer

val manualRecordConfig =
  KafkaTracer.Config.default.withBatchProcessSpanSetup(_ => None)
```

| Configuration | Affects |
| --- | --- |
| `withReceiveSpanSetup` / `withoutReceiveSpans` | Receive spans |
| `withProcessSpanSetup` | Single-record process spans |
| `withBatchProcessSpanSetup` | Batch process spans |
| `withoutProcessSpans` | Both single-record and batch process spans |
| `withCommitSpanSetup` / `withoutCommitSpans` | Commit spans |

Span suppression differs from `KafkaTracer.noop`, which also bypasses propagation.

## Add child spans around selected work

For messages that carry the latest state of a key, your chunk callback can select only the latest records per key.
Use ordinary app spans to measure each selected record's work as a child of the active batch span:

```scala mdoc:silent
import cats.effect.IO
import cats.syntax.all._
import fs2.Chunk
import fs2.kafka.ConsumerRecord
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import fs2.kafka.otel4s.trace.TracedKafkaConsumer
import org.typelevel.otel4s.trace.Tracer

def consumeSelected(
    consumer: TracedKafkaConsumer[IO, String, String],
    selectLatest: Chunk[ConsumerRecord[String, String]] => Chunk[ConsumerRecord[String, String]],
    handle: ConsumerRecord[String, String] => IO[Unit]
)(implicit tracer: Tracer[IO]): IO[Nothing] =
  consumer.consumeChunkTraced { chunk =>
    selectLatest(chunk).traverse_ { record =>
      tracer.span("processor").surround(handle(record))
    }.as(CommitNow)
  }
```

Enable “App processor span per message” in the selector to see this parentage. If the batch process span is disabled,
the app spans become children of receive when it is enabled. With both disabled and no ambient span, they are roots.
The selector illustrates handling all three messages; selecting fewer records creates fewer app spans.

`consumer.spans.process(record)(effect)` has different semantics: it creates a root linked to the incoming record
context and honors `withProcessSpanSetup`. It does not create a child of the batch span. In either approach, the adapter
commits the original chunk's offsets only after the whole callback succeeds.

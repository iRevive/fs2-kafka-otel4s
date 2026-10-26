# Consumer tracing configurator

Choose tracing options, inspect the spans, and copy the Scala example.
Disabling spans keeps processing and commits enabled.

The example uses three messages in one partition, with no active application span.

## Configure your consumer

<div class="tracing-explorer" data-consumer-explorer data-explorer-panel hidden>
  <div class="ce-picker">
    <fieldset class="ce-controls">
      <legend>Consumer and tracing</legend>
      <label>Consumer API <select data-option="method"><option value="record">consumeRecordTraced</option><option value="chunk">consumeChunkTraced</option><option value="stream">recordsTraced</option></select></label>
      <label>Topic <input data-option="topic" value="orders"></label>
      <label>Client ID <input data-option="clientId" value="orders-consumer"></label>
      <label>Group ID <input data-option="groupId" value="orders-group"></label>
      <div class="ce-toggles">
        <label><input type="checkbox" data-option="receive" checked> Receive span</label>
        <label><input type="checkbox" data-option="process" checked> <span data-process-label>Record process spans</span></label>
        <label data-commit-control><input type="checkbox" data-option="commit" checked> Commit span</label>
        <label data-children-control><input type="checkbox" data-option="children"> Child process span per message</label>
        <label><input type="checkbox" data-option="incoming" checked> Incoming trace headers</label>
      </div>
    </fieldset>
    <fieldset class="ce-constants">
      <legend>Constant span attributes</legend>
      <p>Added to library spans only.</p>
      <div data-attribute-editor></div>
      <button type="button" data-add-attribute>+ Add attribute</button>
      <p data-errors role="status"></p>
    </fieldset>
  </div>
</div>

## Trace timeline

Expand a span to see its attributes and parent. Follow a link to see the message context.

<div class="tracing-explorer" data-explorer-panel hidden>
  <p><strong>Input:</strong> <code data-input></code></p>
  <div class="ce-work" data-work></div>
  <p class="ce-muted" data-completion></p>
  <div class="ce-span-row ce-heading"><strong>Span</strong><div class="ce-axis" data-axis></div><strong>Duration</strong><strong class="ce-links-heading">Links</strong></div>
  <div data-spans></div>
  <p class="ce-muted" data-parentage></p>
  <p class="ce-muted" data-incoming></p>
  <p class="ce-muted">Sample times and contexts. Indentation shows parent-child relationships.</p>
</div>

### Incoming message contexts

Consumer spans link to these contexts. Each linked consumer span starts its own trace.

<div class="tracing-explorer ce-contexts" data-explorer-panel data-contexts hidden></div>

## Generated Scala

Supply a configured otel4s `TracerProvider`. Run Kafka at `localhost:9092` and create the selected topic.

<div class="tracing-explorer" data-explorer-panel hidden>
  <div class="ce-code-actions"><button type="button" data-copy>Copy code</button><span data-copy-status role="status"></span></div>
  <pre><code class="language-scala" data-code></code></pre>
</div>

<noscript><p>The interactive selector requires JavaScript. The span behavior is described below.</p></noscript>

For complete handler examples and span diagrams, see [Consumers](../getting-started/consumers.md).
For shared tracing options, see [Configuration](../getting-started/configuration.md).

## Choose a consumer API

| API | Handler calls | Process spans | Commits |
| --- | --- | --- | --- |
| `consumeRecordTraced` | Once per record | One per record | After all handlers in the chunk succeed |
| `consumeChunkTraced` | Once per chunk | One for the entire chunk | After the chunk handler succeeds |
| `recordsTraced` | Once per record | One per record | You choose a downstream commit pipe |

The consume helpers keep receive open during processing, then commit after it ends.
A failed or canceled handler skips the commit. With `recordsTraced`, receive ends before processing.

The stream example uses `consumer.spans.commitBatchWithin` for traced commits. Disabling commit spans
switches to the standard fs2-kafka pipe. Both pipes commit offsets.

## Configure spans independently

Return `None` from a setup function to disable a span. For example, disable the batch process span:

```scala mdoc:silent
import fs2.kafka.otel4s.trace.KafkaTracer

val manualRecordConfig =
  KafkaTracer.Config
    .default
    .withBatchProcessSpanSetup(_ => None)
```

| Configuration | Affects |
| --- | --- |
| `withReceiveSpanSetup` / `withoutReceiveSpans` | Receive spans |
| `withProcessSpanSetup` | Single-record process spans |
| `withBatchProcessSpanSetup` | Batch process spans |
| `withoutProcessSpans` | Both single-record and batch process spans |
| `withCommitSpanSetup` / `withoutCommitSpans` | Commit spans |

`KafkaTracer.noop` disables both spans and propagation.

## Add child spans around selected work

A chunk handler can select records before processing. This example keeps the latest record per key
and adds an application span for each selected record:

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
      Tracer[IO].span("process").surround(handle(record))
    }.as(CommitNow)
  }
```

Enable “Child process span per message” to see the result. The demo processes all three messages.
Selecting fewer records creates fewer child spans.

`consumer.spans.process(record)(effect)` creates a root span linked to the message context.
It uses `withProcessSpanSetup`. The chunk handler still commits the original chunk after success.

## Read the relationships

| Span | Parent in this example | Message links |
| --- | --- | --- |
| Receive | None | All recognized contexts in the chunk |
| Record process | None | Its message context |
| Batch process | None | All recognized contexts in the chunk |
| Application child | Batch process, or receive if batch tracing is disabled | None |
| Commit | None | None |

If both batch process and receive are disabled, application spans are roots.
Turning off incoming headers removes message links. It does not change the generated consumer code.

The stream example commits batches of three offsets. Actual timing and batch sizes depend on your application.

Constant attributes can replace derived attributes. Span names and links stay the same.
Selections are saved in the URL. Keep secrets out of shared attribute values.

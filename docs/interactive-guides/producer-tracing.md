# Producer tracing configurator

Choose a batch mode, inspect spans and outgoing headers, and copy the Scala example.
For a first send, see [Producers](../getting-started/producers.md).

## Configure your producer

<div class="tracing-explorer" data-producer-explorer data-explorer-panel hidden>
  <div class="ce-picker">
    <fieldset class="ce-controls">
      <legend>Producer and tracing</legend>
      <label>Records <select data-option="count"><option value="3">Batch of three</option><option value="1">Single record</option><option value="0">Empty batch</option></select></label>
      <label>Batch mode <select data-option="mode"><option value="PerRecordSpans">PerRecordSpans (default)</option><option value="SharedSendSpan">SharedSendSpan</option></select></label>
      <label>Trace headers <select data-option="incoming"><option value="none">No incoming contexts</option><option value="some">Incoming context on m1</option><option value="all">Incoming context on every record</option></select></label>
      <label>Topic <input data-option="topic" value="orders"></label>
      <label>Client ID <input data-option="clientId" value="orders-producer"></label>
      <div class="ce-toggles">
        <label><input type="checkbox" data-option="send" checked> Send span</label>
        <label><input type="checkbox" data-option="ambient"> Surround with an application span</label>
        <label><input type="checkbox" data-option="partition"> Explicit partition 0</label>
        <label><input type="checkbox" data-option="tombstone"> All records are tombstones</label>
      </div>
    </fieldset>
    <fieldset class="ce-constants">
      <legend>Constant span attributes</legend>
      <p>Added to create and send spans only.</p>
      <div data-attribute-editor></div>
      <button type="button" data-add-attribute>+ Add attribute</button>
      <p data-errors role="status"></p>
    </fieldset>
  </div>
</div>

## Trace timeline

Expand a span to see its attributes and links. Indented spans are children of the application span.

<div class="tracing-explorer" data-explorer-panel hidden>
  <p><strong>Input:</strong> <code data-input></code></p>
  <div class="ce-work" data-work></div>
  <p class="ce-muted" data-completion></p>
  <div class="ce-span-row ce-heading"><strong>Span</strong><div class="ce-axis" data-axis></div><strong>Duration</strong><strong class="ce-links-heading">Links</strong></div>
  <div data-spans></div>
  <p class="ce-muted" data-parentage></p>
  <p class="ce-muted">Sample times and results for successful sends. Single-record results use partition 0 and offset 42.</p>
</div>

### Incoming message contexts

Existing trace contexts are preserved. The demo uses sample W3C `traceparent` headers and needs a W3C propagator.
In your application, keep the upstream headers.

<div class="tracing-explorer ce-contexts" data-explorer-panel data-contexts hidden></div>

## Outgoing trace headers

Follow a context link to see its source. For records in a batch without existing context:

- `SharedSendSpan` injects the send context.
- `PerRecordSpans` creates and injects a separate context per record.

<div class="tracing-explorer" data-explorer-panel hidden>
  <div data-output></div>
  <p class="ce-muted" data-incoming></p>
</div>

## Generated Scala

Supply a configured otel4s `TracerProvider`. Run Kafka at `localhost:9092` and create the selected topic.
The code uses `.flatten` to submit records and wait for acknowledgment.

<div class="tracing-explorer" data-explorer-panel hidden>
  <div class="ce-code-actions"><button type="button" data-copy>Copy code</button><span data-copy-status role="status"></span></div>
  <pre><code class="language-scala" data-code></code></pre>
</div>

<noscript><p>The interactive selector requires JavaScript. The rules below and the producer instrumentation reference describe the same behavior.</p></noscript>

## Understand the choices

| Choice | Effect |
| --- | --- |
| `PerRecordSpans` | For batches, create a span for each record missing trace context, then a `CLIENT` send span linked to every record context |
| `SharedSendSpan` | Create one send span; preserve existing contexts and inject its context into other records |
| Single record | The batch mode has no effect: a missing context gives a `PRODUCER` send span; an existing context gives a linked `CLIENT` send span |
| Empty batch | Call the underlying producer without spans or header preparation |
| Disable send span | Keep publishing and create spans. Records without context use a create span or the active application span, if available |
| Application span | Create and send become siblings under the application span; incoming record contexts remain links |

`withSendSpanSetup` customizes send spans. Create-span names and setup are fixed.
`KafkaTracer.noop` disables all spans and propagation.

Constant attributes apply to library spans. They do not change names, outgoing contexts, or link attributes.
Single-record results add the partition and offset after acknowledgment, replacing matching constant attributes.

For mixed topics, partitions, and other rules, see [Producer instrumentation](../reference/producer-instrumentation.md).
For shared options, see [Configuration](../getting-started/configuration.md).

Selections are saved in the URL. Keep secrets out of shared attribute values.

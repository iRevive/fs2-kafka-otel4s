/* Copyright 2026 Typelevel. Licensed under the Apache License, Version 2.0. */
(function () {
  "use strict";
  const { quote, constantAttributes, mount } = typeof module !== "undefined" && module.exports
    ? require("./tracing-explorer.js") : window.TracingExplorer;
  const defaults = { count: "3", mode: "PerRecordSpans", incoming: "none", send: true, ambient: false,
    partition: false, tombstone: false, topic: "orders", clientId: "orders-producer", attributes: [] };
  const traceparent = i => `00-${String(i + 1).padStart(32, "0")}-${String(i + 1).padStart(16, "0")}-01`;

  function fullCode(state, config, records) {
    const hasHeaders = records.some(r => r.incoming);
    const lines = [
      "import cats.effect.IO", "import fs2.Stream",
      "import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords, ProducerSettings, Serializer}",
      ...(hasHeaders ? ["import fs2.kafka.{Header, Headers}"] : []),
      "import fs2.kafka.otel4s.trace.{BatchSpanMode, KafkaTracer}",
      "import fs2.kafka.otel4s.trace.syntax._",
      ...(config.includes(".withConstAttributes") ? ["import org.typelevel.otel4s.{Attribute, Attributes}"] : []),
      "import org.typelevel.otel4s.trace.TracerProvider", "",
      "// Supply a configured otel4s backend and Kafka broker.",
      ...(hasHeaders ? ["// Synthetic incoming W3C contexts for this demo; configure a W3C propagator.",
        "// In an application, preserve real upstream headers instead of hard-coding contexts."] : []),
      "def run(implicit provider: TracerProvider[IO]): IO[Unit] = {",
      "  val settings = ProducerSettings[IO, String, String](",
      "    Serializer[IO, String], Serializer[IO, String]",
      '  ).withBootstrapServers("localhost:9092")', `    .withClientId(${quote(state.clientId)})`, "",
      "  val config = " + config.split("\n").join("\n  "), "",
      "  val records: ProducerRecords[String, String] = ProducerRecords(",
      "    List[ProducerRecord[String, String]](",
    ];
    records.forEach((record, i) => {
      let code = `      ProducerRecord(${quote(state.topic)}, ${quote(record.id)}, ${state.tombstone ? "null: String" : quote(`Order ${i + 1}`)})`;
      if (state.partition) code += ".withPartition(0)";
      if (record.incoming) code += `.withHeaders(Headers(Header("traceparent", ${quote(traceparent(i))})))`;
      lines.push(code + (i < records.length - 1 ? "," : ""));
    });
    lines.push("    )", "  )", "", "  val publish = Stream",
      "    .resource(KafkaProducer.resource[IO, String, String](settings))", "    .traced(config)",
      "    .evalMap(_.produce(records).flatten)", "    .compile.drain", "");
    if (state.ambient) lines.push('  provider.tracer("orders-app").get.flatMap { appTracer =>',
      '    appTracer.span("publish orders").surround(publish)', "  }");
    else lines.push("  publish");
    lines.push("}");
    return lines.join("\n");
  }

  function describe(input) {
    const state = { ...defaults, ...input };
    const count = ["0", "1", "3"].includes(String(state.count)) ? Number(state.count) : 3;
    const constants = constantAttributes(state.attributes);
    const columns = count + 2;
    const records = Array.from({ length: count }, (_, i) => ({ id: `m${i + 1}`, index: i,
      incoming: state.incoming === "all" || (state.incoming === "some" && i === 0) }));
    const createRecords = state.mode === "PerRecordSpans" && count > 1 ? records.filter(r => !r.incoming) : [];
    const spans = [];
    const attr = (key, value, type = "string", source = "Record / client metadata") => ({ key, value: String(value), type, source });
    const recordAttributes = record => [attr("messaging.destination.name", state.topic),
      ...(state.partition ? [attr("messaging.destination.partition.id", "0")] : []),
      attr("messaging.kafka.message.key", record.id),
      ...(state.tombstone ? [attr("messaging.kafka.message.tombstone", true, "boolean")] : [])];
    function add(id, operation, kind, start, length, record, links) {
      const derived = Object.create(null);
      [attr("messaging.system", "kafka"), attr("messaging.operation.name", operation), attr("messaging.operation.type", operation),
        attr("messaging.destination.name", state.topic), ...(state.clientId ? [attr("messaging.client.id", state.clientId)] : []),
        ...(state.partition ? [attr("messaging.destination.partition.id", "0")] : []),
        ...(record ? [attr("messaging.kafka.message.key", record.id)] : []),
        ...(state.tombstone ? [attr("messaging.kafka.message.tombstone", true, "boolean")] : []),
        ...(operation === "send" && count > 1 ? [attr("messaging.batch.message_count", count, "long")] : []),
      ].forEach(a => derived[a.key] = a);
      const overridden = [];
      constants.attributes.forEach(a => { if (derived[a.key]) overridden.push(a.key); derived[a.key] = a; });
      // Result attributes are added after constant attributes by ProducerSpans.
      if (operation === "send" && count === 1) {
        [attr("messaging.destination.partition.id", "0", "string", "Kafka acknowledgment (illustrative)"),
          attr("messaging.kafka.offset", 42, "long", "Kafka acknowledgment (illustrative)")].forEach(a => derived[a.key] = a);
      }
      spans.push({ id, name: `${operation} ${state.topic}`, record: record ? record.id : null,
        kind, scope: "fs2.kafka", parent: state.ambient ? "app" : null, start, length, links,
        setup: operation === "create" ? "Fixed create-span setup" : "withSendSpanSetup",
        attributes: Object.values(derived), overridden: overridden.filter(key => derived[key].source === "withConstAttributes"), constantCount: constants.attributes.length });
    }
    if (state.ambient) spans.push({ id: "app", name: "publish orders", kind: "INTERNAL", scope: "orders-app",
      parent: null, start: 0, length: columns, links: [], setup: 'Tracer[IO].span("publish orders")', attributes: [], overridden: [] });
    createRecords.forEach(record => add(`create-${record.id}`, "create", "PRODUCER", record.index, count + 1 - record.index, record, []));
    const linkedRecords = records.filter(r => r.incoming || createRecords.includes(r));
    if (state.send && count) add("send", "send", records.some(r => !r.incoming && !createRecords.includes(r)) ? "PRODUCER" : "CLIENT",
      count, 2, count === 1 ? records[0] : null, linkedRecords.map(record => ({
        label: record.incoming ? `incoming ${record.id}` : `create ${record.id}`,
        target: record.incoming ? `incoming-${record.id}` : `span-create-${record.id}`, attributes: recordAttributes(record)
      })));
    const outgoing = records.map(record => ({ record: record.id,
      context: record.incoming ? `incoming ${record.id}` : createRecords.includes(record) ? `create ${record.id}` : state.send ? "send" : state.ambient ? "application" : "none",
      target: record.incoming ? `incoming-${record.id}` : createRecords.includes(record) ? `span-create-${record.id}` : state.send ? "span-send" : state.ambient ? "span-app" : null,
      action: record.incoming ? "Preserved" : state.send || state.ambient || createRecords.includes(record) ? "Injected" : "No context available" }));
    const config = ["KafkaTracer.Config", "  .default", `  .withBatchSpanMode(BatchSpanMode.${state.mode})`];
    if (!state.send) config.push("  .withoutSendSpans");
    if (constants.attributes.length) {
      config.push("  .withConstAttributes(Attributes(");
      constants.attributes.forEach((a, i) => config.push(`    Attribute(${quote(a.key)}, ${a.literal})${i < constants.attributes.length - 1 ? "," : ""}`));
      config.push("  ))");
    }
    return { spans, outgoing, config: config.join("\n"), code: fullCode(state, config.join("\n"), records), errors: constants.errors,
      input: `${state.topic} · ${count} record${count === 1 ? "" : "s"}${state.partition ? " · explicit partition 0" : " · broker chooses partition"}${state.tombstone ? " · tombstones" : ""}`,
      columns, work: [...records.map(r => `prepare ${r.id}`), "submit records", "await Kafka ack"],
      contexts: records.filter(r => r.incoming).map(r => ({ id: `incoming-${r.id}`, title: `${r.id} · incoming message context`,
        description: `Existing context in ${r.id}'s headers. Send preserves it and adds a link. Sample traceparent: ${traceparent(r.index)}.` })),
      noSpans: "No library spans. Records are still published and acknowledged.",
      completion: count ? "Send ends after acknowledgment. Create spans end after submission. Use .flatten to run both produce stages."
        : "Empty input calls the producer without spans, metadata lookup, or propagation.",
      parentage: state.ambient ? "Create and send are children of the application span."
        : "Create and send are separate roots. Each starts a trace.",
      incoming: "Existing contexts are preserved. New contexts come from create, send, or the active application span. If none is available, no context is added."
    };
  }
  if (typeof module !== "undefined" && module.exports) module.exports = { defaults, describe };
  if (typeof document === "undefined") return;
  const adapter = { defaults, describe, choices: { count: ["0", "1", "3"], mode: ["PerRecordSpans", "SharedSendSpan"], incoming: ["none", "some", "all"] },
    booleans: ["send", "ambient", "partition", "tombstone"], texts: ["topic", "clientId"],
    renderExtras({ find, view, text }) {
      const output = find("output"); output.replaceChildren();
      if (!view.outgoing.length) { output.append(text("p", "No outgoing messages.")); return; }
      const table = document.createElement("table"); table.className = "ce-attributes";
      const head = document.createElement("thead"); const headings = document.createElement("tr");
      ["Record", "Context in outgoing headers", "Action"].forEach(h => headings.append(text("th", h))); head.append(headings); table.append(head);
      const body = document.createElement("tbody");
      view.outgoing.forEach(record => {
        const row = document.createElement("tr"); const context = document.createElement("td");
        if (record.target) { const link = text("a", record.context); link.href = `#${record.target}`; context.append(link); }
        else context.textContent = record.context;
        row.append(text("td", record.record), context, text("td", record.action)); body.append(row);
      });
      table.append(body); output.append(table);
    }
  };
  const start = () => document.querySelectorAll("[data-producer-explorer]").forEach(root => mount(root, adapter));
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start, { once: true }); else start();
})();

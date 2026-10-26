/* Copyright 2026 Typelevel. Licensed under the Apache License, Version 2.0. */
(function () {
  "use strict";
  const defaults = { method: "chunk", receive: true, process: true, commit: true, children: false, topic: "orders", clientId: "orders-consumer", groupId: "orders-group", attributes: [] };
  const quote = (value) => JSON.stringify(value).replace(/\u2028/g, "\\u2028").replace(/\u2029/g, "\\u2029");

  function constantAttributes(rows) {
    const attributes = [];
    const errors = [];
    rows.forEach((row, index) => {
      const key = String(row.key || "").trim();
      const type = row.type || "string";
      const value = String(row.value ?? "");
      let literal;
      if (!key) { errors.push(`Attribute ${index + 1}: enter a key.`); return; }
      if (type === "long") {
        if (!/^-?\d+$/.test(value) || BigInt(value) < -9223372036854775808n || BigInt(value) > 9223372036854775807n) {
          errors.push(`Attribute ${index + 1}: enter a signed 64-bit integer.`); return;
        }
        literal = `${BigInt(value)}L`;
      } else if (type === "boolean") {
        if (value !== "true" && value !== "false") { errors.push(`Attribute ${index + 1}: enter true or false.`); return; }
        literal = value;
      } else if (type === "string") literal = quote(value);
      else { errors.push(`Attribute ${index + 1}: unsupported type.`); return; }
      attributes.push({ key, value: type === "long" ? String(BigInt(value)) : value, type, literal, source: "withConstAttributes" });
    });
    return { attributes, errors };
  }

  function fullCode(state, config) {
    const stream = state.method === "stream";
    const chunk = state.method === "chunk";
    const body = stream ? [
      "        traced.recordsTraced { committable =>",
      "          handle(committable.record).as(committable.offset)",
      "        }",
      "          .through(commitBatchWithin[IO](500, 15.seconds))",
      "          .compile.drain",
    ] : chunk ? [
      "        traced.consumeChunkTraced { chunk =>",
      "          chunk.traverse_ { record =>",
      state.children ? '            appTracer.span("processor").surround(handle(record))' : "            handle(record)",
      "          }.as(CommitNow)",
      "        }",
    ] : ["        traced.consumeRecordTraced(handle)"];
    return [
      "import cats.effect.IO", "import cats.syntax.all._", "import fs2.kafka._",
      ...(chunk ? ["import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow"] : []),
      "import fs2.kafka.otel4s.trace.KafkaTracer", "import org.typelevel.otel4s.{Attribute, Attributes}",
      "import org.typelevel.otel4s.trace.TracerProvider",
      ...(stream ? ["import scala.concurrent.duration._"] : []), "",
      "// Supply the TracerProvider from your configured otel4s backend.",
      "def run(implicit provider: TracerProvider[IO]): IO[Unit] = {",
      "  val settings = ConsumerSettings[IO, String, String](",
      "    Deserializer[IO, String], Deserializer[IO, String]",
      '  ).withBootstrapServers("localhost:9092")',
      `    .withClientId(${quote(state.clientId)})`, `    .withGroupId(${quote(state.groupId)})`, "",
      "  val config = " + config.split("\n").join("\n  "), "",
      "  def handle(record: ConsumerRecord[String, String]): IO[Unit] =",
      "    IO.println(record.value)", "", "  for {",
      ...(chunk && state.children ? ['    appTracer <- provider.tracer("orders-app").get'] : []),
      "    kafkaTracer <- KafkaTracer.create[IO](config)",
      "    _ <- KafkaConsumer.resource[IO, String, String](settings).use { consumer =>",
      "      val traced = kafkaTracer.consumer(consumer)",
      `      consumer.subscribeTo(${quote(state.topic)}) *> {`, ...body,
      "      }", "    }", "  } yield ()", "}",
    ].join("\n");
  }

  function describe(input) {
    const state = { ...defaults, ...input };
    const chunk = state.method === "chunk";
    const stream = state.method === "stream";
    const constants = constantAttributes(state.attributes);
    const spans = [];
    function add(id, operation, start, length, setup, recordIndex, parent) {
      const app = operation === "processor";
      const derived = {};
      function attr(key, value, type) { derived[key] = { key, value: String(value), type: type || "string", source: "Record / client metadata" }; }
      if (!app) {
        attr("messaging.system", "kafka");
        attr("messaging.operation.name", operation === "receive" ? "poll" : operation.startsWith("process") ? "process" : "commit");
        attr("messaging.operation.type", operation === "receive" ? "receive" : operation.startsWith("process") ? "process" : "settle");
        attr("messaging.destination.name", state.topic);
        attr("messaging.destination.partition.id", "0");
        attr("messaging.client.id", state.clientId);
        attr("messaging.consumer.group.name", state.groupId);
        if (recordIndex !== undefined) {
          attr("messaging.kafka.message.key", `m${recordIndex + 1}`);
          attr("messaging.kafka.offset", recordIndex + 1, "long");
        } else attr("messaging.batch.message_count", 3, "long");
      }
      const overridden = [];
      if (!app) constants.attributes.forEach((a) => {
        if (derived[a.key]) overridden.push(a.key);
        derived[a.key] = a;
      });
      spans.push({ id, name: app ? "processor" : `${operation === "receive" ? "poll" : operation.startsWith("process") ? "process" : "commit"} ${state.topic}`,
        record: recordIndex === undefined ? null : `m${recordIndex + 1}`, start, length, setup, parent: parent || null,
        kind: app ? "INTERNAL" : operation.startsWith("process") ? "CONSUMER" : "CLIENT", scope: app ? "orders-app" : "fs2.kafka",
        attributes: Object.values(derived), overridden, constantCount: app ? 0 : new Set(constants.attributes.map(a => a.key)).size,
        links: app || operation === "commit" ? [] : recordIndex === undefined ? ["m1", "m2", "m3"] : [`m${recordIndex + 1}`] });
    }
    if (state.receive) add("receive", "receive", 0, stream ? 1 : 3, "withReceiveSpanSetup");
    if (state.process) {
      if (chunk) add("batch", "process batch", 0, 3, "withBatchProcessSpanSetup");
      else for (let i = 0; i < 3; i++) add(`process-${i}`, "process", i + (stream ? 1 : 0), 1, "withProcessSpanSetup", i);
    }
    if (chunk && state.children) {
      const parent = state.process ? "batch" : state.receive ? "receive" : null;
      for (let i = 0; i < 3; i++) add(`app-${i}`, "processor", i, 1, 'Tracer[IO].span("processor")', i, parent);
    }
    if (!stream && state.commit) add("commit", "commit", 3, 1, "withCommitSpanSetup");
    const config = ["KafkaTracer.Config.default"];
    if (!state.receive) config.push("  .withoutReceiveSpans");
    if (!state.process) config.push(`  .${chunk ? "withBatchProcessSpanSetup" : "withProcessSpanSetup"}(_ => None)`);
    if (!stream && !state.commit) config.push("  .withoutCommitSpans");
    if (constants.attributes.length) {
      config.push("  .withConstAttributes(Attributes(");
      constants.attributes.forEach((a, i) => config.push(`    Attribute(${quote(a.key)}, ${a.literal})${i < constants.attributes.length - 1 ? "," : ""}`));
      config.push("  ))");
    }
    return { spans, errors: constants.errors, config: config.join("\n"), code: fullCode(state, config.join("\n")),
      work: chunk ? ["handleChunk([m1,m2,m3]) once", "commit offsets"] : stream ? ["deliver chunk", "handle m1", "handle m2", "handle m3"] : ["handle m1", "handle m2", "handle m3", "commit offsets"],
      completion: stream ? "App-owned committing: the generated example uses standard commitBatchWithin, which creates no commit spans."
        : "The original chunk offsets are committed after success, even with commit tracing disabled.",
      parentage: chunk && state.children ? `App processor spans are ${state.process ? "children of process batch" : state.receive ? "children of receive" : "roots (no current span)"}. Constant attributes apply only to library spans.`
        : "Receive and process spans are independent roots. Incoming message contexts are links, not parents." };
  }
  if (typeof module !== "undefined" && module.exports) module.exports = { describe, constantAttributes, defaults };
  if (typeof document === "undefined") return;

  function init(root) {
    const state = { ...defaults, attributes: [] };
    let selectedId = null;
    const params = new URLSearchParams(window.location.search);
    if (["chunk", "record", "stream"].includes(params.get("method"))) state.method = params.get("method");
    for (const key of ["receive", "process", "commit", "children"]) if (["on", "off"].includes(params.get(key))) state[key] = params.get(key) === "on";
    for (const key of ["topic", "clientId", "groupId"]) if (params.has(key)) state[key] = params.get(key);
    try { const rows = JSON.parse(params.get("attributes") || "[]"); if (Array.isArray(rows)) state.attributes = rows.slice(0, 10).filter(a => a && typeof a.key === "string" && typeof a.value === "string"); } catch (_) { /* Ignore malformed shared settings. */ }
    const find = (name) => root.querySelector(`[data-${name}]`);
    const controls = Array.from(root.querySelectorAll("[data-option]"));
    const text = (tag, value, className) => { const el = document.createElement(tag); el.textContent = value; if (className) el.className = className; return el; };
    function share() {
      const url = new URL(window.location.href);
      for (const key of ["method", "topic", "clientId", "groupId"]) url.searchParams.set(key, state[key]);
      for (const key of ["receive", "process", "commit", "children"]) url.searchParams.set(key, state[key] ? "on" : "off");
      if (state.attributes.length) url.searchParams.set("attributes", JSON.stringify(state.attributes)); else url.searchParams.delete("attributes");
      window.history.replaceState(null, "", url);
    }
    function attributeEditor() {
      const container = find("attribute-editor");
      container.replaceChildren();
      state.attributes.forEach((a, index) => {
        const row = document.createElement("div"); row.className = "ce-attribute-row";
        for (const key of ["key", "type", "value"]) {
          const input = document.createElement(key === "type" ? "select" : "input");
          input.setAttribute("aria-label", `Attribute ${index + 1} ${key}`);
          if (key === "type") for (const type of ["string", "long", "boolean"]) { const option = text("option", type); option.value = type; input.append(option); }
          input.value = a[key] || (key === "type" ? "string" : "");
          input.dataset.attributeIndex = index; input.dataset.attributeField = key;
          row.append(input);
        }
        const remove = text("button", "Remove"); remove.type = "button";
        remove.addEventListener("click", () => { state.attributes.splice(index, 1); attributeEditor(); render(); share(); });
        row.append(remove); container.append(row);
      });
      find("add-attribute").disabled = state.attributes.length >= 10;
    }
    function inspector(span) {
      const container = find("inspector"); container.replaceChildren();
      if (!span) { container.append(text("p", "No span to inspect. Handler and commit work still run.")); return; }
      container.append(text("h3", span.name + (span.record ? ` · ${span.record}` : "")));
      const parent = span.parent ? currentView.spans.find(s => s.id === span.parent).name : "Root (no parent)";
      container.append(text("p", `${span.kind} · ${span.scope} · ${span.start * 10}–${(span.start + span.length) * 10} ms · Parent: ${parent}`));
      container.append(text("p", `Setup: ${span.setup}`, "ce-muted"));
      container.append(text("p", span.links.length ? `Links: incoming contexts from ${span.links.join(", ")}` : "Links: none"));
      if (span.overridden.length) container.append(text("p", `Constant attributes override derived values for: ${span.overridden.join(", ")}.`, "ce-notice"));
      if (!span.attributes.length) { container.append(text("p", "No attributes. KafkaTracer constant attributes do not apply to app spans.")); return; }
      const table = document.createElement("table"); table.className = "ce-attributes";
      const head = document.createElement("thead"); const headings = document.createElement("tr");
      ["Attribute", "Value", "Source"].forEach(h => headings.append(text("th", h))); head.append(headings); table.append(head);
      const body = document.createElement("tbody");
      span.attributes.forEach(a => { const row = document.createElement("tr"); if (a.source === "withConstAttributes") row.className = "ce-constant";
        const key = document.createElement("td"); key.append(text("code", a.key));
        row.append(key, text("td", a.type === "string" ? quote(a.value) : a.value), text("td", a.source)); body.append(row); });
      table.append(body); container.append(table);
    }
    let currentView;
    function render() {
      currentView = describe(state);
      const chunk = state.method === "chunk", stream = state.method === "stream";
      find("process-label").textContent = chunk ? "Batch process span" : "Record process spans";
      find("commit-control").hidden = stream; find("children-control").hidden = !chunk;
      const work = find("work"); work.replaceChildren();
      currentView.work.forEach((label, i) => { const phase = text("div", label, "ce-phase"); if (chunk) phase.style.gridColumn = i === 0 ? "1 / span 3" : "4"; work.append(phase); });
      find("completion").textContent = currentView.completion; find("parentage").textContent = currentView.parentage;
      find("input").textContent = `${state.topic} · partition 0 · [m1 @ 1, m2 @ 2, m3 @ 3]`;
      find("errors").textContent = currentView.errors.join(" ") + (currentView.errors.length ? " Invalid rows are omitted from code and spans." : "");
      find("code").textContent = currentView.code;
      if (!currentView.spans.some(s => s.id === selectedId)) selectedId = currentView.spans[0]?.id || null;
      const spans = find("spans"); spans.replaceChildren();
      currentView.spans.forEach(span => {
        const row = document.createElement("div"); row.className = "ce-span-row"; row.dataset.kind = span.kind;
        if (span.id === selectedId) row.classList.add("ce-selected");
        const name = text("button", `${span.parent ? "  " : ""}> ${span.name}${span.record ? ` (${span.record})` : ""}`, "ce-span-name"); name.type = "button"; name.setAttribute("aria-pressed", span.id === selectedId);
        const track = document.createElement("div"); track.className = "ce-track";
        const bar = text("button", "", "ce-bar"); bar.type = "button"; bar.style.gridColumn = `${span.start + 1} / span ${span.length}`;
        bar.setAttribute("aria-label", `Inspect ${span.name}${span.record ? ` ${span.record}` : ""}, ${span.length * 10} milliseconds`);
        const select = () => { selectedId = span.id; render(); find("selection-status").textContent = `Selected ${span.name}${span.record ? ` ${span.record}` : ""}`; };
        name.addEventListener("click", select); bar.addEventListener("click", select); track.append(bar);
        const summary = text("span", `${span.length * 10} ms${span.constantCount ? ` · ${span.constantCount} const` : ""}`, "ce-duration");
        row.append(name, track, summary); spans.append(row);
      });
      if (!currentView.spans.length) spans.append(text("p", "No spans. The work above still runs."));
      inspector(currentView.spans.find(s => s.id === selectedId));
    }
    controls.forEach(input => { const key = input.dataset.option; if (input.type === "checkbox") input.checked = state[key]; else input.value = state[key]; });
    root.addEventListener("input", event => {
      const input = event.target;
      if (input.matches("[data-option]")) state[input.dataset.option] = input.type === "checkbox" ? input.checked : input.value;
      else if (input.matches("[data-attribute-field]")) state.attributes[Number(input.dataset.attributeIndex)][input.dataset.attributeField] = input.value;
      else return;
      render(); share();
    });
    find("add-attribute").addEventListener("click", () => { state.attributes.push({ key: "app.component", type: "string", value: "orders" }); attributeEditor(); render(); share(); });
    find("copy").addEventListener("click", async () => {
      try { await navigator.clipboard.writeText(currentView.code); find("copy-status").textContent = "Copied"; }
      catch (_) { find("copy-status").textContent = "Select and copy the code below."; }
    });
    attributeEditor(); render(); root.hidden = false;
  }
  const start = () => document.querySelectorAll("[data-consumer-explorer]").forEach(init);
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start, { once: true }); else start();
})();

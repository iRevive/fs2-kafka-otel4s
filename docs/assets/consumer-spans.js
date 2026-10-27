/* Copyright 2026 Typelevel. Licensed under the Apache License, Version 2.0. */
(function () {
  "use strict";
  const defaults = { method: "record", relationship: "Link", count: "3", receive: true, process: true, commit: true, children: false, incoming: true, topic: "orders", clientId: "orders-consumer", groupId: "orders-group", attributes: [] };
  const { quote, constantAttributes, mount } = typeof module !== "undefined" && module.exports
    ? require("./tracing-explorer.js") : window.TracingExplorer;

  function fullCode(state, config) {
    const stream = state.method === "stream";
    const chunk = state.method === "chunk";
    const children = state.children;
    const body = stream ? [
      "    .flatMap { consumer =>",
      "      consumer.recordsTraced { committable =>",
      children
        ? '        appTracer.span("handle order").surround(handle(committable.record)).as(committable.offset)'
        : "        handle(committable.record).as(committable.offset)",
      "      }",
      state.commit
        ? `        .through(consumer.spans.commitBatchWithin(${state.count}, 15.seconds))`
        : `        .through(fs2.kafka.commitBatchWithin[IO](${state.count}, 15.seconds))`,
      "    }",
      "    .compile.drain",
    ] : chunk ? [
      "    .consumeChunkTraced { chunk =>",
      "      chunk.traverse_ { record =>",
      children ? '        appTracer.span("handle order").surround(handle(record))' : "        handle(record)",
      "      }.as(CommitNow)",
      "    }",
    ] : children ? [
      "    .consumeRecordTraced { record =>",
      '      appTracer.span("handle order").surround(handle(record))',
      "    }",
    ] : ["    .consumeRecordTraced(handle)"];
    const program = [
      "  KafkaConsumer", "    .stream[IO, String, String](settings)",
      `    .subscribeTo(${quote(state.topic)})`, "    .traced(config)", ...body,
    ];
    return [
      "import cats.effect.IO",
      ...(chunk ? ["import cats.syntax.all._"] : []),
      "import fs2.kafka.{ConsumerRecord, ConsumerSettings, Deserializer, KafkaConsumer}",
      "import fs2.kafka.KafkaConsumer._",
      ...(chunk ? ["import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow"] : []),
      config.includes(".withRelationship")
        ? "import fs2.kafka.otel4s.trace.{KafkaTracer, ProcessSpanRelationship}"
        : "import fs2.kafka.otel4s.trace.KafkaTracer",
      "import fs2.kafka.otel4s.trace.syntax._",
      ...(config.includes(".withConstAttributes") ? ["import org.typelevel.otel4s.{Attribute, Attributes}"] : []),
      "import org.typelevel.otel4s.trace.TracerProvider",
      ...(stream ? ["import scala.concurrent.duration._"] : []), "",
      "// Supply the TracerProvider from your configured otel4s backend.",
      "// Incoming headers are supplied by the producer; this consumer does not create them.",
      "def run(implicit provider: TracerProvider[IO]): IO[Unit] = {",
      "  val settings = ConsumerSettings[IO, String, String](",
      "    Deserializer[IO, String], Deserializer[IO, String]",
      '  ).withBootstrapServers("localhost:9092")',
      `    .withClientId(${quote(state.clientId)})`, `    .withGroupId(${quote(state.groupId)})`, "",
      "  val config = " + config.split("\n").join("\n  "), "",
      "  def handle(record: ConsumerRecord[String, String]): IO[Unit] =",
      "    IO.println(record.value)", "",
      ...(children ? [
        '  provider.tracer("orders-app").get.flatMap { appTracer =>',
        ...program.map(line => "  " + line), "  }",
      ] : program), "}",
    ].join("\n");
  }

  function describe(input) {
    const state = { ...defaults, ...input };
    const chunk = state.method === "chunk";
    const stream = state.method === "stream";
    const count = state.count === "1" ? 1 : 3;
    const messages = Array.from({ length: count }, (_, i) => `m${i + 1}`);
    const parentFromRecord = state.relationship === "ParentFromRecord" && state.incoming && (!chunk || count === 1);
    const constants = constantAttributes(state.attributes);
    const spans = [];
    function add(id, operation, start, length, setup, recordIndex, parent) {
      const app = operation === "app handle";
      const derived = Object.create(null);
      function attr(key, value, type) { derived[key] = { key, value: String(value), type: type || "string", source: "Record / client metadata" }; }
      if (!app) {
        attr("messaging.system", "kafka");
        attr("messaging.operation.name", operation === "receive" ? "poll" : operation.startsWith("process") ? "process" : "commit");
        attr("messaging.operation.type", operation === "receive" ? "receive" : operation.startsWith("process") ? "process" : "settle");
        attr("messaging.destination.name", state.topic);
        attr("messaging.destination.partition.id", "0");
        attr("messaging.client.id", state.clientId);
        attr("messaging.consumer.group.name", state.groupId);
        const attributeIndex = recordIndex === undefined && count === 1 && !(stream && operation === "commit") ? 0 : recordIndex;
        if (attributeIndex !== undefined) {
          attr("messaging.kafka.message.key", `m${attributeIndex + 1}`);
          attr("messaging.kafka.offset", attributeIndex + 1, "long");
          if (operation === "process batch") attr("messaging.batch.message_count", count, "long");
        } else if (!(stream && operation === "commit")) attr("messaging.batch.message_count", count, "long");
      }
      const overridden = [];
      if (!app) constants.attributes.forEach((a) => {
        if (derived[a.key]) overridden.push(a.key);
        derived[a.key] = a;
      });
      const process = operation.startsWith("process");
      const links = !state.incoming || app || operation === "commit" ? [] : recordIndex === undefined ? [...messages] : [messages[recordIndex]];
      if (process && parentFromRecord) {
        parent = `incoming-${messages[recordIndex === undefined ? 0 : recordIndex]}`;
        if (state.receive && !stream) links.push({ label: "receive", target: "span-receive", attributes: [] });
      }
      spans.push({ id, name: app ? "handle order" : `${operation === "receive" ? "poll" : operation.startsWith("process") ? "process" : "commit"} ${state.topic}`,
        record: recordIndex === undefined ? null : `m${recordIndex + 1}`, start, length, setup, parent: parent || null,
        kind: app ? "INTERNAL" : operation.startsWith("process") ? "CONSUMER" : "CLIENT", scope: app ? "orders-app" : "fs2.kafka",
        attributes: Object.values(derived), overridden, constantCount: app ? 0 : new Set(constants.attributes.map(a => a.key)).size,
        links });
    }
    if (state.receive) add("receive", "receive", 0, stream ? 1 : count, "withReceiveSpanSetup");
    if (chunk && state.process) add("batch", "process batch", 0, count, "withBatchProcessSpanSetup");
    for (let i = 0; i < count; i++) {
      const start = i + (stream ? 1 : 0);
      if (!chunk && state.process) add(`process-${i}`, "process", start, 1, "withProcessSpanSetup", i);
      if (state.children) {
        const parent = state.process ? chunk ? "batch" : `process-${i}` : state.receive && !stream ? "receive" : null;
        add(`app-${i}`, "app handle", start, 1, 'Tracer[IO].span("handle order")', i, parent);
      }
    }
    if (state.commit) add("commit", "commit", stream ? count + 1 : count, 1, "withCommitSpanSetup");
    const config = ["KafkaTracer.Config", "  .default"];
    if (state.relationship === "ParentFromRecord" && state.process) {
      const hook = chunk ? "batchProcessSpanSetup" : "processSpanSetup";
      config.push(`  .${chunk ? "withBatchProcessSpanSetup" : "withProcessSpanSetup"} { ctx =>`,
        `    KafkaTracer.Config.Defaults.${hook}(ctx)`,
        "      .map(_.withRelationship(ProcessSpanRelationship.ParentFromRecord))", "  }");
    }
    if (!state.receive) config.push("  .withoutReceiveSpans");
    if (!state.process) config.push(`  .${chunk ? "withBatchProcessSpanSetup" : "withProcessSpanSetup"}(_ => None)`);
    if (!state.commit) config.push("  .withoutCommitSpans");
    if (constants.attributes.length) {
      config.push("  .withConstAttributes(Attributes(");
      constants.attributes.forEach((a, i) => config.push(`    Attribute(${quote(a.key)}, ${a.literal})${i < constants.attributes.length - 1 ? "," : ""}`));
      config.push("  ))");
    }
    if (config.length === 2) config.splice(0, 2, "KafkaTracer.Config.default");
    return {
      input: `${state.topic} · partition 0 · [${messages.map((m, i) => `${m} @ ${i + 1}`).join(", ")}]`,
      workColumns: chunk ? [`1 / span ${count}`, `${count + 1}`] : null,
      contexts: state.incoming ? messages.map((message, index) => ({ id: `incoming-${message}`, title: `${message} · incoming message context`,
        description: `Context from ${state.topic}, partition 0, offset ${index + 1}. ${state.process && parentFromRecord ? "Process uses this context as parent and retains its message link." : "Consumer spans link to this context."}` })) : [],
      spans, errors: constants.errors, config: config.join("\n"), code: fullCode(state, config.join("\n")),
      columns: count + (stream ? 2 : 1),
      work: chunk ? ["handle chunk once", "commit offsets"] : [...(stream ? ["deliver chunk"] : []), ...messages.map(m => `handle ${m}`), "commit offsets"],
      completion: stream ? (state.commit
        ? `Commit after ${count} offset${count === 1 ? "" : "s"}. Each commit call gets a span, without a record count.`
        : "The standard commitBatchWithin pipe commits offsets without spans.")
        : "Commit the original chunk after success. Disabling tracing keeps commits enabled.",
      parentage: (state.process && parentFromRecord
        ? `Process spans use message context as parent and retain message links.${state.receive && !stream ? " The enclosing receive span is linked as ambient context." : ""} Receive and commit remain roots. `
        : state.process && state.incoming && state.relationship === "ParentFromRecord" && chunk && count > 1
          ? "Multiple-record batch processing remains a root linked to all message contexts. "
          : "Receive, process, and commit are separate roots. Each starts a trace. ") + (state.children ? `Application spans are ${state.process ? chunk ? "children of process batch" : "children of their record process span" : state.receive && !stream ? "children of receive" : "roots (no current span)"}. Constant attributes apply only to library spans.`
        : ""),
      incoming: state.incoming
        ? "Receive and batch process link to every message. Record process links to its own message."
        : "No incoming contexts. Receive and process start new traces without message links." };
  }
  if (typeof module !== "undefined" && module.exports) module.exports = { describe, constantAttributes, defaults };
  if (typeof document === "undefined") return;

  const adapter = { defaults, describe,
    choices: { method: ["chunk", "record", "stream"], relationship: ["Link", "ParentFromRecord"], count: ["1", "3"] },
    booleans: ["receive", "process", "commit", "children", "incoming"], texts: ["topic", "clientId", "groupId"],
    renderExtras({ find, state }) {
      find("process-label").textContent = state.method === "chunk" ? "Batch process span" : "Record process spans";
    }
  };
  const start = () => document.querySelectorAll("[data-consumer-explorer]").forEach(root => mount(root, adapter));
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start, { once: true }); else start();
})();

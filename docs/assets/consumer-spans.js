/* Copyright 2026 Typelevel. Licensed under the Apache License, Version 2.0. */
(function () {
  "use strict";
  const defaults = { method: "record", receive: true, process: true, commit: true, children: false, incoming: true, topic: "orders", clientId: "orders-consumer", groupId: "orders-group", attributes: [] };
  const { quote, constantAttributes, mount } = typeof module !== "undefined" && module.exports
    ? require("./tracing-explorer.js") : window.TracingExplorer;

  function fullCode(state, config) {
    const stream = state.method === "stream";
    const chunk = state.method === "chunk";
    const children = chunk && state.children;
    const body = stream ? [
      "    .flatMap { consumer =>",
      "      consumer.recordsTraced { committable =>",
      "        handle(committable.record).as(committable.offset)",
      "      }",
      state.commit
        ? "        .through(consumer.spans.commitBatchWithin(3, 15.seconds))"
        : "        .through(fs2.kafka.commitBatchWithin[IO](3, 15.seconds))",
      "    }",
      "    .compile.drain",
    ] : chunk ? [
      "    .consumeChunkTraced { chunk =>",
      "      chunk.traverse_ { record =>",
      children ? '        appTracer.span("process").surround(handle(record))' : "        handle(record)",
      "      }.as(CommitNow)",
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
      "import fs2.kafka.otel4s.trace.KafkaTracer",
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
    const constants = constantAttributes(state.attributes);
    const spans = [];
    function add(id, operation, start, length, setup, recordIndex, parent) {
      const app = operation === "app process";
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
        if (recordIndex !== undefined) {
          attr("messaging.kafka.message.key", `m${recordIndex + 1}`);
          attr("messaging.kafka.offset", recordIndex + 1, "long");
        } else if (!(stream && operation === "commit")) attr("messaging.batch.message_count", 3, "long");
      }
      const overridden = [];
      if (!app) constants.attributes.forEach((a) => {
        if (derived[a.key]) overridden.push(a.key);
        derived[a.key] = a;
      });
      spans.push({ id, name: app ? "process" : `${operation === "receive" ? "poll" : operation.startsWith("process") ? "process" : "commit"} ${state.topic}`,
        record: recordIndex === undefined ? null : `m${recordIndex + 1}`, start, length, setup, parent: parent || null,
        kind: app ? "INTERNAL" : operation.startsWith("process") ? "CONSUMER" : "CLIENT", scope: app ? "orders-app" : "fs2.kafka",
        attributes: Object.values(derived), overridden, constantCount: app ? 0 : new Set(constants.attributes.map(a => a.key)).size,
        links: !state.incoming || app || operation === "commit" ? [] : recordIndex === undefined ? ["m1", "m2", "m3"] : [`m${recordIndex + 1}`] });
    }
    if (state.receive) add("receive", "receive", 0, stream ? 1 : 3, "withReceiveSpanSetup");
    if (state.process) {
      if (chunk) add("batch", "process batch", 0, 3, "withBatchProcessSpanSetup");
      else for (let i = 0; i < 3; i++) add(`process-${i}`, "process", i + (stream ? 1 : 0), 1, "withProcessSpanSetup", i);
    }
    if (chunk && state.children) {
      const parent = state.process ? "batch" : state.receive ? "receive" : null;
      for (let i = 0; i < 3; i++) add(`app-${i}`, "app process", i, 1, 'Tracer[IO].span("process")', i, parent);
    }
    if (state.commit) add("commit", "commit", stream ? 4 : 3, 1, "withCommitSpanSetup");
    const config = ["KafkaTracer.Config", "  .default"];
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
      input: `${state.topic} · partition 0 · [m1 @ 1, m2 @ 2, m3 @ 3]`,
      workColumns: chunk ? ["1 / span 3", "4"] : null,
      contexts: state.incoming ? [1, 2, 3].map(i => ({ id: `incoming-m${i}`, title: `m${i} · incoming message context`,
        description: `Context from ${state.topic}, partition 0, offset ${i}. Consumer spans link to this context.` })) : [],
      spans, errors: constants.errors, config: config.join("\n"), code: fullCode(state, config.join("\n")),
      columns: stream ? 5 : 4,
      work: chunk ? ["handle chunk once", "commit offsets"] : stream ? ["deliver chunk", "handle m1", "handle m2", "handle m3", "commit offsets"] : ["handle m1", "handle m2", "handle m3", "commit offsets"],
      completion: stream ? (state.commit
        ? "Commit after three offsets. Each commit call gets a span, without a record count."
        : "The standard commitBatchWithin pipe commits offsets without spans.")
        : "Commit the original chunk after success. Disabling tracing keeps commits enabled.",
      parentage: chunk && state.children ? `App process spans are ${state.process ? "children of process batch" : state.receive ? "children of receive" : "roots (no current span)"}. Constant attributes apply only to library spans.`
        : "Receive, process, and commit are separate roots. Each starts a trace.",
      incoming: state.incoming
        ? "Receive and batch process link to all three messages. Record process links to its own message."
        : "No incoming contexts. Receive and process start new traces without message links." };
  }
  if (typeof module !== "undefined" && module.exports) module.exports = { describe, constantAttributes, defaults };
  if (typeof document === "undefined") return;

  const adapter = { defaults, describe,
    choices: { method: ["chunk", "record", "stream"] },
    booleans: ["receive", "process", "commit", "children", "incoming"], texts: ["topic", "clientId", "groupId"],
    renderExtras({ find, state }) {
      find("process-label").textContent = state.method === "chunk" ? "Batch process span" : "Record process spans";
      find("children-control").hidden = state.method !== "chunk";
    }
  };
  const start = () => document.querySelectorAll("[data-consumer-explorer]").forEach(root => mount(root, adapter));
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start, { once: true }); else start();
})();

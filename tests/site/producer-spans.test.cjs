/* Copyright 2026 Typelevel. Licensed under the Apache License, Version 2.0. */
const test = require("node:test");
const assert = require("node:assert/strict");
const { describe } = require("../../docs/assets/producer-spans.js");
const attribute = (span, key) => span.attributes.find(a => a.key === key);
const library = view => view.spans.filter(s => s.scope === "fs2.kafka");

// Expected cases from the producer instrumentation matrix, independent of rendering.
for (const [count, mode, incoming, createCount, kind, linkCount] of [
  [0, "PerRecordSpans", "none", 0, null, 0],
  [1, "PerRecordSpans", "none", 0, "PRODUCER", 0],
  [1, "SharedSendSpan", "all", 0, "CLIENT", 1],
  [3, "PerRecordSpans", "none", 3, "CLIENT", 3],
  [3, "PerRecordSpans", "some", 2, "CLIENT", 3],
  [3, "PerRecordSpans", "all", 0, "CLIENT", 3],
  [3, "SharedSendSpan", "none", 0, "PRODUCER", 0],
  [3, "SharedSendSpan", "some", 0, "PRODUCER", 1],
  [3, "SharedSendSpan", "all", 0, "CLIENT", 3],
]) {
  test(`matrix: ${count} records, ${mode}, ${incoming} incoming`, () => {
    const view = describe({ count: String(count), mode, incoming });
    assert.equal(view.spans.filter(s => s.id.startsWith("create-")).length, createCount);
    const send = view.spans.find(s => s.id === "send");
    assert.equal(send?.kind || null, kind);
    assert.equal(send?.links.length || 0, linkCount);
    assert(library(view).every(s => s.parent === null));
  });
}

test("all 288 combinations have valid lifetimes, parents, links and outgoing contexts", () => {
  for (const count of ["0", "1", "3"]) for (const mode of ["PerRecordSpans", "SharedSendSpan"])
    for (const incoming of ["none", "some", "all"]) for (let bits = 0; bits < 16; bits++) {
      const state = { count, mode, incoming };
      ["send", "ambient", "partition", "tombstone"].forEach((key, i) => state[key] = Boolean(bits & (1 << i)));
      const view = describe(state);
      assert.equal(view.outgoing.length, Number(count));
      assert(view.work.includes("submit records") && view.work.includes("await Kafka ack"));
      assert.match(view.code, /\.produce\(records\)\.flatten/);
      assert.equal(Boolean(view.spans.find(s => s.id === "send")), state.send && count !== "0");
      for (const span of view.spans) {
        assert(span.start >= 0 && span.start + span.length <= view.columns);
        assert.equal(span.parent, state.ambient && span.id !== "app" ? "app" : null);
        for (const link of span.links) {
          assert(link.target.startsWith("span-")
            ? view.spans.some(s => `span-${s.id}` === link.target)
            : view.contexts.some(c => c.id === link.target));
        }
      }
      for (const record of view.outgoing) {
        if (record.target) assert(view.contexts.some(c => c.id === record.target) || view.spans.some(s => `span-${s.id}` === record.target));
        else assert.equal(record.context, "none");
      }
    }
});

test("send suppression retains create spans and uses ambient context only when needed", () => {
  const perRecord = describe({ send: false });
  assert.equal(library(perRecord).length, 3);
  assert.deepEqual(perRecord.outgoing.map(r => r.context), ["create m1", "create m2", "create m3"]);
  const shared = describe({ mode: "SharedSendSpan", incoming: "some", send: false });
  assert.equal(library(shared).length, 0);
  assert.deepEqual(shared.outgoing.map(r => r.context), ["incoming m1", "none", "none"]);
  const ambient = describe({ mode: "SharedSendSpan", incoming: "some", send: false, ambient: true });
  assert.deepEqual(ambient.outgoing.map(r => r.context), ["incoming m1", "application", "application"]);
  assert.match(ambient.code, /\.withoutSendSpans/);
  assert.match(ambient.code, /appTracer\.span\("publish orders"\)\.surround\(publish\)/);
});

test("batch attributes, link attributes, and final single-record results remain distinct", () => {
  const batch = describe({ incoming: "some", partition: true, tombstone: true });
  const send = batch.spans.find(s => s.id === "send");
  assert.equal(attribute(send, "messaging.batch.message_count").value, "3");
  assert.equal(attribute(send, "messaging.kafka.message.key"), undefined);
  assert.equal(attribute(send, "messaging.kafka.offset"), undefined);
  assert.equal(attribute(send, "messaging.kafka.message.tombstone").value, "true");
  assert.equal(send.links[0].attributes.find(a => a.key === "messaging.kafka.message.key").value, "m1");
  for (const link of send.links) {
    assert(!link.attributes.some(a => ["messaging.client.id", "messaging.kafka.offset", "messaging.batch.message_count"].includes(a.key)));
  }
  const unpartitioned = describe({}).spans.find(s => s.id === "send");
  assert.equal(attribute(unpartitioned, "messaging.destination.partition.id"), undefined);
  const single = describe({ count: "1" }).spans.find(s => s.id === "send");
  assert.equal(attribute(single, "messaging.batch.message_count"), undefined);
  assert.equal(attribute(single, "messaging.destination.partition.id").value, "0");
  assert.equal(attribute(single, "messaging.kafka.offset").value, "42");
  assert.match(attribute(single, "messaging.kafka.offset").source, /acknowledgment/);
});

test("constants apply to library spans; acknowledgment attributes take final precedence", () => {
  const attributes = [
    { key: "messaging.destination.name", type: "string", value: "override" },
    { key: "messaging.kafka.offset", type: "long", value: "99" },
    { key: "messaging.destination.partition.id", type: "string", value: "8" },
    { key: "__proto__", type: "boolean", value: "true" },
  ];
  const batch = describe({ ambient: true, attributes });
  assert.deepEqual(batch.spans.find(s => s.id === "app").attributes, []);
  for (const span of library(batch)) {
    assert.equal(attribute(span, "messaging.destination.name").value, "override");
    assert.equal(attribute(span, "__proto__").value, "true");
    assert(span.name.endsWith(" orders"));
    assert(span.links.every(l => l.attributes.find(a => a.key === "messaging.destination.name").value === "orders"));
  }
  const single = describe({ count: "1", attributes }).spans.find(s => s.id === "send");
  assert.equal(attribute(single, "messaging.kafka.offset").value, "42");
  assert.equal(attribute(single, "messaging.destination.partition.id").value, "0");
});

test("copied Scala reflects incoming headers and safely quotes user input", () => {
  const value = 'orders"\\\n${doNotInterpolate}\u2028';
  const view = describe({ topic: value, clientId: value, incoming: "some", attributes: [
    { key: "app.note", type: "string", value }, { key: "app.max", type: "long", value: "9223372036854775807" }
  ] });
  assert(view.code.includes(JSON.stringify(value).replace(/\u2028/g, "\\u2028")));
  assert.match(view.code, /9223372036854775807L/);
  assert.equal((view.code.match(/\.withHeaders\(/g) || []).length, 1);
  assert.equal((describe({ incoming: "all" }).code.match(/\.withHeaders\(/g) || []).length, 3);
  assert(!describe({ incoming: "none" }).code.includes("traceparent"));
  const invalid = describe({ attributes: [{ key: "bad", type: "long", value: "overflow" }] });
  assert.equal(invalid.errors.length, 1);
  assert(!invalid.code.includes("withConstAttributes"));
});

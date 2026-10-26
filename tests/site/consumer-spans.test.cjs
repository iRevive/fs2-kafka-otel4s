/* Copyright 2026 Typelevel. Licensed under the Apache License, Version 2.0. */
const test = require("node:test");
const assert = require("node:assert/strict");
const { describe, constantAttributes } = require("../../docs/assets/consumer-spans.js");
const attribute = (span, key) => span.attributes.find(a => a.key === key);

for (const method of ["record", "chunk", "stream"]) {
  test(`${method}: all toggle combinations preserve execution and valid span relationships`, () => {
    for (let bits = 0; bits < 32; bits++) {
      const state = { method };
      ["receive", "process", "commit", "children", "incoming"].forEach((key, i) => state[key] = Boolean(bits & (1 << i)));
      const view = describe(state);
      const library = view.spans.filter(s => s.scope === "fs2.kafka");
      assert(library.every(s => s.parent === null));
      assert.equal(library.length, Number(state.receive) + (state.process ? method === "chunk" ? 1 : 3 : 0) + Number(state.commit));
      assert(view.work.includes("commit offsets"), "disabling commit tracing must not remove committing");
      for (const span of view.spans) {
        assert(span.start >= 0 && span.start + span.length <= view.columns);
        if (span.parent) {
          const parent = view.spans.find(s => s.id === span.parent);
          assert(parent, "a child must reference an existing parent");
          assert(span.start >= parent.start && span.start + span.length <= parent.start + parent.length);
        }
        if (!state.incoming) assert.deepEqual(span.links, []);
        if (span.scope === "orders-app") {
          assert.equal(span.parent, state.process ? "batch" : state.receive ? "receive" : null);
          assert.deepEqual(span.links, []);
        }
      }
      assert.match(view.code, /import fs2\.kafka\.otel4s\.trace\.syntax\._/);
      assert.match(view.code, /\.traced\(config\)/);
      assert.match(view.code, new RegExp(`\\.${method === "chunk" ? "consumeChunkTraced" : method === "record" ? "consumeRecordTraced" : "recordsTraced"}`));
    }
  });
}

test("incoming contexts are links on the right operations, never parents", () => {
  const record = describe({ method: "record" });
  assert.deepEqual(record.spans.find(s => s.id === "receive").links, ["m1", "m2", "m3"]);
  assert.deepEqual(record.spans.find(s => s.id === "process-1").links, ["m2"]);
  assert.deepEqual(record.spans.find(s => s.id === "commit").links, []);
  const chunk = describe({ method: "chunk", children: true });
  assert.deepEqual(chunk.spans.find(s => s.id === "batch").links, ["m1", "m2", "m3"]);
  assert.equal(describe({ incoming: false }).code, describe({ incoming: true }).code);
});

test("stream commits use offset-only attributes and the selected commit pipe", () => {
  const traced = describe({ method: "stream" });
  const commit = traced.spans.find(s => s.id === "commit");
  for (const key of ["messaging.batch.message_count", "messaging.kafka.offset", "messaging.kafka.message.key", "messaging.kafka.message.tombstone"]) {
    assert.equal(attribute(commit, key), undefined);
  }
  assert.equal(attribute(commit, "messaging.destination.partition.id").value, "0");
  assert.match(traced.code, /consumer\.spans\.commitBatchWithin\(3, 15.seconds\)/);
  const untraced = describe({ method: "stream", commit: false });
  assert(!untraced.spans.some(s => s.id === "commit"));
  assert.match(untraced.code, /fs2\.kafka\.commitBatchWithin\[IO\]\(3, 15.seconds\)/);
  assert.equal(attribute(describe({ method: "chunk" }).spans.find(s => s.id === "commit"), "messaging.batch.message_count").value, "3");
});

test("constant attributes override derived values without changing span names or application spans", () => {
  const view = describe({ method: "chunk", children: true, attributes: [
    { key: "messaging.destination.name", type: "string", value: "override" },
    { key: "__proto__", type: "string", value: "safe attribute key" },
  ] });
  for (const span of view.spans) {
    if (span.scope === "orders-app") assert.deepEqual(span.attributes, []);
    else {
      assert.equal(attribute(span, "messaging.destination.name").value, "override");
      assert.equal(attribute(span, "__proto__").value, "safe attribute key");
      assert(span.name.endsWith(" orders"));
      assert(span.overridden.includes("messaging.destination.name"));
    }
  }
});

test("attribute literals retain signed 64-bit precision and reject invalid inputs", () => {
  const limits = constantAttributes([
    { key: "min", type: "long", value: "-9223372036854775808" },
    { key: "max", type: "long", value: "9223372036854775807" },
    { key: "enabled", type: "boolean", value: "false" },
  ]);
  assert.deepEqual(limits.errors, []);
  assert.equal(limits.attributes[0].literal, "-9223372036854775808L");
  assert.equal(limits.attributes[1].literal, "9223372036854775807L");
  assert.equal(limits.attributes[2].literal, "false");
  const invalid = constantAttributes([
    { key: "overflow", type: "long", value: "9223372036854775808" },
    { key: "wrong", type: "boolean", value: "yes" },
    { key: "", type: "string", value: "missing key" },
  ]);
  assert.equal(invalid.errors.length, 3);
  assert.deepEqual(invalid.attributes, []);
});

test("copied Scala safely quotes user-supplied settings and attributes", () => {
  const value = 'orders"\\\n${doNotInterpolate}\u2028';
  const view = describe({ topic: value, attributes: [{ key: "app.note", type: "string", value }] });
  assert(view.code.includes(JSON.stringify(value).replace(/\u2028/g, "\\u2028")));
  assert(!view.code.includes('s"orders'));
  assert.equal(view.errors.length, 0);
});

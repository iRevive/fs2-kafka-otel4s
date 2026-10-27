/* Copyright 2026 Typelevel. Licensed under the Apache License, Version 2.0. */
(function () {
  "use strict";
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

  function mount(root, adapter) {
    const { defaults, describe } = adapter;
    const state = { ...defaults, attributes: [] };
    const expandedIds = new Set();
    const scope = root.closest("main") || document;
    const panels = Array.from(scope.querySelectorAll("[data-explorer-panel]"));
    const params = new URLSearchParams(window.location.search);
    for (const [key, choices] of Object.entries(adapter.choices)) if (choices.includes(params.get(key))) state[key] = params.get(key);
    for (const key of adapter.booleans) if (["on", "off"].includes(params.get(key))) state[key] = params.get(key) === "on";
    for (const key of adapter.texts) if (params.has(key)) state[key] = params.get(key);
    try { const rows = JSON.parse(params.get("attributes") || "[]"); if (Array.isArray(rows)) state.attributes = rows.slice(0, 10).filter(a => a && typeof a.key === "string" && typeof a.value === "string"); } catch (_) { /* Ignore malformed shared settings. */ }
    const find = (name) => scope.querySelector(`[data-${name}]`);
    const controls = Array.from(root.querySelectorAll("[data-option]"));
    const text = (tag, value, className) => { const el = document.createElement(tag); el.textContent = value; if (className) el.className = className; return el; };
    function share() {
      const url = new URL(window.location.href);
      for (const key of [...Object.keys(adapter.choices), ...adapter.texts]) url.searchParams.set(key, state[key]);
      for (const key of adapter.booleans) url.searchParams.set(key, state[key] ? "on" : "off");
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
    function spanDetails(container, span) {
      const parentSpan = currentView.spans.find(s => s.id === span.parent);
      const parentContext = currentView.contexts.find(c => c.id === span.parent);
      const metadata = text("p", `${span.kind} · ${span.scope} · ${span.start * 10}–${(span.start + span.length) * 10} ms · Parent: `);
      if (parentSpan || parentContext) {
        const parent = text("a", parentSpan ? parentSpan.name : parentContext.title);
        parent.href = parentSpan ? `#span-${parentSpan.id}` : `#${parentContext.id}`;
        metadata.append(parent);
      } else metadata.append(text("span", "Root (no parent)"));
      container.append(metadata);
      container.append(text("p", `Setup: ${span.setup}`, "ce-muted"));
      if (span.overridden.length) container.append(text("p", `Constant attributes replace: ${span.overridden.join(", ")}.`, "ce-notice"));
      const detailedLinks = span.links.filter(link => typeof link === "object" && link.attributes.length);
      if (detailedLinks.length) {
        const heading = text("p", "Span links (each describes one record)", "ce-notice"); container.append(heading);
        detailedLinks.forEach(link => {
          const detail = document.createElement("details");
          detail.append(text("summary", link.label));
          const list = document.createElement("ul");
          link.attributes.forEach(a => list.append(text("li", `${a.key}: ${quote(a.value)}`)));
          detail.append(list); container.append(detail);
        });
      }
      if (!span.attributes.length) { container.append(text("p", "No attributes. Constant attributes apply to library spans only.")); return; }
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
      if (adapter.renderExtras) adapter.renderExtras({ find, state, view: currentView, text });
      const work = find("work"); work.replaceChildren();
      panels.forEach(panel => panel.style.setProperty("--ce-columns", currentView.columns));
      const axis = find("axis"); axis.replaceChildren();
      for (let i = 0; i <= currentView.columns; i++) axis.append(text("span", `${i * 10}${i === currentView.columns ? " ms" : ""}`));
      find("incoming").textContent = currentView.incoming;
      currentView.work.forEach((label, i) => { const phase = text("div", label, "ce-phase"); if (currentView.workColumns) phase.style.gridColumn = currentView.workColumns[i]; work.append(phase); });
      find("completion").textContent = currentView.completion; find("parentage").textContent = currentView.parentage;
      find("input").textContent = currentView.input;
      find("errors").textContent = currentView.errors.join(" ") + (currentView.errors.length ? " Invalid attributes are skipped." : "");
      find("code").textContent = currentView.code;
      find("copy-status").textContent = "";
      const spans = find("spans"); spans.replaceChildren();
      currentView.spans.forEach(span => {
        const item = document.createElement("details");
        item.className = "ce-span";
        item.dataset.kind = span.kind;
        item.dataset.spanId = span.id;
        item.id = `span-${span.id}`;
        item.open = expandedIds.has(span.id);
        if (span.parent && currentView.spans.some(s => s.id === span.parent)) item.classList.add("ce-child");

        const summary = document.createElement("summary");
        summary.className = "ce-span-row";
        const name = text("span", `${span.name}${span.record ? ` (${span.record})` : ""}`, "ce-span-name");
        const track = document.createElement("span"); track.className = "ce-track";
        const bar = text("span", "", "ce-bar");
        bar.style.gridColumn = `${span.start + 1} / span ${span.length}`;
        track.append(bar);
        const duration = text("span", `${span.length * 10} ms`, "ce-duration");
        const links = document.createElement("span"); links.className = "ce-links";
        if (span.links.length) {
          span.links.forEach(message => {
            const label = typeof message === "string" ? message : message.label;
            const target = typeof message === "string" ? `incoming-${message}` : message.target;
            const link = text("a", label, "ce-context-link");
            link.href = `#${target}`;
            link.setAttribute("aria-label", `${label}; span link`);
            links.append(link);
          });
        } else links.append(text("span", "None", "ce-muted"));
        summary.append(name, track, duration, links);
        const body = document.createElement("div"); body.className = "ce-span-details";
        spanDetails(body, span);
        item.append(summary, body);
        item.addEventListener("toggle", () => {
          // Ignore queued events from a previous render after a control changed.
          if (!item.isConnected) return;
          if (item.open) expandedIds.add(span.id); else expandedIds.delete(span.id);
        });
        spans.append(item);
      });
      if (!currentView.spans.length) spans.append(text("p", currentView.noSpans || "No spans. Processing and commits still run."));

      const contexts = find("contexts"); contexts.replaceChildren();
      currentView.contexts.forEach(context => {
        const card = document.createElement("article");
        card.className = "ce-context";
        card.id = context.id;
        card.tabIndex = -1;
        card.append(text("h4", context.title), text("p", context.description));
        contexts.append(card);
      });
      if (!currentView.contexts.length) contexts.append(text("p", "No recognized incoming trace contexts in this example."));
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
    attributeEditor(); render(); panels.forEach(panel => { panel.hidden = false; });
  }
  const api = { quote, constantAttributes, mount };
  if (typeof module !== "undefined" && module.exports) module.exports = api;
  else window.TracingExplorer = api;
})();

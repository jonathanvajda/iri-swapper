/* sparql.js - SPARQL query IRI mapper (runs in parallel to your ontology tool; no edits to existing JS) */

const DB = {
  name: "myna-sparql-mapper-db",
  version: 1,
  storeRuns: "runs",
};

const UI = {
  queryFile: document.getElementById("queryFile"),
  mappingFile: document.getElementById("mappingFile"),
  queryDrop: document.getElementById("queryDrop"),
  mappingDrop: document.getElementById("mappingDrop"),

  ingestQueryBtn: document.getElementById("ingestQueryBtn"),
  ingestMappingBtn: document.getElementById("ingestMappingBtn"),
  buildPreviewBtn: document.getElementById("buildPreviewBtn"),
  applyMappingBtn: document.getElementById("applyMappingBtn"),

  useNativePrefixes: document.getElementById("useNativePrefixes"),

  queryRunId: document.getElementById("queryRunId"),
  baseIri: document.getElementById("baseIri"),
  prefixJson: document.getElementById("prefixJson"),

  mappingRows: document.getElementById("mappingRows"),
  mappingUniqueOld: document.getElementById("mappingUniqueOld"),
  mappingDupOld: document.getElementById("mappingDupOld"),

  kpiTokens: document.getElementById("kpiTokens"),
  kpiMapping: document.getElementById("kpiMapping"),
  kpiProposed: document.getElementById("kpiProposed"),
  kpiPct: document.getElementById("kpiPct"),

  tableEl: document.getElementById("table"),
  outputPreview: document.getElementById("outputPreview"),
  status: document.getElementById("status"),

  runsSelect: document.getElementById("runsSelect"),
  loadRunBtn: document.getElementById("loadRunBtn"),
  deleteRunBtn: document.getElementById("deleteRunBtn"),
  clearRunsBtn: document.getElementById("clearRunsBtn"),

  downloadBtn: document.getElementById("downloadBtn"),

  toggleThemeBtn: document.getElementById("mbToggleThemeBtn"),
};

const Session = {
  currentInputRunId: null,
  currentOutputRunId: null,
  mapping: new Map(), // old -> new
  mappingMeta: { rows: 0, uniqueOld: 0, dupOld: 0 },
};

let table = null;

init().catch(err => setStatus(`Init error: ${err?.message || err}`, true));

async function init() {
  wireDropzones();
  wireButtons();
  initTable();
  await refreshRunsDropdown();
  setStatus("Ready.");
}

function wireButtons() {
  UI.toggleThemeBtn.addEventListener("click", () => {
    document.getElementById("mb-app").classList.toggle("mb-light");
  });

  UI.ingestQueryBtn.addEventListener("click", async () => {
    const f = UI.queryFile.files?.[0];
    if (!f) return setStatus("Choose a SPARQL file first.", true);
    await ingestQueryFile(f);
  });

  UI.ingestMappingBtn.addEventListener("click", async () => {
    const f = UI.mappingFile.files?.[0];
    if (!f) return setStatus("Choose a mapping file first.", true);
    await ingestMappingFile(f);
  });

  UI.buildPreviewBtn.addEventListener("click", async () => {
    const runId = Session.currentInputRunId || UI.runsSelect.value;
    if (!runId) return setStatus("Ingest or load a run first.", true);
    await buildPreviewFromRun(runId);
  });

  UI.applyMappingBtn.addEventListener("click", async () => {
    if (!Session.currentInputRunId) return setStatus("Ingest or load an input query run first.", true);
    if (Session.mapping.size === 0) return setStatus("Ingest a mapping file first.", true);
    await applyMappingToCurrentRun();
  });

  UI.loadRunBtn.addEventListener("click", async () => {
    const runId = UI.runsSelect.value;
    if (!runId) return setStatus("No run selected.", true);
    await loadRun(runId);
  });

  UI.deleteRunBtn.addEventListener("click", async () => {
    const runId = UI.runsSelect.value;
    if (!runId) return setStatus("No run selected.", true);
    await deleteRun(runId);
    await refreshRunsDropdown();
    setStatus(`Deleted run: ${runId}`);
  });

  UI.clearRunsBtn.addEventListener("click", async () => {
    await clearAllRuns();
    await refreshRunsDropdown();
    Session.currentInputRunId = null;
    Session.currentOutputRunId = null;
    Session.mapping = new Map();
    initTable();
    UI.outputPreview.value = "";
    UI.prefixJson.textContent = "{}";
    UI.baseIri.textContent = "—";
    UI.queryRunId.textContent = "—";
    setStatus("Cleared all runs.");
  });

  UI.downloadBtn.addEventListener("click", async () => {
    const runId = Session.currentOutputRunId || Session.currentInputRunId || UI.runsSelect.value;
    if (!runId) return setStatus("No run available to download.", true);
    await downloadRunAsRq(runId);
  });
}

function wireDropzones() {
  makeDropzone(UI.queryDrop, (file) => {
    UI.queryFile.files = fileListFromSingleFile(file);
    setStatus(`SPARQL selected: ${file.name}`);
  });
  makeDropzone(UI.mappingDrop, (file) => {
    UI.mappingFile.files = fileListFromSingleFile(file);
    setStatus(`Mapping selected: ${file.name}`);
  });
}

function makeDropzone(el, onFile) {
  el.addEventListener("dragover", (e) => { e.preventDefault(); el.style.borderColor = "rgba(96,165,250,0.55)"; });
  el.addEventListener("dragleave", () => { el.style.borderColor = ""; });
  el.addEventListener("drop", (e) => {
    e.preventDefault(); el.style.borderColor = "";
    const file = e.dataTransfer?.files?.[0];
    if (file) onFile(file);
  });
}

function fileListFromSingleFile(file) {
  const dt = new DataTransfer();
  dt.items.add(file);
  return dt.files;
}

/* -------------------- Table -------------------- */

function initTable() {
  UI.tableEl.innerHTML = "";

  table = new Tabulator(UI.tableEl, {
    layout: "fitColumns",
    height: "520px",
    data: [],
    placeholder: "No data yet.",
    columns: [
      { title: "Token", field: "token", formatter: "textarea", headerFilter: "input", widthGrow: 3 },
      { title: "Kind", field: "kind", headerFilter: "select", headerFilterParams: { values: { "": "All", "PrefixDecl": "PrefixDecl", "IRIRef": "IRIRef", "PrefixedName": "PrefixedName", "BaseDecl": "BaseDecl" } }, width: 150 },
      { title: "Expanded IRI", field: "expanded", formatter: "textarea", headerFilter: "input", widthGrow: 4 },
      { title: "To-be IRI", field: "toBe", formatter: "textarea", headerFilter: "input", widthGrow: 4 },
      {
        title: "Status",
        field: "status",
        headerFilter: "select",
        headerFilterParams: { values: { "": "All", "Change": "Change", "No change": "No change" } },
        formatter: (cell) => {
          const v = cell.getValue();
          const cls = v === "Change" ? "mb-pill mb-pillChange" : "mb-pill mb-pillNoChange";
          return `<span class="${cls}">${escapeHtml(v)}</span>`;
        },
        width: 140
      }
    ],
    rowFormatter: (row) => {
      const d = row.getData();
      const el = row.getElement();
      el.classList.remove("mb-rowChange", "mb-rowNoChange");
      if (d.status === "Change") el.classList.add("mb-rowChange");
      else el.classList.add("mb-rowNoChange");
    }
  });
}

/* -------------------- Ingest Query -------------------- */

async function ingestQueryFile(file) {
  setStatus(`Ingesting query: ${file.name} …`);

  const queryText = await file.text();
  const createdAt = new Date().toISOString();
  const runId = makeRunId("input", file.name, createdAt);

  const { prefixes, baseIri } = parsePrefixesAndBase(queryText);
  const tokens = extractTokens(queryText, prefixes);

  const stats = {
    uniqueTokens: tokens.length,
    prefixCount: Object.keys(prefixes).length,
    hasBase: !!baseIri,
  };

  await putRun({
    runId,
    kind: "input",
    parentRunId: null,
    fileName: file.name,
    createdAt,
    queryText,
    prefixes,
    baseIri: baseIri || "",
    tokens,
    stats,
    mappingMeta: null,
  });

  Session.currentInputRunId = runId;
  Session.currentOutputRunId = null;

  UI.queryRunId.textContent = runId;
  UI.baseIri.textContent = baseIri || "—";
  UI.prefixJson.textContent = JSON.stringify(prefixes, null, 2);

  await refreshRunsDropdown(runId);
  await buildPreviewFromRun(runId);

  setStatus(`Query ingested. Staged tokens: ${tokens.length}`);
}

/* PREFIX/BASE parsing: robust enough for typical SPARQL headers */
function parsePrefixesAndBase(queryText) {
  const prefixes = {};
  let baseIri = "";

  const prefixRe = /^\s*PREFIX\s+([A-Za-z_][\w-]*)?:\s*<([^>]+)>\s*$/gmi;
  const baseRe = /^\s*BASE\s+<([^>]+)>\s*$/gmi;

  let m;
  while ((m = prefixRe.exec(queryText))) {
    const pfx = (m[1] || "").trim();
    const ns = (m[2] || "").trim();
    prefixes[pfx] = ns;
  }

  const b = baseRe.exec(queryText);
  if (b && b[1]) baseIri = String(b[1]).trim();

  return { prefixes, baseIri };
}

/**
 * Extract staged tokens:
 * - PrefixDecl (namespace IRIs in PREFIX lines)
 * - BaseDecl (BASE <...>)
 * - IRIRef (<...> anywhere outside strings/comments)
 * - PrefixedName (ex:Foo) expanded using PREFIX map (outside strings/comments/<...>)
 */
function extractTokens(queryText, prefixes) {
  const staged = new Map(); // key -> row object; key uses kind+token+expanded to avoid weird collisions

  // Always stage prefix declarations themselves
  for (const [pfx, ns] of Object.entries(prefixes)) {
    const token = `PREFIX ${pfx}:`;
    staged.set(`PrefixDecl|${token}|${ns}`, {
      token,
      kind: "PrefixDecl",
      expanded: ns,
    });
  }

  const { baseIri } = parsePrefixesAndBase(queryText);
  if (baseIri) {
    staged.set(`BaseDecl|BASE|${baseIri}`, {
      token: "BASE",
      kind: "BaseDecl",
      expanded: baseIri,
    });
  }

  // Scan outside comments/strings for <...> and prefixed names
  const scan = scanSparql(queryText);

  for (const iri of scan.iriRefs) {
    staged.set(`IRIRef|<${iri}>|${iri}`, {
      token: `<${iri}>`,
      kind: "IRIRef",
      expanded: iri,
    });
  }

  for (const pn of scan.prefixedNames) {
    const expanded = expandPrefixedName(pn, prefixes);
    if (!expanded) continue;
    staged.set(`PrefixedName|${pn}|${expanded}`, {
      token: pn,
      kind: "PrefixedName",
      expanded,
    });
  }

  // Return stable sorted output
  return Array.from(staged.values()).sort((a, b) => (a.expanded || "").localeCompare(b.expanded || ""));
}

function expandPrefixedName(token, prefixes) {
  const idx = token.indexOf(":");
  if (idx < 0) return "";
  const pfx = token.slice(0, idx); // may be empty for :local
  const local = token.slice(idx + 1);
  const ns = prefixes[pfx];
  if (!ns) return "";
  return ns + local;
}

/* A small scanner: ignores #comments and quoted strings; extracts <...> and prefixed names */
function scanSparql(text) {
  const iriRefs = new Set();
  const prefixedNames = new Set();

  let i = 0;
  let inComment = false;
  let inS = false, inD = false;
  let inLS = false, inLD = false; // ''' or """
  let inIri = false;

  const isNL = (c) => c === "\n" || c === "\r";
  const isNameStart = (c) => /[A-Za-z_]/.test(c);
  const isNameChar = (c) => /[A-Za-z0-9_\-]/.test(c);
  const isLocalChar = (c) => /[A-Za-z0-9_\-\.]/.test(c);

  while (i < text.length) {
    const c = text[i];
    const c2 = text.slice(i, i + 3);

    // comment
    if (!inS && !inD && !inLS && !inLD && !inIri && c === "#") {
      inComment = true;
    }
    if (inComment) {
      if (isNL(c)) inComment = false;
      i++;
      continue;
    }

    // long strings
    if (!inS && !inD && !inIri && c2 === "'''") { inLS = !inLS; i += 3; continue; }
    if (!inS && !inD && !inIri && c2 === '"""') { inLD = !inLD; i += 3; continue; }
    if (inLS || inLD) { i++; continue; }

    // normal strings
    if (!inD && !inIri && c === "'" && text[i - 1] !== "\\") { inS = !inS; i++; continue; }
    if (!inS && !inIri && c === '"' && text[i - 1] !== "\\") { inD = !inD; i++; continue; }
    if (inS || inD) { i++; continue; }

    // IRI ref
    if (!inIri && c === "<") {
      const j = text.indexOf(">", i + 1);
      if (j > i) {
        const iri = text.slice(i + 1, j).trim();
        if (iri) iriRefs.add(iri);
        i = j + 1;
        continue;
      }
    }

    // prefixed name token (very practical subset)
    if (isNameStart(c) || c === ":") {
      // prefix part can be empty when token starts with :
      let start = i;
      let p = i;

      if (c === ":") {
        // default prefix, local must start next
        p++;
      } else {
        p++; // consumed name start
        while (p < text.length && isNameChar(text[p])) p++;
        if (text[p] !== ":") { i++; continue; }
        p++; // consume ':'
      }

      // local
      if (p >= text.length || !isNameStart(text[p]) && !/[0-9_]/.test(text[p])) { i++; continue; }
      p++;
      while (p < text.length && isLocalChar(text[p])) p++;

      const token = text.slice(start, p);
      // Avoid picking up "http:" (rare outside <...>, but safe-guard)
      if (!token.startsWith("http:") && !token.startsWith("https:")) {
        prefixedNames.add(token);
      }

      i = p;
      continue;
    }

    i++;
  }

  return { iriRefs, prefixedNames };
}

/* -------------------- Ingest Mapping -------------------- */

async function ingestMappingFile(file) {
  setStatus(`Ingesting mapping: ${file.name} …`);

  const ext = (file.name.split(".").pop() || "").toLowerCase();
  let rows = [];

  if (ext === "csv" || ext === "tsv") {
    const text = await file.text();
    const parsed = Papa.parse(text, {
      header: true,
      skipEmptyLines: true,
      delimiter: ext === "tsv" ? "\t" : undefined,
    });
    if (parsed.errors?.length) throw new Error(parsed.errors[0]?.message || "CSV parse error");
    rows = parsed.data || [];
  } else if (ext === "xls" || ext === "xlsx") {
    const buf = await file.arrayBuffer();
    const wb = XLSX.read(buf, { type: "array" });
    const ws = wb.Sheets[wb.SheetNames[0]];
    rows = XLSX.utils.sheet_to_json(ws, { defval: "" });
  } else {
    throw new Error("Unsupported mapping format.");
  }

  const { mapping, meta } = rowsToMapping(rows);
  Session.mapping = mapping;
  Session.mappingMeta = meta;

  UI.mappingRows.textContent = String(meta.rows);
  UI.mappingUniqueOld.textContent = String(meta.uniqueOld);
  UI.mappingDupOld.textContent = String(meta.dupOld);

  UI.kpiMapping.textContent = String(meta.uniqueOld);

  setStatus(`Mapping ingested. Unique old IRIs: ${meta.uniqueOld}`);
}

function rowsToMapping(rows) {
  const norm = (s) => String(s || "").trim().toLowerCase();
  let oldKey = null, newKey = null;

  if (rows.length > 0) {
    const keys = Object.keys(rows[0] || {});
    for (const k of keys) {
      const nk = norm(k);
      if (nk === "old iri") oldKey = k;
      if (nk === "new iri") newKey = k;
    }
    for (const k of keys) {
      const nk = norm(k);
      if (!oldKey && nk.includes("old") && nk.includes("iri")) oldKey = k;
      if (!newKey && nk.includes("new") && nk.includes("iri")) newKey = k;
    }
  }

  if (!oldKey || !newKey) {
    throw new Error(`Expected headers like "Old IRI" and "New IRI". Found: ${rows.length ? Object.keys(rows[0]).join(", ") : "(no rows)"}`);
  }

  const mapping = new Map();
  const seen = new Set();
  let dupOld = 0;

  for (const r of rows) {
    const oldIri = String(r[oldKey] || "").trim();
    const newIri = String(r[newKey] || "").trim();
    if (!oldIri) continue;
    if (seen.has(oldIri)) dupOld++;
    seen.add(oldIri);
    mapping.set(oldIri, newIri);
  }

  return { mapping, meta: { rows: rows.length, uniqueOld: mapping.size, dupOld } };
}

/* -------------------- Preview -------------------- */

async function buildPreviewFromRun(runId) {
  const run = await getRun(runId);
  if (!run) return setStatus("Run not found.", true);

  // set session pointers
  if (run.kind === "input") {
    Session.currentInputRunId = run.runId;
    Session.currentOutputRunId = null;
  } else {
    Session.currentOutputRunId = run.runId;
    Session.currentInputRunId = run.parentRunId || run.runId;
  }

  UI.queryRunId.textContent = Session.currentInputRunId || "—";
  UI.baseIri.textContent = run.baseIri || "—";
  UI.prefixJson.textContent = JSON.stringify(run.prefixes || {}, null, 2);

  const preview = buildPreviewRows(run, Session.mapping);

  table.replaceData(preview.rows);
  UI.kpiTokens.textContent = String(preview.total);
  UI.kpiProposed.textContent = String(preview.proposed);
  UI.kpiPct.textContent = preview.total ? `${Math.round((preview.proposed / preview.total) * 100)}%` : "0%";

  setStatus(`Preview built for run: ${runId}`);

  if (run.kind === "output") {
    UI.outputPreview.value = run.queryText || "";
  } else {
    UI.outputPreview.value = "";
  }
}

function buildPreviewRows(run, mapping) {
  const prefixes = run.prefixes || {};
  const tokens = run.tokens || [];

  // Figure out which prefix namespaces would change (only when mapping includes that exact namespace IRI)
  const prefixNsToBe = {};
  for (const [pfx, ns] of Object.entries(prefixes)) {
    const mapped = mapping.get(ns);
    if (mapped && mapped !== ns) prefixNsToBe[pfx] = mapped;
  }

  const rows = [];
  let proposed = 0;

  for (const t of tokens) {
    const expanded = t.expanded || "";
    let toBe = "";
    let status = "No change";

    if (t.kind === "PrefixDecl" || t.kind === "BaseDecl") {
      const mapped = mapping.get(expanded);
      if (mapped && mapped !== expanded) toBe = mapped;
    } else if (t.kind === "IRIRef") {
      const mapped = mapping.get(expanded);
      if (mapped && mapped !== expanded) toBe = mapped;
    } else if (t.kind === "PrefixedName") {
      // direct term mapping wins
      const mapped = mapping.get(expanded);
      if (mapped && mapped !== expanded) {
        toBe = mapped;
      } else {
        // prefix namespace mapping affects meaning
        const idx = t.token.indexOf(":");
        const pfx = idx >= 0 ? t.token.slice(0, idx) : "";
        const local = idx >= 0 ? t.token.slice(idx + 1) : "";
        if (prefixNsToBe[pfx]) {
          const implied = prefixNsToBe[pfx] + local;
          if (implied !== expanded) toBe = implied;
        }
      }
    }

    if (toBe) {
      status = "Change";
      proposed++;
    }

    rows.push({
      token: t.token,
      kind: t.kind,
      expanded,
      toBe,
      status
    });
  }

  return { rows, proposed, total: rows.length };
}

/* -------------------- Apply mapping -------------------- */

async function applyMappingToCurrentRun() {
  const inputRun = await getRun(Session.currentInputRunId);
  if (!inputRun) return setStatus("Input run not found.", true);

  const createdAt = new Date().toISOString();
  const outputRunId = makeRunId("output", inputRun.fileName, createdAt);

  const useNativePrefixes = !!UI.useNativePrefixes.checked;

  // Apply mapping:
  // 1) Update PREFIX/BASE IRIs if mapping hits them exactly
  // 2) Replace <oldIri> with <newIri> everywhere outside strings/comments
  // 3) If a prefixed name expands to an oldIri that is directly mapped, replace token with <newIri> (or prefixed if possible)
  const out = rewriteSparqlQuery(inputRun.queryText, inputRun.prefixes || {}, Session.mapping, useNativePrefixes);

  const { prefixes: outPrefixes, baseIri: outBaseIri } = parsePrefixesAndBase(out);

  const outTokens = extractTokens(out, outPrefixes);
  const preview = buildPreviewRows({ tokens: outTokens, prefixes: outPrefixes }, new Map()); // no “next changes” on output

  await putRun({
    runId: outputRunId,
    kind: "output",
    parentRunId: inputRun.runId,
    fileName: outputFileName(inputRun.fileName),
    createdAt,
    queryText: out,
    prefixes: outPrefixes,
    baseIri: outBaseIri || "",
    tokens: outTokens,
    stats: {
      uniqueTokens: outTokens.length,
      proposedChangesApplied: countAppliedChanges(inputRun, out),
    },
    mappingMeta: Session.mappingMeta,
  });

  Session.currentOutputRunId = outputRunId;

  await refreshRunsDropdown(outputRunId);
  await loadRun(outputRunId);

  setStatus(`Output run created. Applied changes: ${countAppliedChanges(inputRun, out)}`);
}

function rewriteSparqlQuery(queryText, prefixes, mapping, useNativePrefixes) {
  // First: update PREFIX/BASE declarations via regex (safe, line-based)
  let text = queryText;

  text = text.replace(/^\s*PREFIX\s+([A-Za-z_][\w-]*)?:\s*<([^>]+)>\s*$/gmi, (full, pfxRaw, nsRaw) => {
    const pfx = (pfxRaw || "").trim();
    const ns = (nsRaw || "").trim();
    const mapped = mapping.get(ns);
    if (mapped && mapped !== ns) return full.replace(`<${ns}>`, `<${mapped}>`);
    return full;
  });

  text = text.replace(/^\s*BASE\s+<([^>]+)>\s*$/gmi, (full, baseRaw) => {
    const base = (baseRaw || "").trim();
    const mapped = mapping.get(base);
    if (mapped && mapped !== base) return full.replace(`<${base}>`, `<${mapped}>`);
    return full;
  });

  // Re-parse prefixes after possible prefix namespace updates
  const { prefixes: updatedPrefixes } = parsePrefixesAndBase(text);

  // Now do a single pass replacement for <...> IRIs and prefixed names (skip PREFIX/BASE lines)
  return rewriteBody(text, prefixes, updatedPrefixes, mapping, useNativePrefixes);
}

function rewriteBody(text, originalPrefixes, updatedPrefixes, mapping, useNativePrefixes) {
  let i = 0;
  let out = "";
  let atLineStart = true;
  let skipQNameOnThisLine = false;

  let inComment = false;
  let inS = false, inD = false;
  let inLS = false, inLD = false;

  const isNL = (c) => c === "\n" || c === "\r";
  const isNameStart = (c) => /[A-Za-z_]/.test(c);
  const isNameChar = (c) => /[A-Za-z0-9_\-]/.test(c);
  const isLocalChar = (c) => /[A-Za-z0-9_\-\.]/.test(c);

  while (i < text.length) {
    const c = text[i];
    const c2 = text.slice(i, i + 3);

    // line start detection
    if (atLineStart) {
      skipQNameOnThisLine = false;
      const rest = text.slice(i).replace(/^\s+/, "");
      if (/^PREFIX\b/i.test(rest) || /^BASE\b/i.test(rest)) skipQNameOnThisLine = true;
      atLineStart = false;
    }

    if (isNL(c)) {
      atLineStart = true;
      out += c;
      i++;
      continue;
    }

    // comments
    if (!inS && !inD && !inLS && !inLD && c === "#") inComment = true;
    if (inComment) {
      out += c;
      if (isNL(c)) inComment = false;
      i++;
      continue;
    }

    // long strings
    if (!inS && !inD && c2 === "'''") { inLS = !inLS; out += c2; i += 3; continue; }
    if (!inS && !inD && c2 === '"""') { inLD = !inLD; out += c2; i += 3; continue; }
    if (inLS || inLD) { out += c; i++; continue; }

    // normal strings
    if (!inD && c === "'" && text[i - 1] !== "\\") { inS = !inS; out += c; i++; continue; }
    if (!inS && c === '"' && text[i - 1] !== "\\") { inD = !inD; out += c; i++; continue; }
    if (inS || inD) { out += c; i++; continue; }

    // <IRI> replacement
    if (c === "<") {
      const j = text.indexOf(">", i + 1);
      if (j > i) {
        const iri = text.slice(i + 1, j).trim();
        const mapped = mapping.get(iri);
        if (mapped && mapped !== iri) {
          out += `<${mapped}>`;
        } else {
          out += text.slice(i, j + 1);
        }
        i = j + 1;
        continue;
      }
    }

    // Prefixed name replacement (only for direct term mappings, not prefix-mapping-by-meaning)
    if (!skipQNameOnThisLine && (isNameStart(c) || c === ":")) {
      let start = i;
      let p = i;

      if (c === ":") {
        p++;
      } else {
        p++;
        while (p < text.length && isNameChar(text[p])) p++;
        if (text[p] !== ":") { out += c; i++; continue; }
        p++;
      }

      if (p >= text.length) { out += c; i++; continue; }
      if (!isNameStart(text[p]) && !/[0-9_]/.test(text[p])) { out += c; i++; continue; }

      p++;
      while (p < text.length && isLocalChar(text[p])) p++;

      const token = text.slice(start, p);
      if (token.startsWith("http:") || token.startsWith("https:")) {
        out += token;
        i = p;
        continue;
      }

      const expanded = expandPrefixedName(token, originalPrefixes);
      const mapped = expanded ? mapping.get(expanded) : "";
      if (mapped && mapped !== expanded) {
        out += chooseQNameOrIri(mapped, updatedPrefixes, useNativePrefixes);
      } else {
        out += token;
      }

      i = p;
      continue;
    }

    out += c;
    i++;
  }

  return out;
}

function chooseQNameOrIri(newIri, prefixes, allowQName) {
  if (!allowQName) return `<${newIri}>`;

  // Try to shorten with any prefix namespace that matches the start
  for (const [pfx, ns] of Object.entries(prefixes || {})) {
    if (!pfx) continue;
    if (newIri.startsWith(ns)) {
      const local = newIri.slice(ns.length);
      // very pragmatic local-name safety (don’t get fancy)
      if (/^[A-Za-z0-9_\-\.]+$/.test(local)) return `${pfx}:${local}`;
    }
  }
  return `<${newIri}>`;
}

function countAppliedChanges(inputRun, outText) {
  // Simple metric: how many mapping keys disappear and mapping values appear in output
  // (Fast eyeball metric; not a formal diff.)
  let count = 0;
  for (const [oldIri, newIri] of Session.mapping.entries()) {
    if (!oldIri || !newIri || oldIri === newIri) continue;
    if (inputRun.queryText.includes(oldIri) && outText.includes(newIri)) count++;
  }
  return count;
}

function outputFileName(inputName) {
  const idx = inputName.lastIndexOf(".");
  if (idx <= 0) return `${inputName}.mapped.rq`;
  return `${inputName.slice(0, idx)}.mapped${inputName.slice(idx)}`;
}

/* -------------------- Runs: IndexedDB -------------------- */

function makeRunId(kind, fileName, iso) {
  const safe = fileName.replace(/[^\w.-]+/g, "_");
  return `urn:myna:sparql:${kind}:${safe}:${iso}`;
}

async function openDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB.name, DB.version);
    req.onerror = () => reject(req.error);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(DB.storeRuns)) {
        const store = db.createObjectStore(DB.storeRuns, { keyPath: "runId" });
        store.createIndex("createdAt", "createdAt", { unique: false });
        store.createIndex("kind", "kind", { unique: false });
      }
    };
    req.onsuccess = () => resolve(req.result);
  });
}

async function putRun(run) {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(DB.storeRuns, "readwrite");
    tx.objectStore(DB.storeRuns).put(run);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function getRun(runId) {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(DB.storeRuns, "readonly");
    const req = tx.objectStore(DB.storeRuns).get(runId);
    req.onsuccess = () => resolve(req.result || null);
    req.onerror = () => reject(req.error);
  });
}

async function listRuns() {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(DB.storeRuns, "readonly");
    const req = tx.objectStore(DB.storeRuns).getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
}

async function deleteRun(runId) {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(DB.storeRuns, "readwrite");
    tx.objectStore(DB.storeRuns).delete(runId);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function clearAllRuns() {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(DB.storeRuns, "readwrite");
    tx.objectStore(DB.storeRuns).clear();
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function refreshRunsDropdown(selectRunId = null) {
  const runs = await listRuns();
  runs.sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));

  UI.runsSelect.innerHTML = "";
  for (const r of runs) {
    const opt = document.createElement("option");
    opt.value = r.runId;
    const stamp = (r.createdAt || "").replace("T", " ").replace("Z", "");
    opt.textContent = `[${r.kind}] ${stamp} — ${r.fileName}`;
    UI.runsSelect.appendChild(opt);
  }

  if (selectRunId) UI.runsSelect.value = selectRunId;
  else if (runs.length) UI.runsSelect.value = runs[0].runId;
}

async function loadRun(runId) {
  const run = await getRun(runId);
  if (!run) return setStatus("Run not found.", true);

  if (run.kind === "input") {
    Session.currentInputRunId = run.runId;
    Session.currentOutputRunId = null;
  } else {
    Session.currentOutputRunId = run.runId;
    Session.currentInputRunId = run.parentRunId || run.runId;
  }

  UI.queryRunId.textContent = Session.currentInputRunId || "—";
  UI.baseIri.textContent = run.baseIri || "—";
  UI.prefixJson.textContent = JSON.stringify(run.prefixes || {}, null, 2);

  await buildPreviewFromRun(run.kind === "output" ? Session.currentInputRunId : runId);

  if (run.kind === "output") {
    UI.outputPreview.value = run.queryText || "";
  } else {
    UI.outputPreview.value = "";
  }

  setStatus(`Loaded run: ${runId}`);
}

async function downloadRunAsRq(runId) {
  const run = await getRun(runId);
  if (!run) return setStatus("Run not found.", true);

  const body = run.queryText || "";
  const name = ensureRqExtension(run.fileName || "query.rq");

  const blob = new Blob([body], { type: "application/sparql-query" });
  const url = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();

  setTimeout(() => URL.revokeObjectURL(url), 1500);
  setStatus(`Downloaded: ${name}`);
}

function ensureRqExtension(name) {
  if (/\.(rq|sparql)$/i.test(name)) return name;
  return name + ".rq";
}

/* -------------------- Utilities -------------------- */

function escapeHtml(str) {
  return String(str ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setStatus(msg, isError = false) {
  UI.status.textContent = msg;
  UI.status.style.color = isError ? "var(--danger)" : "var(--muted)";
  console.log(isError ? "[myna-sparql:error]" : "[myna-sparql]", msg);
}

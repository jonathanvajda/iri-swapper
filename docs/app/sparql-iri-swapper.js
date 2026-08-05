/* sparql-iri-swapper.js - SPARQL query IRI mapper (runs in parallel to your ontology tool; no edits to existing JS) */
import { downloadTextFile } from './shared/browser-file-io/index.js';
import {
  createIriMappingFromRows,
  parseDelimitedText
} from './shared/tabular-io/index.js';
import {
  downloadRunOutputForExport,
  resolveOutputRunForExport
} from './shared/indexeddb-data-management/index.js';
import {
  clearIriSwapperRuns,
  createIriSwapperRunId,
  deleteIriSwapperRun,
  listIriSwapperRuns,
  readIriSwapperRun,
  storeIriSwapperRun
} from './iri-swapper-run-store.js';
import {
  buildSparqlIriPreviewRows,
  countSparqlAppliedChanges,
  extractSparqlIriTokens,
  parsePrefixesAndBase,
  rewriteSparqlQuery
} from './sparql-iri-swapper-core.js';

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
  UI.toggleThemeBtn?.addEventListener("click", () => {
    document.getElementById("mb-app").classList.toggle("mb-light");
  });

  UI.ingestQueryBtn.addEventListener("click", runUiAction("ingest query", async () => {
    const f = UI.queryFile.files?.[0];
    if (!f) return setStatus("Choose a SPARQL file first.", true);
    await ingestQueryFile(f);
  }));

  UI.ingestMappingBtn.addEventListener("click", runUiAction("ingest mapping", async () => {
    const f = UI.mappingFile.files?.[0];
    if (!f) return setStatus("Choose a mapping file first.", true);
    await ingestMappingFile(f);
  }));

  UI.buildPreviewBtn.addEventListener("click", runUiAction("build preview", async () => {
    const runId = Session.currentInputRunId || UI.runsSelect.value;
    if (!runId) return setStatus("Ingest or load a run first.", true);
    await buildPreviewFromRun(runId);
  }));

  UI.applyMappingBtn.addEventListener("click", runUiAction("apply mapping", async () => {
    if (!Session.currentInputRunId) return setStatus("Ingest or load an input query run first.", true);
    if (Session.mapping.size === 0) return setStatus("Ingest a mapping file first.", true);
    await applyMappingToCurrentRun();
  }));

  UI.loadRunBtn.addEventListener("click", runUiAction("load run", async () => {
    const runId = UI.runsSelect.value;
    if (!runId) return setStatus("No run selected.", true);
    await loadRun(runId);
  }));

  UI.deleteRunBtn.addEventListener("click", runUiAction("delete run", async () => {
    const runId = UI.runsSelect.value;
    if (!runId) return setStatus("No run selected.", true);
    await deleteRun(runId);
    await refreshRunsDropdown();
    setStatus(`Deleted run: ${runId}`);
  }));

  UI.clearRunsBtn.addEventListener("click", runUiAction("clear runs", async () => {
    await clearAllRuns();
    await refreshRunsDropdown();
    Session.currentInputRunId = null;
    Session.currentOutputRunId = null;
    Session.mapping = new Map();
    initTable();
    UI.outputPreview.value = "";
    UI.prefixJson.textContent = "{}";
    UI.baseIri.textContent = "â€”";
    UI.queryRunId.textContent = "â€”";
    setStatus("Cleared all runs.");
  }));

  UI.downloadBtn.addEventListener("click", runUiAction("download output", async () => {
    const runId = await resolveActiveOutputRunId();
    if (!runId) return setStatus("Apply mappings to create an output run before downloading.", true);
    await downloadRunAsRq(runId);
  }));
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
      { title: "Kind", field: "kind", headerFilter: "select", headerFilterParams: { values: { "": "All", "IRIRef": "IRIRef", "PrefixedName": "PrefixedName" } }, width: 150 },
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
  setStatus(`Ingesting query: ${file.name} â€¦`);

  const queryText = await file.text();
  const createdAt = new Date().toISOString();
  const runId = makeRunId("input", file.name, createdAt);

  const { prefixes, baseIri } = parsePrefixesAndBase(queryText);
  const tokens = extractSparqlIriTokens(queryText, prefixes);

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
  UI.baseIri.textContent = baseIri || "â€”";
  UI.prefixJson.textContent = JSON.stringify(prefixes, null, 2);

  await refreshRunsDropdown(runId);
  await buildPreviewFromRun(runId);

  setStatus(`Query ingested. Staged tokens: ${tokens.length}`);
}

/* -------------------- Ingest Mapping -------------------- */

async function ingestMappingFile(file) {
  setStatus(`Ingesting mapping: ${file.name} â€¦`);

  const ext = (file.name.split(".").pop() || "").toLowerCase();
  let rows = [];

  if (ext === "csv" || ext === "tsv") {
    const text = await file.text();
    const parsed = parseDelimitedText(text, {
      hasHeader: true,
      trimHeaders: true,
      trimCells: true,
      delimiter: ext === "tsv" ? "\t" : undefined,
    });
    if (parsed.warnings?.length) {
      console.warn("Mapping parse warnings:", parsed.warnings);
    }
    rows = parsed.records || [];
  } else if (ext === "xls" || ext === "xlsx") {
    const buf = await file.arrayBuffer();
    const wb = XLSX.read(buf, { type: "array" });
    const ws = wb.Sheets[wb.SheetNames[0]];
    rows = XLSX.utils.sheet_to_json(ws, { defval: "" });
  } else {
    throw new Error("Unsupported mapping format.");
  }

  const { mapping, meta } = createIriMappingFromRows(rows);
  const mappingMeta = {
    rows: meta.rows,
    uniqueOld: meta.uniqueOld,
    dupOld: meta.duplicateOld
  };
  Session.mapping = mapping;
  Session.mappingMeta = mappingMeta;

  UI.mappingRows.textContent = String(mappingMeta.rows);
  UI.mappingUniqueOld.textContent = String(mappingMeta.uniqueOld);
  UI.mappingDupOld.textContent = String(mappingMeta.dupOld);

  UI.kpiMapping.textContent = String(mappingMeta.uniqueOld);

  setStatus(`Mapping ingested. Unique old IRIs: ${mappingMeta.uniqueOld}`);
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

  if (run.kind === "input") {
    Session.currentInputRunId = run.runId;
  } else {
    Session.currentOutputRunId = run.runId;
    Session.currentInputRunId = run.parentRunId || run.runId;
  }

  UI.queryRunId.textContent = Session.currentInputRunId || "â€”";
  UI.baseIri.textContent = run.baseIri || "â€”";
  UI.prefixJson.textContent = JSON.stringify(run.prefixes || {}, null, 2);

  const preview = buildSparqlIriPreviewRows(run, Session.mapping);

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

  const outTokens = extractSparqlIriTokens(out, outPrefixes);
  const preview = buildSparqlIriPreviewRows({ tokens: outTokens, prefixes: outPrefixes }, new Map()); // no next changes on output

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
      proposedChangesApplied: countSparqlAppliedChanges(inputRun, out, Session.mapping, { useNativePrefixes }),
    },
    mappingMeta: Session.mappingMeta,
  });

  Session.currentOutputRunId = outputRunId;

  await refreshRunsDropdown(outputRunId);
  await loadRun(outputRunId);

  setStatus(`Output run created. Applied changes: ${countSparqlAppliedChanges(inputRun, out, Session.mapping, { useNativePrefixes })}`);
}

function outputFileName(inputName) {
  const idx = inputName.lastIndexOf(".");
  if (idx <= 0) return `${inputName}.mapped.rq`;
  return `${inputName.slice(0, idx)}.mapped${inputName.slice(idx)}`;
}

/* -------------------- Runs: IndexedDB -------------------- */

function makeRunId(kind, fileName, iso) {
  return createIriSwapperRunId('sparql', kind, fileName, iso);
}

async function putRun(run) {
  return storeIriSwapperRun(run, { runKind: 'sparql-iri-rewrite' });
}

async function getRun(runId) {
  return readIriSwapperRun(runId);
}

async function listRuns() {
  return listIriSwapperRuns({ runKind: 'sparql-iri-rewrite' });
}

async function deleteRun(runId) {
  return deleteIriSwapperRun(runId);
}

async function clearAllRuns() {
  return clearIriSwapperRuns({ runKind: 'sparql-iri-rewrite' });
}

async function refreshRunsDropdown(selectRunId = null) {
  const runs = await listRuns();
  runs.sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)));

  UI.runsSelect.innerHTML = "";
  for (const r of runs) {
    const opt = document.createElement("option");
    opt.value = r.runId;
    const stamp = (r.createdAt || "").replace("T", " ").replace("Z", "");
    opt.textContent = `[${r.kind}] ${stamp} â€” ${r.fileName}`;
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

  UI.queryRunId.textContent = Session.currentInputRunId || "â€”";
  UI.baseIri.textContent = run.baseIri || "â€”";
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

  const result = await downloadRunOutputForExport(run, {
    mimeType: "application/sparql-query",
    textProperty: "queryText",
    downloadTextFile
  });
  setStatus(`Downloaded: ${result.serialized.fileName}`);
}

async function resolveActiveOutputRunId() {
  const resolved = await resolveOutputRunForExport({
    activeOutputRunId: Session.currentOutputRunId,
    selectedRunId: UI.runsSelect.value,
    inputRunId: Session.currentInputRunId,
    readRun: getRun,
    listRuns
  });
  if (!resolved) return null;
  Session.currentOutputRunId = resolved.runId;
  Session.currentInputRunId = resolved.parentRunId || Session.currentInputRunId;
  UI.runsSelect.value = resolved.runId;
  return resolved.runId;
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

function runUiAction(label, action) {
  return async () => {
    try {
      await action();
    } catch (error) {
      console.error(`[myna-sparql:${label}] failed`, error);
      setStatus(`${label} failed: ${error?.message || error}`, true);
    }
  };
}

/* app.js (ES module) - core logic stays out of the DOM; DOM only supplies options/events */

const APP = {
  dbName: "myna-iri-mapper-db",
  dbVersion: 1,
  storeRuns: "runs",
};

const UI = {
  ontologyFile: document.getElementById("ontologyFile"),
  mappingFile: document.getElementById("mappingFile"),
  ontologyDrop: document.getElementById("ontologyDrop"),
  mappingDrop: document.getElementById("mappingDrop"),
  loadOntologyBtn: document.getElementById("loadOntologyBtn"),
  loadMappingBtn: document.getElementById("loadMappingBtn"),
  buildPreviewBtn: document.getElementById("buildPreviewBtn"),
  applyMappingBtn: document.getElementById("applyMappingBtn"),
  downloadBtn: document.getElementById("downloadBtn"),
  exportFormat: document.getElementById("exportFormat"),

  baseIri: document.getElementById("baseIri"),
  useNativePrefixes: document.getElementById("useNativePrefixes"),

  ontologyFormat: document.getElementById("ontologyFormat"),
  ontologyRunId: document.getElementById("ontologyRunId"),
  prefixJson: document.getElementById("prefixJson"),

  mappingRows: document.getElementById("mappingRows"),
  mappingUniqueOld: document.getElementById("mappingUniqueOld"),
  mappingDupOld: document.getElementById("mappingDupOld"),

  kpiOntologyIris: document.getElementById("kpiOntologyIris"),
  kpiMappingIris: document.getElementById("kpiMappingIris"),
  kpiProposedChanges: document.getElementById("kpiProposedChanges"),
  kpiPctChanged: document.getElementById("kpiPctChanged"),

  outputPreview: document.getElementById("outputPreview"),
  status: document.getElementById("status"),

  runsSelect: document.getElementById("runsSelect"),
  loadRunBtn: document.getElementById("loadRunBtn"),
  deleteRunBtn: document.getElementById("deleteRunBtn"),
  clearRunsBtn: document.getElementById("clearRunsBtn"),

  toggleThemeBtn: document.getElementById("mbToggleThemeBtn"),
};

let table = null;

const Session = {
  currentOntologyRunId: null,
  currentOutputRunId: null,
  ontologyPrefixes: {},
  mapping: new Map(), // oldIri -> newIri
  mappingMeta: { rows: 0, uniqueOld: 0, dupOld: 0 },
};

init().catch(err => setStatus(`Init error: ${err?.message || err}`, true));

async function init() {
  wireDropzones();
  wireButtons();
  await registerServiceWorker();
  await refreshRunsDropdown();
  initTable();
  setStatus("Ready.");
}

function wireButtons() {
  UI.loadOntologyBtn.addEventListener("click", async () => {
    const f = UI.ontologyFile.files?.[0];
    if (!f) return setStatus("Choose an ontology file first.", true);
    await ingestOntology(f);
  });

  UI.loadMappingBtn.addEventListener("click", async () => {
    const f = UI.mappingFile.files?.[0];
    if (!f) return setStatus("Choose a mapping file first.", true);
    await ingestMapping(f);
  });

  UI.buildPreviewBtn.addEventListener("click", async () => {
    if (!Session.currentOntologyRunId) return setStatus("Ingest an ontology first.", true);
    await buildPreviewFromRun(Session.currentOntologyRunId);
  });

  UI.applyMappingBtn.addEventListener("click", async () => {
    if (!Session.currentOntologyRunId) return setStatus("Ingest an ontology first.", true);
    if (Session.mapping.size === 0) return setStatus("Ingest a mapping file first.", true);
    await applyMappingToCurrentOntology();
  });

  UI.downloadBtn.addEventListener("click", async () => {
    const runId = Session.currentOutputRunId || Session.currentOntologyRunId;
    if (!runId) return setStatus("Load or create an output run first.", true);
    await downloadRun(runId, UI.exportFormat.value);
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
    Session.currentOntologyRunId = null;
    Session.currentOutputRunId = null;
    Session.mapping = new Map();
    initTable();
    UI.outputPreview.value = "";
    setStatus("Cleared all runs.");
  });

  UI.toggleThemeBtn.addEventListener("click", () => {
    const root = document.getElementById("mb-app");
    root.classList.toggle("mb-light");
  });
}

function wireDropzones() {
  makeDropzone(UI.ontologyDrop, (file) => {
    UI.ontologyFile.files = fileListFromSingleFile(file);
    setStatus(`Ontology selected: ${file.name}`);
  });

  makeDropzone(UI.mappingDrop, (file) => {
    UI.mappingFile.files = fileListFromSingleFile(file);
    setStatus(`Mapping selected: ${file.name}`);
  });
}

function makeDropzone(el, onFile) {
  el.addEventListener("dragover", (e) => {
    e.preventDefault();
    el.style.borderColor = "rgba(96,165,250,0.55)";
  });
  el.addEventListener("dragleave", () => {
    el.style.borderColor = "";
  });
  el.addEventListener("drop", (e) => {
    e.preventDefault();
    el.style.borderColor = "";
    const file = e.dataTransfer?.files?.[0];
    if (file) onFile(file);
  });
}

function fileListFromSingleFile(file) {
  const dt = new DataTransfer();
  dt.items.add(file);
  return dt.files;
}

function initTable() {
  const el = document.getElementById("table");
  el.innerHTML = "";

  table = new Tabulator(el, {
    layout: "fitColumns",
    height: "520px",
    placeholder: "No data yet.",
    reactiveData: false,
    data: [],
    columns: [
      { title: "IRI", field: "iri", formatter: "textarea", headerFilter: "input", widthGrow: 3 },
      { title: "rdfs:label", field: "label", formatter: "textarea", headerFilter: "input", widthGrow: 2 },
      { title: "To-be IRI", field: "newIri", formatter: "textarea", headerFilter: "input", widthGrow: 3 },
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
      const data = row.getData();
      const el = row.getElement();
      el.classList.remove("mb-rowChange", "mb-rowNoChange");
      if (data.status === "Change") el.classList.add("mb-rowChange");
      else el.classList.add("mb-rowNoChange");
    }
  });
}

/* -----------------------------
   Ontology ingest
------------------------------ */

async function ingestOntology(file) {
  setStatus(`Ingesting ontology: ${file.name} …`);

  const detected = detectOntologyFormat(file.name);
  UI.ontologyFormat.textContent = detected.label;

  const createdAt = new Date().toISOString();
  const runId = makeRunId("input", file.name, createdAt);

  const baseIri = UI.baseIri.value?.trim() || "urn:myna:base:";
  const useNativePrefixes = !!UI.useNativePrefixes.checked;

  const { nquads, prefixes, stats } = await parseOntologyToNQuads({
    file,
    runId,
    baseIri,
  });

  Session.currentOntologyRunId = runId;
  Session.currentOutputRunId = null;
  Session.ontologyPrefixes = prefixes || {};

  UI.ontologyRunId.textContent = runId;
  UI.prefixJson.textContent = JSON.stringify(prefixes || {}, null, 2);

  await putRun({
    runId,
    kind: "input",
    parentRunId: null,
    fileName: file.name,
    createdAt,
    sourceFormat: detected.contentType,
    useNativePrefixes,
    prefixes: prefixes || {},
    nquads,
    stats,
    mapping: null,
  });

  await refreshRunsDropdown();
  await buildPreviewFromRun(runId);

  setStatus(`Ontology ingested. IRIs found: ${stats?.uniqueIris || 0}`);
}

function detectOntologyFormat(fileName) {
  const ext = (fileName.split(".").pop() || "").toLowerCase();
  if (ext === "ttl" || ext === "turtle" || ext === "trig") return { contentType: "text/turtle", label: "Turtle" };
  if (ext === "nt") return { contentType: "application/n-triples", label: "N-Triples" };
  if (ext === "nq") return { contentType: "application/n-quads", label: "N-Quads" };
  if (ext === "jsonld" || ext === "json") return { contentType: "application/ld+json", label: "JSON-LD" };
  if (ext === "rdf" || ext === "owl" || ext === "xml") return { contentType: "application/rdf+xml", label: "RDF/XML (or .owl as RDF/XML)" };
  return { contentType: "application/octet-stream", label: "Unknown (will attempt parsing)" };
}

async function parseOntologyToNQuads({ file, runId, baseIri }) {
  const text = await file.text();
  const ext = (file.name.split(".").pop() || "").toLowerCase();

  // Graph name for this run (all imported statements go into this named graph)
  const DF = N3.DataFactory;
  const graphNode = DF.namedNode(runId);

  // Prefixes (best effort depending on format)
  let prefixes = {};
  let quads = [];

  if (ext === "ttl" || ext === "turtle" || ext === "trig") {
    prefixes = parseTurtlePrefixes(text);
    quads = parseWithN3(text, "text/turtle", baseIri).map(q => DF.quad(q.subject, q.predicate, q.object, graphNode));
  } else if (ext === "nt") {
    quads = parseWithN3(text, "application/n-triples", baseIri).map(q => DF.quad(q.subject, q.predicate, q.object, graphNode));
  } else if (ext === "nq") {
    // If the source is already N-Quads, still re-home quads into this run graph for consistent storage
    const src = parseWithN3(text, "application/n-quads", baseIri);
    quads = src.map(q => DF.quad(q.subject, q.predicate, q.object, graphNode));
  } else if (ext === "jsonld" || ext === "json") {
    const { jsonObj, contextPrefixes } = parseJsonLdPrefixes(text);
    prefixes = contextPrefixes;
    const nquads = await jsonld.toRDF(jsonObj, { format: "application/n-quads" });
    const src = parseWithN3(nquads, "application/n-quads", baseIri);
    quads = src.map(q => DF.quad(q.subject, q.predicate, q.object, graphNode));
  } else if (ext === "rdf" || ext === "owl" || ext === "xml") {
    // Best-effort RDF/XML. If it is true OWL 2 XML Syntax, this will likely fail.
    prefixes = parseXmlnsPrefixes(text);
    const nt = await parseRdfXmlToNTriples(text, baseIri);
    const src = parseWithN3(nt, "application/n-triples", baseIri);
    quads = src.map(q => DF.quad(q.subject, q.predicate, q.object, graphNode));
  } else {
    // Try Turtle first, then RDF/XML
    try {
      prefixes = parseTurtlePrefixes(text);
      quads = parseWithN3(text, "text/turtle", baseIri).map(q => DF.quad(q.subject, q.predicate, q.object, graphNode));
    } catch {
      prefixes = parseXmlnsPrefixes(text);
      const nt = await parseRdfXmlToNTriples(text, baseIri);
      const src = parseWithN3(nt, "application/n-triples", baseIri);
      quads = src.map(q => DF.quad(q.subject, q.predicate, q.object, graphNode));
    }
  }

  const nquadsOut = await quadsToNQuads(quads);
  const stats = computeStatsFromQuads(quads);

  return { nquads: nquadsOut, prefixes, stats };
}

function parseWithN3(text, format, baseIri) {
  const parser = new N3.Parser({ format, baseIRI: baseIri });
  return parser.parse(text);
}

async function quadsToNQuads(quads) {
  const writer = new N3.Writer({ format: "N-Quads" });
  writer.addQuads(quads);
  return new Promise((resolve, reject) => {
    writer.end((err, result) => (err ? reject(err) : resolve(result)));
  });
}

function parseTurtlePrefixes(text) {
  const out = {};
  const re1 = /@prefix\s+([A-Za-z][\w-]*)?:\s*<([^>]+)>\s*\./gi;
  const re2 = /PREFIX\s+([A-Za-z][\w-]*)?:\s*<([^>]+)>/gi;
  let m;
  while ((m = re1.exec(text))) out[m[1] || ""] = m[2];
  while ((m = re2.exec(text))) out[m[1] || ""] = m[2];
  return out;
}

function parseXmlnsPrefixes(xmlText) {
  try {
    const doc = new DOMParser().parseFromString(xmlText, "application/xml");
    const root = doc.documentElement;
    const out = {};
    for (const attr of root.attributes) {
      if (attr.name === "xmlns") out[""] = attr.value;
      if (attr.name.startsWith("xmlns:")) out[attr.name.slice("xmlns:".length)] = attr.value;
    }
    return out;
  } catch {
    return {};
  }
}

function parseJsonLdPrefixes(text) {
  const jsonObj = JSON.parse(text);
  const out = {};
  const ctx = jsonObj?.["@context"];
  if (ctx && typeof ctx === "object" && !Array.isArray(ctx)) {
    for (const [k, v] of Object.entries(ctx)) {
      if (!k.startsWith("@") && typeof v === "string") out[k] = v;
    }
  }
  return { jsonObj, contextPrefixes: out };
}

async function parseRdfXmlToNTriples(xmlText, baseIri) {
  return new Promise((resolve, reject) => {
    try {
      const store = $rdf.graph();
      $rdf.parse(xmlText, store, baseIri, "application/rdf+xml");
      $rdf.serialize(null, store, baseIri, "application/n-triples", (err, str) => {
        if (err) reject(err);
        else resolve(str);
      });
    } catch (e) {
      reject(e);
    }
  });
}

function computeStatsFromQuads(quads) {
  const iris = new Set();
  let total = 0;

  const RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";
  const labelMap = new Map();

  for (const q of quads) {
    total++;

    if (q.subject.termType === "NamedNode") iris.add(q.subject.value);
    if (q.predicate.termType === "NamedNode") iris.add(q.predicate.value);
    if (q.object.termType === "NamedNode") iris.add(q.object.value);

    // labels
    if (q.predicate.termType === "NamedNode" && q.predicate.value === RDFS_LABEL) {
      if (q.subject.termType === "NamedNode" && q.object.termType === "Literal") {
        const cur = labelMap.get(q.subject.value);
        // prefer @en if possible
        if (!cur) labelMap.set(q.subject.value, { value: q.object.value, lang: q.object.language || "" });
        else if (cur.lang !== "en" && (q.object.language || "") === "en") {
          labelMap.set(q.subject.value, { value: q.object.value, lang: "en" });
        }
      }
    }
  }

  return {
    totalQuads: total,
    uniqueIris: iris.size,
    labelCount: labelMap.size
  };
}

/* -----------------------------
   Mapping ingest
------------------------------ */

async function ingestMapping(file) {
  setStatus(`Ingesting mapping: ${file.name} …`);

  const ext = (file.name.split(".").pop() || "").toLowerCase();
  let rows = [];

  if (ext === "csv" || ext === "tsv") {
    const text = await file.text();
    const delim = ext === "tsv" ? "\t" : "";
    const parsed = Papa.parse(text, {
      header: true,
      skipEmptyLines: true,
      delimiter: delim || undefined,
    });
    if (parsed.errors?.length) {
      throw new Error(`CSV parse error: ${parsed.errors[0]?.message || "unknown"}`);
    }
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

  UI.kpiMappingIris.textContent = String(meta.uniqueOld);

  setStatus(`Mapping ingested. Unique old IRIs: ${meta.uniqueOld}`);
}

function rowsToMapping(rows) {
  // Try exact headers first, then fallback by fuzzy key match
  const norm = (s) => String(s || "").trim().toLowerCase();

  let oldKey = null;
  let newKey = null;

  if (rows.length > 0) {
    const keys = Object.keys(rows[0] || {});
    for (const k of keys) {
      const nk = norm(k);
      if (nk === "old iri") oldKey = k;
      if (nk === "new iri") newKey = k;
    }
    // fallback: contains patterns
    for (const k of keys) {
      const nk = norm(k);
      if (!oldKey && nk.includes("old") && nk.includes("iri")) oldKey = k;
      if (!newKey && nk.includes("new") && nk.includes("iri")) newKey = k;
    }
  }

  if (!oldKey || !newKey) {
    throw new Error(`Mapping file must have headers like "Old IRI" and "New IRI". Found: ${rows.length ? Object.keys(rows[0]).join(", ") : "(no rows)"}`);
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

    // Keep last occurrence (common spreadsheet behavior)
    mapping.set(oldIri, newIri);
  }

  return {
    mapping,
    meta: {
      rows: rows.length,
      uniqueOld: mapping.size,
      dupOld
    }
  };
}

/* -----------------------------
   Preview table
------------------------------ */

async function buildPreviewFromRun(runId) {
  const run = await getRun(runId);
  if (!run) return setStatus("Run not found in IndexedDB.", true);

  Session.currentOntologyRunId = run.kind === "input" ? runId : (run.parentRunId || runId);
  Session.currentOutputRunId = run.kind === "output" ? runId : null;

  Session.ontologyPrefixes = run.prefixes || {};
  UI.prefixJson.textContent = JSON.stringify(Session.ontologyPrefixes, null, 2);

  const quads = parseWithN3(run.nquads, "application/n-quads", UI.baseIri.value || "urn:myna:base:");
  const { rows, proposedChanges, uniqueIris } = buildRowsFromQuads(quads, Session.mapping);

  table.replaceData(rows);

  UI.kpiOntologyIris.textContent = String(uniqueIris);
  UI.kpiProposedChanges.textContent = String(proposedChanges);
  UI.kpiPctChanged.textContent = uniqueIris ? `${Math.round((proposedChanges / uniqueIris) * 100)}%` : "0%";

  setStatus(`Preview built for run: ${runId}`);
}

function buildRowsFromQuads(quads, mapping) {
  const iris = new Set();
  const RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label";
  const labelMap = new Map();

  for (const q of quads) {
    if (q.subject.termType === "NamedNode") iris.add(q.subject.value);
    if (q.predicate.termType === "NamedNode") iris.add(q.predicate.value);
    if (q.object.termType === "NamedNode") iris.add(q.object.value);

    if (q.predicate.termType === "NamedNode" && q.predicate.value === RDFS_LABEL) {
      if (q.subject.termType === "NamedNode" && q.object.termType === "Literal") {
        const cur = labelMap.get(q.subject.value);
        const lang = q.object.language || "";
        if (!cur) labelMap.set(q.subject.value, { value: q.object.value, lang });
        else if (cur.lang !== "en" && lang === "en") labelMap.set(q.subject.value, { value: q.object.value, lang: "en" });
      }
    }
  }

  let proposedChanges = 0;
  const rows = [];
  const sorted = Array.from(iris).sort((a, b) => a.localeCompare(b));

  for (const iri of sorted) {
    const lbl = labelMap.get(iri)?.value || "";
    const mapped = mapping?.has(iri) ? (mapping.get(iri) || "") : "";
    const isChange = !!mapped && mapped !== iri;

    if (isChange) proposedChanges++;

    rows.push({
      iri,
      label: lbl,
      newIri: isChange ? mapped : "",
      status: isChange ? "Change" : "No change",
    });
  }

  return { rows, proposedChanges, uniqueIris: iris.size };
}

/* -----------------------------
   Apply mapping → output run
------------------------------ */

async function applyMappingToCurrentOntology() {
  const inputRun = await getRun(Session.currentOntologyRunId);
  if (!inputRun) return setStatus("Input run not found.", true);

  const createdAt = new Date().toISOString();
  const outputRunId = makeRunId("output", inputRun.fileName, createdAt);

  const baseIri = UI.baseIri.value?.trim() || "urn:myna:base:";
  const useNativePrefixes = !!UI.useNativePrefixes.checked;

  const inputQuads = parseWithN3(inputRun.nquads, "application/n-quads", baseIri);
  const { outputQuads, changeStats } = rewriteQuads(inputQuads, Session.mapping, outputRunId);

  const nquads = await quadsToNQuads(outputQuads);

  await putRun({
    runId: outputRunId,
    kind: "output",
    parentRunId: inputRun.runId,
    fileName: outputFileName(inputRun.fileName),
    createdAt,
    sourceFormat: inputRun.sourceFormat,
    useNativePrefixes,
    prefixes: inputRun.prefixes || {},
    nquads,
    stats: {
      ...computeStatsFromQuads(outputQuads),
      changeStats
    },
    mapping: {
      rows: Session.mappingMeta.rows,
      uniqueOld: Session.mappingMeta.uniqueOld,
      dupOld: Session.mappingMeta.dupOld,
      // Store mapping pairs for reproducibility (can be large; remove if you prefer)
      pairs: Array.from(Session.mapping.entries())
    }
  });

  Session.currentOutputRunId = outputRunId;

  await refreshRunsDropdown(outputRunId);
  await buildPreviewFromRun(inputRun.runId); // show staged changes vs input
  await renderOutputPreview(outputRunId);

  // show “% ontology changed” as quad-level impact too
  const pctQuads = changeStats.totalQuads ? Math.round((changeStats.quadsTouched / changeStats.totalQuads) * 100) : 0;
  setStatus(`Output run created. Quads touched: ${changeStats.quadsTouched}/${changeStats.totalQuads} (~${pctQuads}%).`);
}

function rewriteQuads(inputQuads, mapping, outputRunId) {
  const DF = N3.DataFactory;
  const outGraph = DF.namedNode(outputRunId);

  let totalQuads = 0;
  let quadsTouched = 0;
  let termReplacements = 0;

  const outputQuads = inputQuads.map(q => {
    totalQuads++;

    let touched = false;

    const s = replaceIfMapped(q.subject, mapping); if (s !== q.subject) touched = true;
    const p = replaceIfMapped(q.predicate, mapping); if (p !== q.predicate) touched = true;
    const o = replaceIfMapped(q.object, mapping); if (o !== q.object) touched = true;

    if (touched) {
      quadsTouched++;
      // count replacements conservatively as number of terms changed
      if (s !== q.subject) termReplacements++;
      if (p !== q.predicate) termReplacements++;
      if (o !== q.object) termReplacements++;
    }

    return DF.quad(s, p, o, outGraph);
  });

  return { outputQuads, changeStats: { totalQuads, quadsTouched, termReplacements } };
}

function replaceIfMapped(term, mapping) {
  if (term.termType !== "NamedNode") return term;
  const next = mapping.get(term.value);
  if (!next || next === term.value) return term;
  return N3.DataFactory.namedNode(next);
}

function outputFileName(inputName) {
  const idx = inputName.lastIndexOf(".");
  if (idx <= 0) return `${inputName}.mapped`;
  return `${inputName.slice(0, idx)}.mapped${inputName.slice(idx)}`;
}

/* -----------------------------
   Export / Preview
------------------------------ */

async function renderOutputPreview(runId) {
  try {
    const ttl = await serializeRun(runId, "text/turtle");
    UI.outputPreview.value = ttl;
  } catch (e) {
    UI.outputPreview.value = "";
    setStatus(`Preview error: ${e?.message || e}`, true);
  }
}

async function downloadRun(runId, contentType) {
  const body = await serializeRun(runId, contentType);
  const run = await getRun(runId);
  const fileNameBase = (run?.fileName || "ontology.mapped");

  const ext = contentTypeToExt(contentType);
  const outName = ensureExt(fileNameBase, ext);

  const blob = new Blob([body], { type: contentType });
  const url = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = url;
  a.download = outName;
  a.click();

  setTimeout(() => URL.revokeObjectURL(url), 1500);
  setStatus(`Downloaded: ${outName}`);
}

function contentTypeToExt(ct) {
  if (ct === "text/turtle") return ".ttl";
  if (ct === "application/n-triples") return ".nt";
  if (ct === "application/rdf+xml") return ".rdf";
  if (ct === "application/ld+json") return ".jsonld";
  return ".txt";
}

function ensureExt(name, ext) {
  const lower = name.toLowerCase();
  if (lower.endsWith(ext)) return name;
  // strip common rdf extensions then add
  return name.replace(/\.(ttl|nt|nq|trig|rdf|owl|xml|jsonld|json)$/i, "") + ext;
}

async function serializeRun(runId, contentType) {
  const run = await getRun(runId);
  if (!run) throw new Error("Run not found.");

  const baseIri = UI.baseIri.value?.trim() || "urn:myna:base:";
  const useNativePrefixes = !!UI.useNativePrefixes.checked;
  const prefixes = (useNativePrefixes ? (run.prefixes || {}) : {});

  const quads = parseWithN3(run.nquads, "application/n-quads", baseIri);

  // Convert quads -> triples (drop graph) for ontology-style export
  const triples = quads.map(q => N3.DataFactory.quad(q.subject, q.predicate, q.object));

  if (contentType === "text/turtle") {
    return writeWithN3(triples, "Turtle", prefixes);
  }

  if (contentType === "application/n-triples") {
    return writeWithN3(triples, "N-Triples", {});
  }

  if (contentType === "application/rdf+xml") {
    const nt = await writeWithN3(triples, "N-Triples", {});
    return serializeNTriplesToRdfXml(nt, baseIri, prefixes);
  }

  if (contentType === "application/ld+json") {
    const nquads = await quadsToNQuads(quads);
    const doc = await jsonld.fromRDF(nquads, { format: "application/n-quads" });

    if (useNativePrefixes && prefixes && Object.keys(prefixes).length) {
      const ctx = prefixesToJsonLdContext(prefixes);
      const compacted = await jsonld.compact(doc, ctx);
      return JSON.stringify(compacted, null, 2);
    }

    return JSON.stringify(doc, null, 2);
  }

  throw new Error(`Unsupported export contentType: ${contentType}`);
}

function prefixesToJsonLdContext(prefixes) {
  const ctx = {};
  for (const [k, v] of Object.entries(prefixes)) {
    if (k === "") continue; // default prefix isn't valid as a JSON-LD term key
    ctx[k] = v;
  }
  return { "@context": ctx };
}

async function writeWithN3(triples, format, prefixes) {
  const writer = new N3.Writer({ format, prefixes });
  writer.addQuads(triples);
  return new Promise((resolve, reject) => {
    writer.end((err, result) => (err ? reject(err) : resolve(result)));
  });
}

async function serializeNTriplesToRdfXml(ntriples, baseIri, prefixes) {
  return new Promise((resolve, reject) => {
    try {
      const store = $rdf.graph();
      // best effort prefix binding (rdflib.js provides setPrefixForURI on the store in many builds)
      if (typeof store.setPrefixForURI === "function") {
        for (const [pfx, ns] of Object.entries(prefixes || {})) {
          if (!pfx) continue;
          store.setPrefixForURI(pfx, ns);
        }
      }

      $rdf.parse(ntriples, store, baseIri, "application/n-triples");
      $rdf.serialize(null, store, baseIri, "application/rdf+xml", (err, str) => {
        if (err) reject(err);
        else resolve(str);
      });
    } catch (e) {
      reject(e);
    }
  });
}

/* -----------------------------
   Runs (IndexedDB)
------------------------------ */

function makeRunId(kind, fileName, iso) {
  const safe = fileName.replace(/[^\w.-]+/g, "_");
  return `urn:myna:${kind}:${safe}:${iso}`;
}

async function openDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(APP.dbName, APP.dbVersion);
    req.onerror = () => reject(req.error);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(APP.storeRuns)) {
        const store = db.createObjectStore(APP.storeRuns, { keyPath: "runId" });
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
    const tx = db.transaction(APP.storeRuns, "readwrite");
    const store = tx.objectStore(APP.storeRuns);
    store.put(run);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function getRun(runId) {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(APP.storeRuns, "readonly");
    const store = tx.objectStore(APP.storeRuns);
    const req = store.get(runId);
    req.onsuccess = () => resolve(req.result || null);
    req.onerror = () => reject(req.error);
  });
}

async function listRuns() {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(APP.storeRuns, "readonly");
    const store = tx.objectStore(APP.storeRuns);
    const req = store.getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
}

async function deleteRun(runId) {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(APP.storeRuns, "readwrite");
    const store = tx.objectStore(APP.storeRuns);
    store.delete(runId);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function clearAllRuns() {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(APP.storeRuns, "readwrite");
    const store = tx.objectStore(APP.storeRuns);
    store.clear();
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
    const stamp = r.createdAt?.replace("T", " ").replace("Z", "");
    opt.textContent = `[${r.kind}] ${stamp} — ${r.fileName}`;
    UI.runsSelect.appendChild(opt);
  }

  if (selectRunId) UI.runsSelect.value = selectRunId;
  else if (runs.length) UI.runsSelect.value = runs[0].runId;
}

async function loadRun(runId) {
  const run = await getRun(runId);
  if (!run) return setStatus("Run not found.", true);

  // set “current” pointers
  if (run.kind === "input") {
    Session.currentOntologyRunId = run.runId;
    Session.currentOutputRunId = null;
  } else {
    Session.currentOutputRunId = run.runId;
    Session.currentOntologyRunId = run.parentRunId || run.runId;
  }

  UI.ontologyRunId.textContent = Session.currentOntologyRunId || "—";
  UI.ontologyFormat.textContent = run.sourceFormat || "—";
  UI.prefixJson.textContent = JSON.stringify(run.prefixes || {}, null, 2);

  await buildPreviewFromRun(Session.currentOntologyRunId);

  if (Session.currentOutputRunId) {
    await renderOutputPreview(Session.currentOutputRunId);
  } else {
    UI.outputPreview.value = "";
  }

  setStatus(`Loaded run: ${runId}`);
}

/* -----------------------------
   Service worker
------------------------------ */

async function registerServiceWorker() {
  if (!("serviceWorker" in navigator)) return;
  try {
    await navigator.serviceWorker.register("./sw.js");
  } catch (e) {
    // Non-fatal; still usable online
    setStatus(`Service worker registration failed: ${e?.message || e}`, true);
  }
}

/* -----------------------------
   Utilities
------------------------------ */

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
  console.log(isError ? "[myna:error]" : "[myna]", msg);
}

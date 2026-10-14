import {
  PROJECT_RECORD_JSONLD_CONTEXT,
  openProjectPortfolioDatabase,
  createProjectPortfolioStores,
  ensureProjectPortfolioProject,
  DEFAULT_PROJECT_PORTFOLIO_PROJECT_ID
} from './shared/indexeddb-data-management/index.js';
import { COMMON_NAMESPACE_IRIS } from './shared/namespace-registry/index.js';

const IRI_SWAPPER_PROJECT_ID = DEFAULT_PROJECT_PORTFOLIO_PROJECT_ID;
const IRI_SWAPPER_APP_ID = 'iri-swapper';

let portfolioPromise = null;

/**
 * Opens the shared project portfolio and returns stores used by IRI Swapper.
 *
 * @returns {Promise<{db: IDBDatabase, runs: object, artifacts: object}>}
 */
async function openIriSwapperPortfolio() {
  if (!portfolioPromise) {
    portfolioPromise = openProjectPortfolioDatabase().then(async (db) => {
      const stores = createProjectPortfolioStores(db);
      await ensureProjectPortfolioProject(stores, {
        projectId: IRI_SWAPPER_PROJECT_ID,
        label: 'Default Project',
        storageBackend: 'indexeddb'
      });
      return { db, runs: stores.runs, artifacts: stores.artifacts };
    });
  }
  return portfolioPromise;
}

/**
 * Creates a deterministic run id matching the existing IRI Swapper UI shape.
 *
 * @param {string} domain
 * @param {string} kind
 * @param {string} fileName
 * @param {string} iso
 * @returns {string}
 */
export function createIriSwapperRunId(domain, kind, fileName, iso) {
  const safe = String(fileName || 'run').replace(/[^\w.-]+/g, '_');
  const middle = domain ? `${domain}:` : '';
  return `urn:myna:${middle}${kind}:${safe}:${iso}`;
}

/**
 * Stores an IRI Swapper run as a shared project run record while preserving the
 * legacy run payload fields used by the current page.
 *
 * @param {object} run
 * @param {object} [options]
 * @param {string} [options.runKind]
 * @returns {Promise<object>}
 */
export async function storeIriSwapperRun(run, { runKind = 'rdf-iri-rewrite' } = {}) {
  const { runs } = await openIriSwapperPortfolio();
  const runId = String(run?.runId || '');
  await runs.storeRunRecord({
    runId,
    projectId: IRI_SWAPPER_PROJECT_ID,
    runKind,
    label: String(run?.fileName || runId || 'IRI Swapper run'),
    createdAt: run?.createdAt || new Date().toISOString(),
    inputArtifactIds: [],
    outputArtifactIds: [],
    payload: convertIriSwapperRunToJsonLd(run, { runKind }),
    metadata: {
      [COMMON_NAMESPACE_IRIS.okea.appId]: IRI_SWAPPER_APP_ID
    },
    uiState: null
  });
  return run;
}

/**
 * Reads an IRI Swapper run by id.
 *
 * @param {string} runId
 * @returns {Promise<object|null>}
 */
export async function readIriSwapperRun(runId) {
  if (!runId) return null;
  const { runs } = await openIriSwapperPortfolio();
  const record = await runs.getRunRecord(runId);
  return readIriSwapperRunFromJsonLd(record?.payload) || null;
}

/**
 * Lists IRI Swapper runs for one run kind.
 *
 * @param {object} [options]
 * @param {string} [options.runKind]
 * @returns {Promise<object[]>}
 */
export async function listIriSwapperRuns({ runKind = null } = {}) {
  const { runs } = await openIriSwapperPortfolio();
  const records = await runs.listRunRecords({
    projectId: IRI_SWAPPER_PROJECT_ID,
    ...(runKind ? { runKind } : {})
  });
  return records.map((record) => readIriSwapperRunFromJsonLd(record.payload)).filter(Boolean);
}

/**
 * Deletes one IRI Swapper run.
 *
 * @param {string} runId
 * @returns {Promise<boolean>}
 */
export async function deleteIriSwapperRun(runId) {
  if (!runId) return false;
  const { runs } = await openIriSwapperPortfolio();
  return runs.deleteRunRecord(runId);
}

/**
 * Deletes IRI Swapper runs matching the supplied kind.
 *
 * @param {object} [options]
 * @param {string} [options.runKind]
 * @returns {Promise<number>}
 */
export async function clearIriSwapperRuns({ runKind = null } = {}) {
  const { runs } = await openIriSwapperPortfolio();
  const records = await runs.listRunRecords({
    projectId: IRI_SWAPPER_PROJECT_ID,
    ...(runKind ? { runKind } : {})
  });
  await Promise.all(records.map((record) => runs.deleteRunRecord(record.runId)));
  return records.length;
}

/**
 * Creates a JSON-LD literal for stored string values.
 *
 * @param {unknown} value Source value.
 * @returns {{'@value': string, '@type': string}|null} JSON-LD literal.
 */
function createJsonLdStringLiteral(value) {
  if (value === null || value === undefined || value === '') return null;
  return {
    '@value': String(value),
    '@type': COMMON_NAMESPACE_IRIS.xsd.string
  };
}

/**
 * Creates a JSON-LD date-time literal.
 *
 * @param {unknown} value Source ISO timestamp.
 * @returns {{'@value': string, '@type': string}|null} JSON-LD date-time literal.
 */
function createJsonLdDateTimeLiteral(value) {
  if (!value) return null;
  return {
    '@value': String(value),
    '@type': COMMON_NAMESPACE_IRIS.xsd.dateTime
  };
}

/**
 * Reads a scalar value from a JSON-LD literal, IRI reference, or legacy value.
 *
 * @param {object} node JSON-LD object.
 * @param {string} iri Full property IRI.
 * @param {unknown} [fallback=''] Fallback value.
 * @returns {unknown} Resolved scalar.
 */
function readJsonLdScalarValueForIri(node, iri, fallback = '') {
  const value = node?.[iri];
  if (value && typeof value === 'object' && !Array.isArray(value) && '@value' in value) return value['@value'];
  if (value && typeof value === 'object' && !Array.isArray(value) && '@id' in value) return value['@id'];
  return value ?? fallback;
}

/**
 * Converts an IRI Swapper run DTO to a registry-backed JSON-LD envelope for
 * durable project-portfolio storage.
 *
 * The nested `rdf:value` member preserves the existing RDF/SPARQL page DTO so
 * current preview, apply, and download handlers can continue to share one read
 * path while the durable envelope follows the semantic model.
 *
 * @param {object} run IRI Swapper run DTO.
 * @param {object} [options]
 * @param {string} [options.runKind='rdf-iri-rewrite'] Shared run kind.
 * @returns {object} JSON-LD run payload.
 */
export function convertIriSwapperRunToJsonLd(run, { runKind = 'rdf-iri-rewrite' } = {}) {
  const runId = String(run?.runId || '');
  const createdAt = run?.createdAt || new Date().toISOString();
  const fileName = String(run?.fileName || runId || 'IRI Swapper run');
  return {
    '@context': PROJECT_RECORD_JSONLD_CONTEXT,
    '@id': runId,
    '@type': COMMON_NAMESPACE_IRIS.cceo.ComputerProgramExecution,
    [COMMON_NAMESPACE_IRIS.dcterms.identifier]: createJsonLdStringLiteral(runId),
    [COMMON_NAMESPACE_IRIS.dcterms.title]: createJsonLdStringLiteral(fileName),
    [COMMON_NAMESPACE_IRIS.dcterms.created]: createJsonLdDateTimeLiteral(createdAt),
    [COMMON_NAMESPACE_IRIS.dcterms.format]: createJsonLdStringLiteral(run?.sourceFormat || run?.mimeType || ''),
    [COMMON_NAMESPACE_IRIS.okea.fileName]: createJsonLdStringLiteral(fileName),
    [COMMON_NAMESPACE_IRIS.okea.role]: createJsonLdStringLiteral(run?.kind || ''),
    [COMMON_NAMESPACE_IRIS.okea.runKind]: createJsonLdStringLiteral(runKind),
    [COMMON_NAMESPACE_IRIS.rdf.value]: {
      ...run,
      runId,
      fileName,
      createdAt
    }
  };
}

/**
 * Reads an IRI Swapper app-facing run DTO from the JSON-LD storage envelope.
 * Legacy payloads are returned unchanged for existing browser sessions.
 *
 * @param {object|null|undefined} payload Stored run payload.
 * @returns {object|null} App-facing run DTO.
 */
export function readIriSwapperRunFromJsonLd(payload) {
  if (!payload || typeof payload !== 'object') return null;
  if (!payload['@context']) return payload;

  const value = payload[COMMON_NAMESPACE_IRIS.rdf.value] || {};
  return {
    ...value,
    runId: value.runId || payload['@id'] || '',
    fileName: value.fileName || readJsonLdScalarValueForIri(payload, COMMON_NAMESPACE_IRIS.okea.fileName, ''),
    createdAt: value.createdAt || readJsonLdScalarValueForIri(payload, COMMON_NAMESPACE_IRIS.dcterms.created, ''),
    kind: value.kind || readJsonLdScalarValueForIri(payload, COMMON_NAMESPACE_IRIS.okea.role, ''),
    sourceFormat: value.sourceFormat || readJsonLdScalarValueForIri(payload, COMMON_NAMESPACE_IRIS.dcterms.format, '')
  };
}

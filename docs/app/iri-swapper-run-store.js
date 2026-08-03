import {
  openProjectPortfolioDatabase,
  createProjectPortfolioStores,
  ensureProjectPortfolioProject,
  DEFAULT_PROJECT_PORTFOLIO_PROJECT_ID
} from './shared/indexeddb-data-management/index.js';

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
    payload: {
      ...run,
      appId: IRI_SWAPPER_APP_ID
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
  return record?.payload || null;
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
  return records.map((record) => record.payload).filter(Boolean);
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

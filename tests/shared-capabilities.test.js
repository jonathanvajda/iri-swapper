import {
  getPreferredExtensionForMimeType,
  getSupportedMimeTypeForFilename,
  normalizeSupportedMimeType
} from '../docs/app/shared/format-registry/mime-registry.js';
import { createIriMappingFromRows } from '../docs/app/shared/tabular-io/iri-mapping.js';
import {
  COMMON_NAMESPACE_IRIS,
  iriForNamespaceId,
  namespacePrefixMapFromRegistry
} from '../docs/app/shared/namespace-registry/namespace-registry.js';
import {
  convertIriSwapperRunToJsonLd,
  createIriSwapperRunId,
  readIriSwapperRunFromJsonLd
} from '../docs/app/iri-swapper-run-store.js';
import {
  buildSparqlIriPreviewRows,
  countSparqlAppliedChanges,
  extractSparqlIriTokens,
  parsePrefixesAndBase,
  rewriteSparqlQuery
} from '../docs/app/sparql-iri-swapper-core.js';
import {
  RDF_GRAPH_EXPORT_MIME_TYPES,
  getRdfGraphExportGraphShape,
  isSupportedRdfGraphExportMimeType
} from '../docs/app/shared/rdf-io/index.js';
import {
  DEFAULT_PROJECT_PORTFOLIO_PROJECT_ID,
  createMemoryRecordAdapter,
  createRunRecordStore,
  downloadRunOutputForExport
} from '../docs/app/shared/indexeddb-data-management/index.js';

describe('IRI mapping rows', () => {
  test('creates a normalized old-to-new IRI map from common headers', () => {
    const result = createIriMappingFromRows([
      { 'Old IRI': '<http://example.test/old-a>', 'New IRI': 'http://example.test/new-a' },
      { 'Old IRI': 'http://example.test/old-b', 'New IRI': '<http://example.test/new-b>' }
    ]);

    expect([...result.mapping.entries()]).toEqual([
      ['http://example.test/old-a', 'http://example.test/new-a'],
      ['http://example.test/old-b', 'http://example.test/new-b']
    ]);
    expect(result.meta).toEqual({
      rows: 2,
      uniqueOld: 2,
      duplicateOld: 0,
      skippedRows: 0
    });
    expect(result.warnings).toEqual([]);
  });

  test('reports skipped rows and duplicate conflicts with the selected policy', () => {
    const result = createIriMappingFromRows([
      { source_iri: 'http://example.test/a', target_iri: 'http://example.test/b' },
      { source_iri: 'http://example.test/a', target_iri: 'http://example.test/c' },
      { source_iri: 'http://example.test/missing-target', target_iri: '' }
    ], {
      oldIriHeaders: ['source iri'],
      newIriHeaders: ['target iri'],
      duplicatePolicy: 'first'
    });

    expect(result.mapping.get('http://example.test/a')).toBe('http://example.test/b');
    expect(result.meta).toEqual({
      rows: 3,
      uniqueOld: 1,
      duplicateOld: 1,
      skippedRows: 1
    });
    expect(result.warnings.map((warning) => warning.code)).toEqual([
      'conflicting_mapping',
      'missing_mapping_value'
    ]);
  });

  test('throws when required mapping columns are absent or conflicts are disallowed', () => {
    expect(() => createIriMappingFromRows([{ old: 'a', replacement: 'b' }]))
      .toThrow('Mapping rows must include old and new IRI columns');

    expect(() => createIriMappingFromRows([
      { 'old iri': 'http://example.test/a', 'new iri': 'http://example.test/b' },
      { 'old iri': 'http://example.test/a', 'new iri': 'http://example.test/c' }
    ], { duplicatePolicy: 'error' })).toThrow('Conflicting mapping');
  });
});

describe('format registry', () => {
  test('detects supported MIME descriptors from filenames and aliases', () => {
    expect(getSupportedMimeTypeForFilename('ontology.ttl')).toMatchObject({
      ok: true,
      value: { id: 'turtle', mimeType: 'text/turtle', category: 'rdf' }
    });
    expect(normalizeSupportedMimeType('jsonld')).toMatchObject({
      ok: true,
      value: { id: 'jsonLd', mimeType: 'application/ld+json' }
    });
    expect(getPreferredExtensionForMimeType('application/sparql-query')).toEqual({
      ok: true,
      value: 'rq'
    });
  });

  test('returns structured unknown-filetype results for unsupported inputs', () => {
    expect(getSupportedMimeTypeForFilename('notes.unknown')).toEqual({
      ok: false,
      error: 'unknown filetype',
      input: 'notes.unknown',
      extension: 'unknown'
    });
  });
});

describe('promoted RDF export options', () => {
  test('covers every RDF export format offered by the page', () => {
    expect(RDF_GRAPH_EXPORT_MIME_TYPES).toEqual([
      'text/turtle',
      'application/n-triples',
      'application/n-quads',
      'application/trig',
      'application/rdf+xml',
      'application/ld+json'
    ]);
    for (const mimeType of RDF_GRAPH_EXPORT_MIME_TYPES) {
      expect(isSupportedRdfGraphExportMimeType(mimeType)).toBe(true);
      expect(getPreferredExtensionForMimeType(mimeType).ok).toBe(true);
    }
  });

  test('distinguishes ontology-style triple exports from graph-preserving exports', () => {
    expect(getRdfGraphExportGraphShape('text/turtle')).toBe('triples');
    expect(getRdfGraphExportGraphShape('application/n-triples')).toBe('triples');
    expect(getRdfGraphExportGraphShape('application/rdf+xml')).toBe('triples');
    expect(getRdfGraphExportGraphShape('application/n-quads')).toBe('quads');
    expect(getRdfGraphExportGraphShape('application/trig')).toBe('quads');
    expect(getRdfGraphExportGraphShape('application/ld+json')).toBe('quads');
  });

  test('rejects unsupported RDF export MIME types explicitly', () => {
    expect(isSupportedRdfGraphExportMimeType('text/plain')).toBe(false);
    expect(() => getRdfGraphExportGraphShape('text/plain')).toThrow('Unsupported RDF graph export MIME type');
  });
});

describe('namespace registry', () => {
  test('provides common ontology IRIs and prefix maps without local constants', () => {
    expect(COMMON_NAMESPACE_IRIS.rdf.type).toBe('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
    expect(COMMON_NAMESPACE_IRIS.owl.equivalentClass).toBe('http://www.w3.org/2002/07/owl#equivalentClass');
    expect(COMMON_NAMESPACE_IRIS.xsd.boolean).toBe('http://www.w3.org/2001/XMLSchema#boolean');

    expect(namespacePrefixMapFromRegistry()).toMatchObject({
      rdf: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
      owl: 'http://www.w3.org/2002/07/owl#',
      xsd: 'http://www.w3.org/2001/XMLSchema#'
    });
  });

  test('builds registry IRIs and reports missing namespace IDs explicitly', () => {
    expect(iriForNamespaceId('skos', 'prefLabel')).toEqual({
      ok: true,
      value: 'http://www.w3.org/2004/02/skos/core#prefLabel'
    });
    expect(iriForNamespaceId('owl', 'notReal')).toEqual({
      ok: false,
      error: 'unknown namespace id',
      input: 'notReal'
    });
  });
});

describe('SPARQL IRI rewrite pipeline', () => {
  test('uses the same expanded IRI mapping for preview, apply, and download output', async () => {
    const queryText = [
      'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>',
      'PREFIX owl: <http://www.w3.org/2002/07/owl#>',
      '',
      'SELECT ?s ?label WHERE {',
      '  ?s a owl:AnnotationProperty ;',
      '     rdfs:label ?label .',
      '}',
      ''
    ].join('\n');
    const mapping = new Map([
      ['http://www.w3.org/2000/01/rdf-schema#label', 'http://www.w3.org/2000/01/rdf-schema#name']
    ]);
    const { prefixes } = parsePrefixesAndBase(queryText);
    const tokens = extractSparqlIriTokens(queryText, prefixes);
    const inputRun = {
      runId: 'run:sparql-input',
      kind: 'input',
      fileName: 'query.rq',
      queryText,
      prefixes,
      tokens
    };

    const preview = buildSparqlIriPreviewRows(inputRun, mapping);
    expect(preview.rows).toEqual(expect.arrayContaining([
      expect.objectContaining({
        token: 'rdfs:label',
        expanded: 'http://www.w3.org/2000/01/rdf-schema#label',
        toBe: 'http://www.w3.org/2000/01/rdf-schema#name',
        status: 'Change'
      })
    ]));

    const outputText = rewriteSparqlQuery(queryText, prefixes, mapping, true);
    expect(outputText).toContain('rdfs:name ?label');
    expect(outputText).not.toContain('rdfs:label ?label');
    expect(countSparqlAppliedChanges(inputRun, outputText, mapping, { useNativePrefixes: true })).toBe(1);

    const downloads = [];
    const result = await downloadRunOutputForExport({
      runId: 'run:sparql-output',
      kind: 'output',
      fileName: 'query.mapped.rq',
      queryText: outputText
    }, {
      mimeType: 'application/sparql-query',
      textProperty: 'queryText',
      downloadTextFile(fileName, text, options) {
        downloads.push({ fileName, text, options });
        return { fileName };
      }
    });

    expect(result.serialized.text).toContain('rdfs:name ?label');
    expect(downloads[0]).toMatchObject({
      fileName: 'query.mapped.rq',
      text: expect.stringContaining('rdfs:name ?label'),
      options: { mimeType: 'application/sparql-query' }
    });
  });
});

describe('project portfolio run storage', () => {
  test('creates IRI Swapper RDF and SPARQL run ids with existing conventions', () => {
    expect(createIriSwapperRunId('', 'input', 'source ontology.ttl', '2026-08-02T12:00:00.000Z'))
      .toBe('urn:myna:input:source_ontology.ttl:2026-08-02T12:00:00.000Z');
    expect(createIriSwapperRunId('sparql', 'output', 'query file.rq', '2026-08-02T12:00:00.000Z'))
      .toBe('urn:myna:sparql:output:query_file.rq:2026-08-02T12:00:00.000Z');
  });

  test('stores migrated IRI Swapper run payloads as registry-backed JSON-LD records', async () => {
    const adapter = createMemoryRecordAdapter();
    const runs = createRunRecordStore(adapter);
    const payload = convertIriSwapperRunToJsonLd({
      runId: 'urn:myna:input:source.ttl:2026-08-02T12:00:00.000Z',
      kind: 'input',
      fileName: 'source.ttl',
      createdAt: '2026-08-02T12:00:00.000Z',
      sourceFormat: 'text/turtle',
      nquads: '<s> <p> <o> .'
    }, { runKind: 'rdf-iri-rewrite' });

    await runs.storeRunRecord({
      runId: 'urn:myna:input:source.ttl:2026-08-02T12:00:00.000Z',
      projectId: DEFAULT_PROJECT_PORTFOLIO_PROJECT_ID,
      runKind: 'rdf-iri-rewrite',
      label: 'source.ttl',
      createdAt: '2026-08-02T12:00:00.000Z',
      payload
    });

    const [rawRecord] = [...adapter.snapshot().values()];
    expect(rawRecord.payload['@context']).toBeTruthy();
    expect(rawRecord.payload[COMMON_NAMESPACE_IRIS.okea.fileName]['@value']).toBe('source.ttl');
    expect(rawRecord.payload[COMMON_NAMESPACE_IRIS.rdf.value].nquads).toBe('<s> <p> <o> .');

    const [record] = await runs.listRunRecords({
      projectId: DEFAULT_PROJECT_PORTFOLIO_PROJECT_ID,
      runKind: 'rdf-iri-rewrite'
    });
    const roundTrip = readIriSwapperRunFromJsonLd(record.payload);
    expect(roundTrip.fileName).toBe('source.ttl');
    expect(roundTrip.kind).toBe('input');
  });
});

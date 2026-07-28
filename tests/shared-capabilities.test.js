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

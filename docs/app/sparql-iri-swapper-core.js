import { extractSparqlPrefixesFromText } from './shared/namespace-registry/sparql-prefixes.js';
import { expandCurieToIri, compactIriToCurie, findLongestPrefixMatch } from './shared/namespace-registry/curie.js';

export function parsePrefixesAndBase(queryText) {
  const extracted = extractSparqlPrefixesFromText(queryText);
  return { prefixes: extracted.prefixes, baseIri: extracted.baseIri };
}

/**
 * Extracts query-body IRI tokens that can be rewritten.
 *
 * PREFIX and BASE declarations are metadata. They are saved on the run and
 * used for expansion/rewrite, but they are not staged table rows.
 *
 * @param {string} queryText SPARQL query text.
 * @param {Record<string, string>} prefixes Prefix map.
 * @returns {Array<{token: string, kind: 'IRIRef'|'PrefixedName', expanded: string}>}
 */
export function extractSparqlIriTokens(queryText, prefixes) {
  const staged = new Map();
  const scan = scanSparql(stripSparqlPrologueDeclarationLines(queryText));

  for (const iri of scan.iriRefs) {
    staged.set(`IRIRef|<${iri}>|${iri}`, {
      token: `<${iri}>`,
      kind: 'IRIRef',
      expanded: iri
    });
  }

  for (const prefixedName of scan.prefixedNames) {
    const expanded = expandPrefixedName(prefixedName, prefixes);
    if (!expanded) continue;
    staged.set(`PrefixedName|${prefixedName}|${expanded}`, {
      token: prefixedName,
      kind: 'PrefixedName',
      expanded
    });
  }

  return Array.from(staged.values()).sort((left, right) => (left.expanded || '').localeCompare(right.expanded || ''));
}

export function buildSparqlIriPreviewRows(run, mapping) {
  const prefixes = run.prefixes || {};
  const tokens = run.tokens || [];
  const prefixNsToBe = {};

  for (const [prefix, namespaceIri] of Object.entries(prefixes)) {
    const mapped = mapping.get(namespaceIri);
    if (mapped && mapped !== namespaceIri) prefixNsToBe[prefix] = mapped;
  }

  const rows = [];
  let proposed = 0;

  for (const token of tokens) {
    const expanded = token.expanded || '';
    let toBe = '';
    let status = 'No change';

    if (token.kind === 'IRIRef') {
      const mapped = mapping.get(expanded);
      if (mapped && mapped !== expanded) toBe = mapped;
    } else if (token.kind === 'PrefixedName') {
      const mapped = mapping.get(expanded);
      if (mapped && mapped !== expanded) {
        toBe = mapped;
      } else {
        const idx = token.token.indexOf(':');
        const prefix = idx >= 0 ? token.token.slice(0, idx) : '';
        const local = idx >= 0 ? token.token.slice(idx + 1) : '';
        if (prefixNsToBe[prefix]) {
          const implied = prefixNsToBe[prefix] + local;
          if (implied !== expanded) toBe = implied;
        }
      }
    }

    if (toBe) {
      status = 'Change';
      proposed++;
    }

    rows.push({
      token: token.token,
      kind: token.kind,
      expanded,
      toBe,
      status
    });
  }

  return { rows, proposed, total: rows.length };
}

export function rewriteSparqlQuery(queryText, prefixes, mapping, useNativePrefixes) {
  let text = String(queryText || '');

  text = text.replace(/^\s*PREFIX\s+([A-Za-z_][\w-]*)?:\s*<([^>]+)>\s*$/gmi, (full, _prefixRaw, namespaceRaw) => {
    const namespaceIri = (namespaceRaw || '').trim();
    const mapped = mapping.get(namespaceIri);
    if (mapped && mapped !== namespaceIri) return full.replace(`<${namespaceIri}>`, `<${mapped}>`);
    return full;
  });

  text = text.replace(/^\s*BASE\s+<([^>]+)>\s*$/gmi, (full, baseRaw) => {
    const baseIri = (baseRaw || '').trim();
    const mapped = mapping.get(baseIri);
    if (mapped && mapped !== baseIri) return full.replace(`<${baseIri}>`, `<${mapped}>`);
    return full;
  });

  const { prefixes: updatedPrefixes } = parsePrefixesAndBase(text);
  return rewriteBody(text, prefixes, updatedPrefixes, mapping, useNativePrefixes);
}

export function countSparqlAppliedChanges(inputRun, outputText, mapping, { useNativePrefixes = true } = {}) {
  let count = 0;
  const outputPrefixes = parsePrefixesAndBase(outputText).prefixes;

  for (const token of inputRun.tokens || []) {
    const oldIri = token.expanded || '';
    const newIri = mapping.get(oldIri);
    if (!oldIri || !newIri || oldIri === newIri) continue;
    const expectedOutputToken = chooseQNameOrIri(newIri, outputPrefixes, useNativePrefixes);
    if (String(outputText || '').includes(newIri) || String(outputText || '').includes(expectedOutputToken)) count++;
  }

  return count;
}

function expandPrefixedName(token, prefixes) {
  const expanded = expandCurieToIri(token, prefixes);
  return expanded.ok ? expanded.value : '';
}

function stripSparqlPrologueDeclarationLines(queryText) {
  return String(queryText || '')
    .split(/\r?\n/)
    .map((line) => /^\s*(PREFIX|BASE)\b/i.test(line) ? '' : line)
    .join('\n');
}

function scanSparql(text) {
  const iriRefs = new Set();
  const prefixedNames = new Set();

  let i = 0;
  let inComment = false;
  let inS = false;
  let inD = false;
  let inLS = false;
  let inLD = false;
  let inIri = false;

  const isNL = (c) => c === '\n' || c === '\r';
  const isNameStart = (c) => /[A-Za-z_]/.test(c);
  const isNameChar = (c) => /[A-Za-z0-9_-]/.test(c);
  const isLocalChar = (c) => /[A-Za-z0-9_.-]/.test(c);

  while (i < text.length) {
    const c = text[i];
    const c2 = text.slice(i, i + 3);

    if (!inS && !inD && !inLS && !inLD && !inIri && c === '#') inComment = true;
    if (inComment) {
      if (isNL(c)) inComment = false;
      i++;
      continue;
    }

    if (!inS && !inD && !inIri && c2 === "'''") { inLS = !inLS; i += 3; continue; }
    if (!inS && !inD && !inIri && c2 === '"""') { inLD = !inLD; i += 3; continue; }
    if (inLS || inLD) { i++; continue; }

    if (!inD && !inIri && c === "'" && text[i - 1] !== '\\') { inS = !inS; i++; continue; }
    if (!inS && !inIri && c === '"' && text[i - 1] !== '\\') { inD = !inD; i++; continue; }
    if (inS || inD) { i++; continue; }

    if (!inIri && c === '<') {
      const j = text.indexOf('>', i + 1);
      if (j > i) {
        const iri = text.slice(i + 1, j).trim();
        if (iri) iriRefs.add(iri);
        i = j + 1;
        continue;
      }
    }

    if (isNameStart(c) || c === ':') {
      const start = i;
      let p = i;

      if (c === ':') {
        p++;
      } else {
        p++;
        while (p < text.length && isNameChar(text[p])) p++;
        if (text[p] !== ':') { i++; continue; }
        p++;
      }

      if (p >= text.length || (!isNameStart(text[p]) && !/[0-9_]/.test(text[p]))) { i++; continue; }
      p++;
      while (p < text.length && isLocalChar(text[p])) p++;

      const token = text.slice(start, p);
      if (!token.startsWith('http:') && !token.startsWith('https:')) prefixedNames.add(token);
      i = p;
      continue;
    }

    i++;
  }

  return { iriRefs, prefixedNames };
}

function rewriteBody(text, originalPrefixes, updatedPrefixes, mapping, useNativePrefixes) {
  let i = 0;
  let out = '';
  let atLineStart = true;
  let skipQNameOnThisLine = false;

  let inComment = false;
  let inS = false;
  let inD = false;
  let inLS = false;
  let inLD = false;

  const isNL = (c) => c === '\n' || c === '\r';
  const isNameStart = (c) => /[A-Za-z_]/.test(c);
  const isNameChar = (c) => /[A-Za-z0-9_-]/.test(c);
  const isLocalChar = (c) => /[A-Za-z0-9_.-]/.test(c);

  while (i < text.length) {
    const c = text[i];
    const c2 = text.slice(i, i + 3);

    if (atLineStart) {
      skipQNameOnThisLine = false;
      const rest = text.slice(i).replace(/^\s+/, '');
      if (/^PREFIX\b/i.test(rest) || /^BASE\b/i.test(rest)) skipQNameOnThisLine = true;
      atLineStart = false;
    }

    if (isNL(c)) {
      atLineStart = true;
      inComment = false;
      out += c;
      i++;
      continue;
    }

    if (!inS && !inD && !inLS && !inLD && c === '#') inComment = true;
    if (inComment) {
      out += c;
      i++;
      continue;
    }

    if (!inS && !inD && c2 === "'''") { inLS = !inLS; out += c2; i += 3; continue; }
    if (!inS && !inD && c2 === '"""') { inLD = !inLD; out += c2; i += 3; continue; }
    if (inLS || inLD) { out += c; i++; continue; }

    if (!inD && c === "'" && text[i - 1] !== '\\') { inS = !inS; out += c; i++; continue; }
    if (!inS && c === '"' && text[i - 1] !== '\\') { inD = !inD; out += c; i++; continue; }
    if (inS || inD) { out += c; i++; continue; }

    if (c === '<') {
      const j = text.indexOf('>', i + 1);
      if (j > i) {
        const iri = text.slice(i + 1, j).trim();
        const mapped = mapping.get(iri);
        out += mapped && mapped !== iri ? `<${mapped}>` : text.slice(i, j + 1);
        i = j + 1;
        continue;
      }
    }

    if (!skipQNameOnThisLine && (isNameStart(c) || c === ':')) {
      const start = i;
      let p = i;

      if (c === ':') {
        p++;
      } else {
        p++;
        while (p < text.length && isNameChar(text[p])) p++;
        if (text[p] !== ':') { out += c; i++; continue; }
        p++;
      }

      if (p >= text.length || (!isNameStart(text[p]) && !/[0-9_]/.test(text[p]))) { out += c; i++; continue; }
      p++;
      while (p < text.length && isLocalChar(text[p])) p++;

      const token = text.slice(start, p);
      if (token.startsWith('http:') || token.startsWith('https:')) {
        out += token;
        i = p;
        continue;
      }

      const expanded = expandPrefixedName(token, originalPrefixes);
      const mapped = expanded ? mapping.get(expanded) : '';
      out += mapped && mapped !== expanded
        ? chooseQNameOrIri(mapped, updatedPrefixes, useNativePrefixes)
        : token;
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

  const compacted = compactIriToCurie(newIri, prefixes);
  if (compacted.ok && compacted.prefix) return compacted.value;

  const match = findLongestPrefixMatch(newIri, prefixes);
  if (match.ok && match.prefix) {
    const local = newIri.slice(match.namespaceIri.length);
    if (/^[A-Za-z0-9_.-]+$/.test(local)) return `${match.prefix}:${local}`;
  }

  for (const [prefix, namespaceIri] of Object.entries(prefixes || {})) {
    if (!prefix) continue;
    if (newIri.startsWith(namespaceIri)) {
      const local = newIri.slice(namespaceIri.length);
      if (/^[A-Za-z0-9_.-]+$/.test(local)) return `${prefix}:${local}`;
    }
  }

  return `<${newIri}>`;
}

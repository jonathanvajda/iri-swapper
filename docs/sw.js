/* sw.js - Offline-capable cache. Works on https:// or localhost. */

const CACHE_VERSION = "myna-v1";
const CACHE_SHELL = `${CACHE_VERSION}-shell`;
const CACHE_RUNTIME = `${CACHE_VERSION}-runtime`;

// Keep this list aligned with your local files if you vendor libraries.
const SHELL_ASSETS = [
  "./",
  "./index.html",
  './sparql-iri-swapper.html',
  "./styles/normalize.css",
  "./styles/skeleton.css",
  "./styles/tabulator.min.css",
  "./styles/app-base.css",
  "./styles/iri-swapper.css",
  "./app/shared/vendor/tabulator.min.js",
  "./app/shared/vendor/n3.min.js",
  "./app/shared/vendor/jsonld.min.js",
  "./app/shared/vendor/xlsx.full.min.js",
  "./app/shared/vendor/rdflib.min.js",
  "./app/site-header.js",
  "./app/ont-iri-swapper.js",
  './app/sparql-iri-swapper.js',
  "./app/shared/namespace-registry/prefix-map.js",
  "./app/shared/namespace-registry/rdf-prefixes.js",
  "./app/shared/namespace-registry/rdf-serialization-prefixes.js",
  "./app/shared/namespace-registry/sparql-prefixes.js",
  "./app/shared/namespace-registry/curie.js",
  "./app/shared/namespace-registry/namespace-registry.js",
  "./app/shared/format-registry/mime-registry.js",
  "./app/shared/format-registry/rdf-parser-formats.js",
  "./app/shared/browser-file-io/index.js",
  "./app/shared/browser-file-io/create-accept-attribute.js",
  "./app/shared/browser-file-io/create-text-blob.js",
  "./app/shared/browser-file-io/download-blob.js",
  "./app/shared/browser-file-io/download-text-file.js",
  "./app/shared/browser-file-io/read-file-as-array-buffer.js",
  "./app/shared/browser-file-io/read-file-as-text.js",
  "./app/shared/rdf-io/index.js",
  "./app/shared/rdf-io/jsonld-adapter.js",
  "./app/shared/rdf-io/n3-adapter.js",
  "./app/shared/rdf-io/object-to-rdf.js",
  "./app/shared/rdf-io/rdf-model.js",
  "./app/shared/rdf-io/rdflib-adapter.js",
  "./app/shared/rdf-io/runtime.js",
  "./app/shared/rdf-io/serialize-rdf.js",
  "./app/shared/tabular-io/delimited-text.js",
  "./app/shared/tabular-io/index.js",
  "./app/shared/tabular-io/iri-mapping.js",
  "./app/shared/tabular-io/query-records.js",
  "./manifest.webmanifest",
  "./icon.svg",
  "./sw.js",
];

self.addEventListener("install", (event) => {
  event.waitUntil((async () => {
    const cache = await caches.open(CACHE_SHELL);

    // Avoid “install fails entirely” if a single asset 404s.
    for (const url of SHELL_ASSETS) {
      try {
        await cache.add(url);
      } catch (e) {
        // Non-fatal; app can still work online
        console.warn("[myna:sw] cache add failed:", url, e);
      }
    }

    self.skipWaiting();
  })());
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.map((k) => {
      if (!k.startsWith(CACHE_VERSION)) return caches.delete(k);
    }));
    self.clients.claim();
  })());
});

// Cache-first for same-origin shell; stale-while-revalidate-ish for others
self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;

  event.respondWith((async () => {
    const url = new URL(req.url);

    // Same-origin: prefer shell cache
    if (url.origin === self.location.origin) {
      const cached = await caches.match(req);
      if (cached) return cached;

      const fresh = await fetch(req);
      const cache = await caches.open(CACHE_SHELL);
      cache.put(req, fresh.clone()).catch(() => {});
      return fresh;
    }

    // Cross-origin (CDNs): runtime cache (available offline after first load)
    const cached = await caches.match(req);
    if (cached) return cached;

    try {
      const fresh = await fetch(req);
      const cache = await caches.open(CACHE_RUNTIME);
      cache.put(req, fresh.clone()).catch(() => {});
      return fresh;
    } catch {
      return cached || Response.error();
    }
  })());
});

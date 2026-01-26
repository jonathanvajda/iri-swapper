/* sw.js - Offline-capable cache. Works on https:// or localhost. */

const CACHE_VERSION = "myna-v1";
const CACHE_SHELL = `${CACHE_VERSION}-shell`;
const CACHE_RUNTIME = `${CACHE_VERSION}-runtime`;

// Keep this list aligned with your local files if you vendor libraries.
const SHELL_ASSETS = [
  "./",
  "./index.html",
  "./styles/normalize.css",
  "./styles/skeleton.css",
  "./styles/tabulator.min.css",
  "./styles/app.css",
  "./scripts/tabulator.min.js",
  "./scripts/n3.min.js",
  "./scripts/jsonld.min.js",
  "./scripts/papaparse.min.js",
  "./scripts/xlsx.full.min.js",
  "./scripts/rdflib.min.js",
  "./scripts/app.js",
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

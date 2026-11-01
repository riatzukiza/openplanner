(ns openplanner.extern.js.hydration
  "Nbb loadFile export boundary matching existing hydration ESM names.

  This is not a generated dist module. Add cache/src and hydration/src to the
  nbb classpath first; production still uses its existing Shadow build."
  (:require [openplanner.stores.document-hydration :as hydration]))

#js {:documentNeedsHydration hydration/document-needs-hydration
     :documentSourceRef hydration/document-source-ref
     :documentCacheKey hydration/document-cache-key
     :hydrateDocumentRow hydration/hydrate-document-row
     :rowToDocument hydration/row-to-document
     :createMemoryLruCache hydration/create-memory-lru-cache
     :createLmdbCache hydration/create-lmdb-cache
     :createLayeredCache hydration/create-layered-cache
     :cacheGet hydration/cache-get-js
     :cachePut hydration/cache-put-js
     :cacheEvict hydration/cache-evict-js
     :cacheTouch hydration/cache-touch-js
     :cacheCleanup hydration/cache-cleanup-js
     :cacheStats hydration/cache-stats-js}

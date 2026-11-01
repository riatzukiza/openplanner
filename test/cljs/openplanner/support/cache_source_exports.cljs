(ns openplanner.support.cache-source-exports
  "Source-derived export map matching the existing cache Shadow ESM build."
  (:require [openplanner.stores.cache.boundary :as cache]))

#js {:createMemoryLruCache cache/create-memory-lru-cache
     :createLmdbCache cache/create-lmdb-cache
     :createLayeredCache cache/create-layered-cache
     :cacheGet cache/cache-get-js
     :cachePut cache/cache-put-js
     :cacheEvict cache/cache-evict-js
     :cacheTouch cache/cache-touch-js
     :cacheCleanup cache/cache-cleanup-js
     :cacheStats cache/cache-stats-js
     :cacheEntry cache/cache-entry-js
     :explainCacheEntry cache/explain-cache-entry-js
     :projectionEnvelope cache/projection-envelope-js
     :explainProjectionEnvelope cache/explain-projection-envelope-js}

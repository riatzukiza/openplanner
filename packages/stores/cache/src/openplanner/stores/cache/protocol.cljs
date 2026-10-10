(ns openplanner.stores.cache.protocol
  "Protocol boundary for hot/warm cache stores.

  Values are intentionally opaque. Domain stores own serialization and schema;
  adapters own only lookup, TTL, eviction, touch, cleanup, and stats semantics.")

(defprotocol CacheStore
  (cache-get [this k])
  (cache-put! [this k v opts])
  (cache-evict! [this k])
  (cache-touch! [this k opts])
  (cache-cleanup! [this])
  (cache-stats [this]))

(defn cache-store?
  [value]
  (satisfies? CacheStore value))

(defprotocol CacheEntryStore
  "Optional value plus absolute expiry lookup for cross-layer promotion.
   Implementers also accept :expires-at-ms in cache-put! without renewing expiry."
  (cache-get-entry [this k]))

(ns openplanner.stores.cache.adapters.memory
  (:require [openplanner.stores.cache.core :as core]
            [openplanner.stores.cache.protocol :refer [CacheStore CacheEntryStore cache-get]]
            [openplanner.stores.cache.schema :as schema]))

(deftype MemoryLruCache [state recency max-entries default-ttl-ms]
  CacheEntryStore
  (cache-get-entry [this k]
    (when-some [value (cache-get this k)]
      {:value value :expires-at-ms (schema/entry-expires-at (get @state k))}))
  CacheStore
  (cache-get [_ k]
    (let [entry (get @state k)
          now (core/now-ms)]
      (cond
        (nil? entry) nil
        (schema/entry-expired? entry now)
        (do (swap! state dissoc k) nil)
        :else
        (do (swap! state assoc k (assoc (schema/touch-entry entry nil) :cache/recency (swap! recency inc)))
            (schema/entry-value entry)))))

  (cache-put! [_ k v opts]
    (let [ttl-ms (core/ttl-ms opts default-ttl-ms)
          options (core/opts-map opts)
          entry (cond-> (assoc (schema/cache-entry {:key k :value v :ttl-ms ttl-ms})
                              :cache/recency (swap! recency inc))
                  (contains? options :expires-at-ms)
                  (assoc :cache/expires-at-ms (:expires-at-ms options)))]
      (swap! state assoc k entry)
      (while (> (count @state) max-entries)
        (let [victim (reduce-kv (fn [oldest key entry]
                                  (if (or (nil? oldest)
                                          (< (:cache/recency entry) (:cache/recency (get @state oldest))))
                                    key
                                    oldest))
                                nil @state)]
          (swap! state dissoc victim)))
      true))

  (cache-evict! [_ k]
    (let [present? (contains? @state k)]
      (swap! state dissoc k)
      present?))

  (cache-touch! [_ k opts]
    (let [entry (get @state k)]
      (cond
        (nil? entry) false
        (schema/entry-expired? entry (core/now-ms))
        (do (swap! state dissoc k) false)
        :else
        (let [ttl-ms (core/ttl-ms opts default-ttl-ms)]
          (swap! state assoc k (assoc (schema/touch-entry entry ttl-ms)
                                     :cache/recency (swap! recency inc)))
          true))))

  (cache-cleanup! [_]
    (let [before (count @state)
          now (core/now-ms)]
      (swap! state (fn [m]
                     (into {} (remove (fn [[_ entry]]
                                        (schema/entry-expired? entry now))
                                      m))))
      (- before (count @state))))

  (cache-stats [_]
    {:type "memory-lru"
     :size (count @state)
     :maxEntries max-entries
     :defaultTtlMs default-ttl-ms}))

(defn create-memory-lru-cache
  ([] (create-memory-lru-cache nil))
  ([opts]
   (let [opts (core/opts-map opts)
         capacity (or (:maxEntries opts) (:max-entries opts) 512)]
     (when-not (and (number? capacity) (js/Number.isSafeInteger capacity) (<= 0 capacity))
       (throw (js/Error. "maxEntries must be a non-negative safe integer")))
     (MemoryLruCache. (atom {}) (atom 0)
                      capacity
                      (long (or (:defaultTtlMs opts) (:default-ttl-ms opts) (* 5 60 60 1000)))))))

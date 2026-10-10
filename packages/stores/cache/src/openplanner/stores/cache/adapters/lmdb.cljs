(ns openplanner.stores.cache.adapters.lmdb
  (:require [openplanner.stores.cache.core :as core]
            [openplanner.stores.cache.protocol :refer [CacheStore CacheEntryStore cache-get]]))

(deftype LmdbTtlCache [^js db prefix default-ttl-ms]
  CacheEntryStore
  (cache-get-entry [this k]
    (when-some [value (cache-get this k)]
      {:value value :expires-at-ms (core/jget (.get db (str prefix k)) "expiresAt")}))
  CacheStore
  (cache-get [_ k]
    (let [key (str prefix k)
          entry (.get db key)
          now (core/now-ms)]
      (cond
        (nil? entry) nil
        (and (core/jget entry "expiresAt") (< (core/jget entry "expiresAt") now))
        (do (.remove db key) nil)
        :else (core/jget entry "value"))))

  (cache-put! [_ k v opts]
    (let [ttl-ms (core/ttl-ms opts default-ttl-ms)
          now (core/now-ms)
          options (core/opts-map opts)
          expires-at (if (contains? options :expires-at-ms)
                       (:expires-at-ms options)
                       (when (pos? ttl-ms) (+ now ttl-ms)))]
      (.put db (str prefix k) #js {:value v
                                   :createdAt now
                                   :touchedAt now
                                   :expiresAt expires-at})))

  (cache-evict! [_ k]
    (.remove db (str prefix k)))

  (cache-touch! [_ k opts]
    (let [key (str prefix k)
          entry (.get db key)
          now (core/now-ms)]
      (cond
        (nil? entry) false
        (and (core/jget entry "expiresAt") (< (core/jget entry "expiresAt") now))
        (do (.remove db key) false)
        :else
        (let [ttl-ms (core/ttl-ms opts default-ttl-ms)]
          (.put db key #js {:value (core/jget entry "value")
                            :createdAt (or (core/jget entry "createdAt") now)
                            :touchedAt now
                            :expiresAt (when (pos? ttl-ms) (+ now ttl-ms))})))))

  (cache-cleanup! [_]
    ;; LMDB key-range cleanup is intentionally left to explicit future compaction.
    0)

  (cache-stats [_]
    {:type "lmdb-ttl"
     :prefix prefix
     :defaultTtlMs default-ttl-ms}))

(defn create-lmdb-cache
  [opts]
  (let [db (core/jget opts "db")
        prefix (or (core/jget opts "prefix") "")
        default-ttl-ms (or (core/jget opts "defaultTtlMs") (* 5 60 60 1000))]
    (when-not db
      (throw (js/Error. "createLmdbCache requires an open LMDB database handle")))
    (LmdbTtlCache. db prefix (long default-ttl-ms))))

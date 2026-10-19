(ns openplanner.stores.cache.adapters.lmdb
  (:require [clojure.string :as str]
            [openplanner.stores.cache.core :as core]
            [openplanner.stores.cache.protocol :refer [CacheStore CacheEntryStore]]))

(defn- remove-expired! [^js db key]
  ;; Expiry already makes this entry a miss. Purging is best effort; retain
  ;; ownership of asynchronous failure without changing synchronous reads.
  (try
    (.catch (js/Promise.resolve (.remove db key)) (fn [_error] nil))
    (catch :default _error nil)))

(deftype LmdbTtlCache [^js db prefix default-ttl-ms]
  CacheEntryStore
  (cache-get-entry [_ k] (core/checked-key k)
    (let [key (str prefix k)
          entry (.get db key)
          expires-at (core/jget entry "expiresAt")]
      (cond
        (nil? entry) nil
        (and expires-at (<= expires-at (core/now-ms))) (do (remove-expired! db key) nil)
        :else (when-some [value (core/jget entry "value")]
                {:value value :expires-at-ms expires-at}))))
  CacheStore
  (cache-get [_ k] (core/checked-key k)
    (let [key (str prefix k)
          entry (.get db key)
          now (core/now-ms)]
      (cond
        (nil? entry) nil
        (and (core/jget entry "expiresAt") (<= (core/jget entry "expiresAt") now))
        (do (remove-expired! db key) nil)
        :else (core/jget entry "value"))))

  (cache-put! [_ k v opts] (core/checked-key k)
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

  (cache-evict! [_ k] (core/checked-key k)
    (.remove db (str prefix k)))

  (cache-touch! [_ k opts] (core/checked-key k)
    (let [key (str prefix k)
          entry (.get db key)
          now (core/now-ms)]
      (cond
        (nil? entry) false
        (and (core/jget entry "expiresAt") (<= (core/jget entry "expiresAt") now))
        (do (remove-expired! db key) false)
        :else
        (let [ttl-ms (core/ttl-ms opts default-ttl-ms)]
          (.put db key #js {:value (core/jget entry "value")
                            :createdAt (or (core/jget entry "createdAt") now)
                            :touchedAt now
                            :expiresAt (when (pos? ttl-ms) (+ now ttl-ms))})))))

  (cache-cleanup! [_]
    (let [now (core/now-ms)
          rows (.getRange db #js {:start prefix})
          iterator (.call (aget rows js/Symbol.iterator) rows)
          expired (try
                    (loop [keys []]
                      (let [step (.next iterator)
                            row (.-value step)
                            k (core/jget row "key")]
                        (if (or (.-done step)
                                (not (and (string? k) (str/starts-with? k prefix))))
                          keys
                          (let [expires-at (core/jget (core/jget row "value") "expiresAt")]
                            (recur (if (and (number? expires-at) (<= expires-at now))
                                     (conj keys k)
                                     keys))))))
                    (finally
                      (when (fn? (.-return iterator)) (.return iterator))))]
      (core/pthen (js/Promise.all (into-array (map #(.remove db %) expired)))
                  (fn [removed] (count (filter true? (array-seq removed)))))))

  (cache-stats [_]
    {:type "lmdb-ttl"
     :prefix prefix
     :defaultTtlMs default-ttl-ms}))

(defn create-lmdb-cache
  [opts]
  (let [db (core/jget opts "db")
        prefix (or (core/jget opts "prefix") "")
        default-ttl-ms (core/default-ttl-ms opts)]
    (when-not db
      (throw (js/Error. "createLmdbCache requires an open LMDB database handle")))
    (LmdbTtlCache. db prefix default-ttl-ms)))

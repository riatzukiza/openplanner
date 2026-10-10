(ns openplanner.stores.cache.layered
  (:require [openplanner.stores.cache.core :as core]
            [openplanner.stores.cache.protocol :refer [CacheStore CacheEntryStore cache-get-entry
                                                       cache-cleanup!
                                                       cache-evict!
                                                       cache-get
                                                       cache-put!
                                                       cache-stats
                                                       cache-touch!]]))

(defn- entry! [layer k]
  (if (satisfies? CacheEntryStore layer)
    (cache-get-entry layer k)
    (core/pthen (cache-get layer k)
                (fn [value] (when (some? value) {:value value :unknown-expiry? true})))))

(defn- lookup! [layers k]
  (letfn [(try-layer [seen remaining]
            (if (empty? remaining)
              (core/promise nil)
              (let [layer (first remaining)]
                (core/pthen (entry! layer k)
                            (fn [entry]
                              (if (some? entry)
                                (core/pthen
                                 (if (:unknown-expiry? entry)
                                   (core/promise nil)
                                   (js/Promise.all
                                    (clj->js (map #(cache-put! % k (:value entry)
                                                              {:expires-at-ms (:expires-at-ms entry)})
                                                  (filter #(satisfies? CacheEntryStore %) seen)))))
                                 (fn [_] entry))
                                (try-layer (conj seen layer) (rest remaining))))))))]
    (try-layer [] layers)))

(deftype LayeredCache [layers]
  CacheEntryStore
  (cache-get-entry [_ k] (lookup! layers k))
  CacheStore
  (cache-get [_ k]
    (core/pthen (lookup! layers k) (fn [entry] (:value entry))))

  (cache-put! [_ k v opts]
    (js/Promise.all (clj->js (map #(cache-put! % k v opts) layers))))

  (cache-evict! [_ k]
    (js/Promise.all (clj->js (map #(cache-evict! % k) layers))))

  (cache-touch! [_ k opts]
    (js/Promise.all (clj->js (map #(cache-touch! % k opts) layers))))

  (cache-cleanup! [_]
    (core/pthen (js/Promise.all (clj->js (map cache-cleanup! layers)))
                (fn [xs] (reduce + 0 (js->clj xs)))))

  (cache-stats [_]
    {:type "layered"
     :layers (mapv cache-stats layers)}))

(defn create-layered-cache
  [caches]
  (LayeredCache. (vec caches)))

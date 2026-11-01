(ns openplanner.contract.cache-exports-contract
  "Check configured cache export names and their nbb source functions.

  This is not a compiled Shadow ESM reference or an nbb ESM ABI contract."
  (:require ["node:assert/strict" :as assert]
            ["node:fs" :as fs]
            [cljs.reader :as reader]
            [openplanner.stores.cache.boundary]))

(def exports
  (-> (reader/read-string (fs/readFileSync "packages/stores/cache/shadow-cljs.edn" "utf8"))
      :builds :lib :modules :index :exports))

(defn source-function [name]
  (let [sym (get exports (keyword name))
        target (when sym (resolve sym))]
    (assert/ok target (str name " resolves its configured source var"))
    (let [f @target]
      (assert/ok (instance? js/Function f) (str name " source is JS-callable"))
      f)))

(defn run-contract []
  (let [names (sort (map name (keys exports)))]
    (assert/deepEqual (clj->js names)
                      #js ["cacheCleanup" "cacheEntry" "cacheEvict" "cacheGet" "cachePut"
                           "cacheStats" "cacheTouch" "createLayeredCache" "createLmdbCache"
                           "createMemoryLruCache" "explainCacheEntry" "explainProjectionEnvelope"
                           "projectionEnvelope"]
                      "configured export names")
    (doseq [name names] (source-function name))
    (let [cache ((source-function "createMemoryLruCache") #js {:maxEntries 2})]
      (assert/equal ((source-function "cachePut") cache "alpha" "value") true)
      (assert/equal ((source-function "cacheGet") cache "alpha") "value")
      (assert/equal ((source-function "cacheEvict") cache "alpha") true)
      (assert/equal ((source-function "cacheGet") cache "alpha") nil))
    (let [entry ((source-function "cacheEntry") #js {:key "k" :value "v" :ttlMs 5 :nowMs 1})]
      (assert/equal (aget entry "cache/key") "k")
      (assert/equal (aget entry "cache/expires-at-ms") 6))
    (println "Cache configured source functions passed on nbb; compiled ESM not tested")))

(run-contract)

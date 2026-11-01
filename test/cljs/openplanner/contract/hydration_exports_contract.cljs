(ns openplanner.contract.hydration-exports-contract
  "Check configured hydration export names and their nbb source functions.

  This is not a compiled Shadow ESM reference or an nbb ESM ABI contract."
  (:require ["node:assert/strict" :as assert]
            ["node:fs" :as fs]
            [cljs.reader :as reader]
            [openplanner.stores.document-hydration]))

(def exports
  (-> (reader/read-string (fs/readFileSync "packages/stores/document-hydration/shadow-cljs.edn" "utf8"))
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
                      #js ["cacheCleanup" "cacheEvict" "cacheGet" "cachePut" "cacheStats" "cacheTouch"
                           "createLayeredCache" "createLmdbCache" "createMemoryLruCache" "documentCacheKey"
                           "documentNeedsHydration" "documentSourceRef" "hydrateDocumentRow" "rowToDocument"]
                      "configured export names")
    (doseq [name names] (source-function name))
    (let [row #js {:id "doc-1" :kind "code" :project "synthetic" :text ""
                   :extra #js {:source_path "synthetic/file.cljs" :migration_2 #js {:text_hash_sha256 "abc123"}}}]
      (assert/equal ((source-function "documentNeedsHydration") row) true)
      (assert/equal ((source-function "documentCacheKey") row) "openplanner:source:synthetic:abc123")
      (let [result ((source-function "hydrateDocumentRow") row "synthetic source")
            document ((source-function "rowToDocument") (.-row result))]
        (assert/equal (.-hydrated result) true)
        (assert/equal (.-content document) "synthetic source")
        (assert/equal (.-kind document) "code")))
    (let [cache ((source-function "createMemoryLruCache") #js {:maxEntries 2})]
      (assert/equal ((source-function "cachePut") cache "k" "v") true)
      (assert/equal ((source-function "cacheGet") cache "k") "v"))
    (println "Hydration configured source functions passed on nbb; compiled ESM not tested")))

(run-contract)

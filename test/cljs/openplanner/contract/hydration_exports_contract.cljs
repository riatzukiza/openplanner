(ns openplanner.contract.hydration-exports-contract
  "Nbb JS export contract for the existing hydration source API; no compiled reference dist."
  (:require ["node:assert/strict" :as assert]
            ["node:fs" :as fs]
            [cljs.reader :as reader]
            ["node:path" :as path]
            ["nbb" :as nbb]))

(def names
  (->> (reader/read-string (fs/readFileSync "packages/stores/document-hydration/shadow-cljs.edn" "utf8"))
       :builds :lib :modules :index :exports keys (map name) sort vec))

(defn ^:async run-contract []
  (let [selector (.-OPENPLANNER_HYDRATION_EXPORTS_IMPL js/process.env)
        _ (when-not (or (nil? selector) (= selector "source") (= selector "nbb"))
            (throw (js/Error. (str "Unsupported OPENPLANNER_HYDRATION_EXPORTS_IMPL: " selector))))
        _ (nbb/addClassPath (path/resolve "packages/stores/cache/src"))
        _ (nbb/addClassPath (path/resolve "packages/stores/document-hydration/src"))
        file (if (= selector "nbb") "src/cljs/openplanner/extern/js/hydration.cljs" "test/cljs/openplanner/support/hydration_source_exports.cljs")
        api (await (nbb/loadFile (path/resolve file)))]
    (assert/deepEqual (.sort (js/Object.keys api)) (clj->js (sort names)))
    (doseq [name names]
      (assert/ok (instance? js/Function (aget api name)) (str name " is JS-callable")))
    (let [row #js {:id "doc-1" :kind "code" :project "synthetic" :text ""
                   :extra #js {:source_path "synthetic/file.cljs" :migration_2 #js {:text_hash_sha256 "abc123"}}}]
      (assert/equal ((.-documentNeedsHydration api) row) true)
      (assert/equal ((.-documentCacheKey api) row) "openplanner:source:synthetic:abc123")
      (let [result ((.-hydrateDocumentRow api) row "synthetic source")
            document ((.-rowToDocument api) (.-row result))]
        (assert/equal (.-hydrated result) true)
        (assert/equal (.-content document) "synthetic source")
        (assert/equal (.-kind document) "code")))
    (let [cache ((.-createMemoryLruCache api) #js {:maxEntries 2})]
      (assert/equal ((.-cachePut api) cache "k" "v") true)
      (assert/equal ((.-cacheGet api) cache "k") "v"))
    (println "Hydration JS exports contract passed against" (or selector "source"))))

(run-contract)

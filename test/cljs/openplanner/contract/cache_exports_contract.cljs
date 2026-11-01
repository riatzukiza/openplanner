(ns openplanner.contract.cache-exports-contract
  "Nbb JS export contract for the existing cache source API; no compiled reference dist."
  (:require ["node:assert/strict" :as assert]
            ["node:fs" :as fs]
            [cljs.reader :as reader]
            ["node:path" :as path]
            ["nbb" :as nbb]))

(def names
  (->> (reader/read-string (fs/readFileSync "packages/stores/cache/shadow-cljs.edn" "utf8"))
       :builds :lib :modules :index :exports keys (map name) sort vec))

(defn ^:async run-contract []
  (let [selector (.-OPENPLANNER_CACHE_EXPORTS_IMPL js/process.env)
        _ (when-not (or (nil? selector) (= selector "source") (= selector "nbb"))
            (throw (js/Error. (str "Unsupported OPENPLANNER_CACHE_EXPORTS_IMPL: " selector))))
        _ (nbb/addClassPath (path/resolve "packages/stores/cache/src"))
        file (if (= selector "nbb") "src/cljs/openplanner/extern/js/cache.cljs" "test/cljs/openplanner/support/cache_source_exports.cljs")
        api (await (nbb/loadFile (path/resolve file)))]
    (assert/deepEqual (.sort (js/Object.keys api)) (clj->js (sort names)) "export names")
    (doseq [name names]
      (assert/ok (instance? js/Function (aget api name)) (str name " is JS-callable")))
    (let [cache ((.-createMemoryLruCache api) #js {:maxEntries 2})]
      (assert/equal ((.-cachePut api) cache "alpha" "value") true)
      (assert/equal ((.-cacheGet api) cache "alpha") "value")
      (assert/equal ((.-cacheEvict api) cache "alpha") true)
      (assert/equal ((.-cacheGet api) cache "alpha") nil))
    (let [entry ((.-cacheEntry api) #js {:key "k" :value "v" :ttlMs 5 :nowMs 1})]
      (assert/equal (aget entry "cache/key") "k")
      (assert/equal (aget entry "cache/expires-at-ms") 6))
    (println "Cache JS exports contract passed against" (or selector "source"))))

(run-contract)

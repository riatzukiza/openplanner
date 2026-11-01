(ns closure-object-contract
  "Object-classification boundary characterization for Node/nbb.

  The reference mode exercises official google-closure-library npm 20230802.
  That is not the project's unavailable shadow-resolved 2025 Clojure fork."
  (:require ["node:assert/strict" :as assert]
            ["node:module" :as node-module]
            ["node:path" :as path]
            ["node:url" :as url]
            ["node:vm" :as vm]
            ["nbb" :as nbb]))

(defn ^:async sci-async-function [] true)
(defn ordinary-function [] true)

(def cases
  [["null" nil false]
   ["undefined" js/undefined false]
   ["boolean" false false]
   ["number" 1 false]
   ["string" "x" false]
   ["symbol" (js/Symbol "x") false]
   ["bigint" (js/BigInt 1) false]
   ["array" #js [] false]
   ["plain" #js {} true]
   ["null prototype" (js/Object.create nil) true]
   ["date" (js/Date.) true]
   ["map" (js/Map.) true]
   ["set" (js/Set.) true]
   ["promise" (js/Promise.resolve 1) true]
   ["regexp" #"x" true]
   ["typed array" (js/Uint8Array. 1) true]
   ["boxed number" (js/Number. 1) true]
   ["function" ordinary-function false]
   ["SCI async callable object" sci-async-function true]
   ["cross realm function" (vm/runInNewContext "(function(){})") false]
   ["cross realm object" (vm/runInNewContext "({})") true]
   ["cross realm array" (vm/runInNewContext "[]") false]
   ["function proxy" (js/Proxy. ordinary-function #js {}) false]
   ["object proxy" (js/Proxy. #js {} #js {}) true]])

(defn reference-predicate [directory]
  (let [node-require (node-module/createRequire (str (url/pathToFileURL (path/resolve "package.json"))))
        version (.-version (node-require (path/resolve directory "package.json")))]
    (assert/equal version "20230802.0.0" "characterized Closure version")
    (node-require (path/resolve directory "closure/goog/bootstrap/nodejs.js"))
    (let [goog (.-goog js/globalThis)]
      (fn [value] (= "object" (.typeOf goog value))))))

(defn ^:async run-contract []
  (let [selector (.-OPENPLANNER_TYPE_IMPL js/process.env)
        _ (when-not (or (= selector "reference") (= selector "nbb"))
            (throw (js/Error. "Set OPENPLANNER_TYPE_IMPL to reference or nbb")))
        impl (if (= selector "reference")
               (reference-predicate (.-OPENPLANNER_CLOSURE_REFERENCE js/process.env))
               (do (nbb/addClassPath (path/resolve "packages/stores/cache/src"))
                   (.-jsObject (await (nbb/loadFile (path/resolve "tests/support/cache_compat_exports.cljs"))))))]
    (doseq [[label value expected] cases]
      (assert/equal (boolean (impl value)) expected label))
    (let [r (js/Proxy.revocable #js {} #js {})]
      ((.-revoke r))
      (let [error-name (try (impl (.-proxy r)) nil
                            (catch :default error (.-name error)))]
        (assert/equal error-name "TypeError" "revoked object proxy remains an error")))
    (let [r (js/Proxy.revocable ordinary-function #js {})]
      ((.-revoke r))
      (assert/equal (boolean (impl (.-proxy r))) false "revoked callable proxy remains a function"))
    (let [prototype (.-prototype js/Function)
          saved (.-toString prototype)
          observed (atom nil)]
      (try
        (set! (.-toString prototype) (fn [] (throw (js/Error. "synthetic mutation"))))
        (reset! observed (boolean (impl ordinary-function)))
        (finally (set! (.-toString prototype) saved)))
      (assert/equal @observed false "function classification ignores toString mutation"))
    (println "Closure object-classification contract passed against" selector)))

(run-contract)

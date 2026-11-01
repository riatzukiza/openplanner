(ns openplanner.contract.closure-object-intrinsic-regression
  "Regression for a classifier bug under mutated Function.prototype.toString.

  This briefly replaces Function.prototype.toString in this disposable process
  and restores it in finally. No service, filesystem state or credentials."
  (:require ["node:assert/strict" :as assert]
            ["node:path" :as path]
            ["nbb" :as nbb]))

(defn ordinary-function [] true)
(defn ^:async main []
  (nbb/addClassPath (path/resolve "packages/stores/cache/src"))
  (let [module (await (nbb/loadFile (path/resolve "test/cljs/openplanner/support/cache_compat_exports.cljs")))
        object? (.-jsObject module)
        prototype (.-prototype js/Function)
        original (.-toString prototype)]
    (assert/equal (boolean (object? ordinary-function)) false "baseline")
    (try
      (set! (.-toString prototype) (fn [] (throw (js/Error. "synthetic mutation"))))
      ;; Closure and Lodash use native typeof; the result must not depend on
      ;; the mutable Function.prototype.toString property.
      (assert/equal (boolean (object? ordinary-function)) false "intrinsic mutation")
      (finally (set! (.-toString prototype) original)))
    (assert/equal (boolean (object? ordinary-function)) false "intrinsic restored")
    (println "Classifier ignores synthetic Function.prototype.toString mutation.")))

(main)

(ns openplanner.graph.recall.boundary
  "Established Node/ESM conversion boundary for the versioned recall data API.
   Pure selection only: caller snapshots must be produced by the trusted
   storage/authority adapter. This export grants no access and performs no I/O."
  (:require [openplanner.graph.recall.contract :as contract]
            [openplanner.graph.recall.core :as recall]))

(defn- keyword-value [value]
  (if (string? value) (keyword value) value))

(defn recall-plan-js
  "Accept canonical kebab-case JSON keys; return a JSON-compatible plan.
   Invalid shapes remain invalid. No coercion fills in authority or budgets."
  [snapshot decisions request]
  (let [snapshot (js->clj snapshot :keywordize-keys true)
        snapshot (if (map? snapshot)
                   (cond-> (update snapshot :index-status keyword-value)
                     (vector? (:influences snapshot))
                     (update :influences #(mapv (fn [row] (if (map? row) (update row :kind keyword-value) row)) %)))
                   snapshot)
        request (js->clj request :keywordize-keys true)
        request (if (map? request) (update request :feedback keyword-value) request)
        result (recall/recall-plan snapshot (js->clj decisions :keywordize-keys true) request)]
    (clj->js (if (contract/valid-result? result) result
                {:status :failed :hits [] :failure {:stage :graph :code :invalid-result}
                 :feedback {:status :not-requested :attempted 0 :completed 0}}))))

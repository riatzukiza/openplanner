(ns openplanner.graph.claims.lifecycle
  "Pure lifecycle transition semantics for edge claims.

  Routes/adapters decide how to persist these transition plans. This namespace
  only decides the next status, confidence, event bucket, and timestamped update
  shape from normalized transition input."
  (:require [openplanner.graph.claims.boundary :as boundary]))

(def lifecycle-actions #{:support :refute :withdraw})

(defn- event-ids-from-js
  [value]
  (when-not (and (array? value)
                 (every? #(and (string? %) (seq (.trim %))) (array-seq value)))
    (throw (js/Error. "Lifecycle evidence IDs must be an array of nonblank strings")))
  (->> (array-seq value) (map #(.trim %)) distinct vec))

(defn- jget
  [obj k]
  (when (and (some? obj)
             (= "object" (goog/typeOf obj)))
    (aget obj k)))

(defn- agreeing-evidence-ids
  [body fields]
  (let [aliases (when (and (some? body) (= "object" (goog/typeOf body)))
                  (filter #(js/Object.hasOwn body %) fields))
        values (mapv #(event-ids-from-js (jget body %)) aliases)]
    (when (and (seq values) (not (apply = values)))
      (throw (ex-info "Lifecycle evidence aliases must agree" {:fields (vec aliases)})))
    (or (first values) [])))

(defn- action-keyword
  [value]
  (let [action (some-> value str .trim .toLowerCase keyword)]
    (when (contains? lifecycle-actions action) action)))

(defn- clamp-confidence
  [value fallback]
  (let [n (if (nil? value) fallback (js/Number value))]
    (if (js/Number.isFinite n)
      (max 0 (min 1 n))
      fallback)))

(defn transition-plan
  [action body]
  (let [action (or (action-keyword action)
                   (throw (ex-info "Unknown edge claim lifecycle action" {:action action})))]
    (case action
      :support
      (let [status (boundary/normalize-edge-claim-status (jget body "status") :supported)
            next-status (if (= :active status) :active :supported)]
        {:action :support
         :status next-status
         :confidence (clamp-confidence (jget body "confidence") 0.75)
         :event-field :support_event_ids
         :event-ids (agreeing-evidence-ids body ["event_ids" "eventIds" "support_event_ids"])})

      :refute
      {:action :refute
       :status :refuted
       :confidence (clamp-confidence (jget body "confidence") 0)
       :event-field :refute_event_ids
       :event-ids (agreeing-evidence-ids body ["event_ids" "eventIds" "refute_event_ids"])}

      :withdraw
      {:action :withdraw
       :status :withdrawn
       :confidence nil
       :event-field nil
       :event-ids []})))

(defn transition-plan-js
  [action body]
  (let [{:keys [action status confidence event-field event-ids]} (transition-plan action body)]
    #js {:action (name action)
         :status (name status)
         :confidence confidence
         :eventField (some-> event-field name)
         :eventIds (clj->js event-ids)}))

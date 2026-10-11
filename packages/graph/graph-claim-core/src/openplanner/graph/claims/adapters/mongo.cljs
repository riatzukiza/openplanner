(ns openplanner.graph.claims.adapters.mongo
  "Mongo row/document adapter for graph edge claims.

  This namespace normalizes Mongo-ish document shapes into the same JS-facing
  boundary used by HTTP/ESM callers. It contains field alias knowledge only; it
  does not decide whether a claim should be accepted or projected."
  (:require [openplanner.graph.claims.boundary :as boundary]
            [openplanner.graph.claims.core :as claims]))

(defn- get-any
  [m ks]
  (some (fn [k]
          (when (contains? m k)
            (get m k)))
        ks))

(defn- present-entry
  "Retain alias presence separately from its value, including false and nil."
  [doc fields]
  (some (fn [field]
          (when (contains? doc field)
            {:value (get doc field)}))
        fields))

(defn- mongo-claim->js
  [doc]
  (let [camel (present-entry doc [:claim-id :claimId "claimId"])
        snake (present-entry doc [:claim_id "claim_id"])
        storage (when-not (or camel snake) (present-entry doc [:_id "_id" :id "id"]))
        row #js {:direction (get-any doc [:direction "direction"])
       :status (get-any doc [:status "status"])
       :confidence (get-any doc [:confidence "confidence"])
       :scope (clj->js (or (get-any doc [:scope "scope"])
                           (select-keys doc [:tenant_id :org_id :project :lake :graph_version])))}]
    ;; Preserve all supplied identity aliases for the canonical boundary's agreement law.
    (doseq [field ["sourceNodeId" "source_node_id" "source-node-id" "source"
                   "targetNodeId" "target_node_id" "target-node-id" "target"
                   "relationKind" "relation_kind" "relation-kind" "kind"]]
      (when-let [entry (present-entry doc [(keyword field) field])]
        (aset row field (:value entry))))
    (when-let [expiration (present-entry doc [:validUntil "validUntil"])]
      (aset row "validUntil" (:value expiration)))
    (when-let [expiration (present-entry doc [:valid-until :valid_until "valid_until"])]
      (aset row "valid_until" (:value expiration)))
    (when camel (aset row "claimId" (:value camel)))
    (when snake (aset row "claim_id" (:value snake)))
    (when storage (aset row "claim_id" (:value storage)))
    row))

(defn edge-claim-from-mongo
  [doc]
  (boundary/edge-claim-from-js (mongo-claim->js doc)))

(defn edge-claims-from-mongo
  [docs]
  (mapv edge-claim-from-mongo docs))

(defn- projected-edge->js
  [edge]
  (clj->js {:source (:source edge)
            :target (:target edge)
            :kind (:kind edge)
            :claim_id (:claim-id edge)
            :confidence (:confidence edge)
            :direction (name (:direction edge))
            :scope (:scope edge)
            :status (name (:status edge))}))

(defn project-mongo-edge-claims
  ([docs] (project-mongo-edge-claims docs #js {}))
  ([docs opts]
   (let [normalized (edge-claims-from-mongo docs)
         result (claims/project-claims normalized (boundary/projection-options-from-js opts))]
     #js {:edges (clj->js (mapv #(js->clj (projected-edge->js %) :keywordize-keys true) (:edges result)))
          :stats (clj->js (:stats result))})))

(defn project-mongo-edge-claims-js
  "JS export for projecting Mongo/API edge claim rows.

  Accepts an array of row-like JS objects. Field alias coercion stays inside the
  graph-claim boundary; callers do not need to map Mongo rows into projected
  edges themselves."
  ([docs] (project-mongo-edge-claims-js docs #js {}))
  ([docs opts]
   (project-mongo-edge-claims (js->clj docs :keywordize-keys true) opts)))

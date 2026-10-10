(ns openplanner.graph.claims.core-test
  (:require [cljs.test :refer [deftest is run-tests]]
            [openplanner.graph.claims.adapters.mongo :as mongo]
            [openplanner.graph.claims.boundary :as boundary]
            [openplanner.graph.claims.core :as claims]
            [openplanner.graph.claims.lifecycle :as lifecycle]
            [openplanner.graph.claims.policy :as policy]
            [openplanner.graph.claims.schema :as schema]))

(deftest pure-projection-only-accepts-supported-or-active-non-expired-claims
  (let [base {:claim-id "edge_claim:test"
              :source-node-id "node:a"
              :target-node-id "node:b"
              :relation-kind "depends_on"
              :direction :directed
              :scope {:project "devel"}
              :scope-json "{}"
              :confidence 0.8
              :valid-until-ms nil}
        opts {:statuses claims/projectable-statuses
              :include-expired? false
              :now-ms 1000}]
    (is (nil? (claims/claim->projected-edge (assoc base :status :proposed) opts)))
    (is (= "depends_on" (:kind (claims/claim->projected-edge (assoc base :status :supported) opts))))
    (is (= "edge_claim:test" (:claim-id (claims/claim->projected-edge (assoc base :status :active) opts))))
    (is (nil? (claims/claim->projected-edge (assoc base :status :supported :valid-until-ms 999) opts)))
    (is (some? (claims/claim->projected-edge (assoc base :status :supported :valid-until-ms 999)
                                             (assoc opts :include-expired? true))))))

(deftest claim-id-canonicalizes-undirected-endpoints
  (let [left #js {:source_node_id "node:a"
                  :target_node_id "node:b"
                  :relation_kind "related_to"
                  :direction "undirected"
                  :scope #js {:project "devel"}}
        right #js {:source_node_id "node:b"
                   :target_node_id "node:a"
                   :relation_kind "related_to"
                   :direction "undirected"
                   :scope #js {:project "devel"}}]
    (is (= (boundary/build-edge-claim-id left)
           (boundary/build-edge-claim-id right)))
    (is (re-matches #"edge_claim:[a-f0-9]{24}" (boundary/build-edge-claim-id left)))))

(deftest boundary-projects-js-claims-with-explicit-coercion
  (let [claims #js [#js {:claim_id "edge_claim:one"
                         :source_node_id "node:a"
                         :target_node_id "node:b"
                         :relation_kind "supports"
                         :direction "directed"
                         :status "supported"
                         :confidence "0.75"
                         :valid_until "2099-01-01T00:00:00.000Z"
                         :scope #js {:project "devel"}}
                    #js {:claim_id "edge_claim:two"
                         :source_node_id "node:a"
                         :target_node_id "node:c"
                         :relation_kind "supports"
                         :direction "directed"
                         :status "proposed"
                         :confidence 0.2
                         :scope #js {:project "devel"}}]
        result (boundary/project-edge-claims-js claims #js {:now "2026-01-01T00:00:00.000Z"})
        edges (aget result "edges")
        stats (aget result "stats")
        first-edge (aget edges 0)]
    (is (= 2 (aget stats "claims")))
    (is (= 1 (aget stats "edges")))
    (is (= "edge_claim:one" (aget first-edge "claim_id")))
    (is (= "supported" (aget first-edge "status")))))

(deftest schema-explains-invalid-normalized-claims
  (let [claim {:claim-id "edge_claim:bad"
               :source-node-id "node:a"
               :target-node-id "node:a"
               :relation-kind "supports"
               :direction :directed
               :scope-json "{}"
               :status :supported
               :confidence 2}
        explanation (schema/explain-edge-claim claim)]
    (is (false? (:valid? explanation)))
    (is (= #{:self-edge-not-allowed :number-between-zero-and-one}
           (set (map :error (:errors explanation)))))))

(deftest policy-makes-data-decisions-from-normalized-claims
  (let [base {:claim-id "edge_claim:policy"
              :source-node-id "node:a"
              :target-node-id "node:b"
              :relation-kind "supports"
              :direction :directed
              :scope-json "{}"
              :scope {}
              :confidence 0.9}]
    (is (= :accept (:decision/kind (policy/evaluate-claim (assoc base :status :supported)))))
    (is (= :reject (:decision/kind (policy/evaluate-claim (assoc base :status :rejected)))))
    (is (= :defer (:decision/kind (policy/evaluate-claim (assoc base :status :proposed)))))
    (is (= :supersede (:decision/kind (policy/evaluate-claim (assoc base :status :superseded)))))))

(deftest mongo-adapter-projects-row-like-js-documents
  (let [rows #js [#js {:_id "edge_claim:row"
                       :claim_id "edge_claim:row"
                       :source_node_id "node:a"
                       :target_node_id "node:b"
                       :relation_kind "supports"
                       :direction "directed"
                       :status "active"
                       :confidence 0.7
                       :scope #js {:project "devel"}
                       :valid_until nil}]
        result (mongo/project-mongo-edge-claims-js rows #js {:now "2026-01-01T00:00:00.000Z"})
        edges (aget result "edges")]
    (is (= 1 (aget (aget result "stats") "edges")))
    (is (= "edge_claim:row" (aget (aget edges 0) "claim_id")))))

(deftest boundary-normalizes-create-input-with-canonical-undirected-storage
  (let [normalized (boundary/normalize-edge-claim-input-js
                     #js {:source_node_id "node:b"
                          :target_node_id "node:a"
                          :relation_kind "supports"
                          :direction "undirected"
                          :status "supported"
                          :confidence "0.8"
                          :scope #js {:project "devel"}})]
    (is (= "node:a" (aget normalized "source_node_id")))
    (is (= "node:b" (aget normalized "target_node_id")))
    (is (= "supported" (aget normalized "status")))
    (is (= 0.8 (aget normalized "confidence")))
    (is (re-matches #"edge_claim:[a-f0-9]{24}" (aget normalized "claim_id")))))

(deftest boundary-exposes-validation-and-policy-decisions
  (let [claim #js {:claim_id "edge_claim:three"
                   :source_node_id "node:a"
                   :target_node_id "node:b"
                   :relation_kind "supports"
                   :direction "directed"
                   :status "supported"
                   :confidence 1}
        explanation (boundary/explain-edge-claim-js claim)
        decision (boundary/evaluate-edge-claim-js claim)]
    (is (true? (aget explanation "valid?")))
    (is (= "accept" (aget decision "kind")))
    (is (= "projectable-status" (aget decision "reason")))))

(deftest lifecycle-plans-route-transition-updates
  (let [support (lifecycle/transition-plan-js "support" #js {:status "active"
                                                             :confidence "0.9"
                                                             :event_ids #js ["event:1" "event:1" "event:2"]})
        refute (lifecycle/transition-plan-js "refute" #js {:eventIds #js ["event:3"]})
        withdraw (lifecycle/transition-plan-js "withdraw" #js {})]
    (is (= "active" (aget support "status")))
    (is (= 0.9 (aget support "confidence")))
    (is (= "support_event_ids" (aget support "eventField")))
    (is (= 2 (.-length (aget support "eventIds"))))
    (is (= "refuted" (aget refute "status")))
    (is (= "refute_event_ids" (aget refute "eventField")))
    (is (= "withdrawn" (aget withdraw "status")))
    (is (nil? (aget withdraw "eventField")))))

(defn -main []
  (let [result (run-tests 'openplanner.graph.claims.core-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (js/process.exit 1))))

(deftest mongo-id-only-rows-preserve-the-stored-identity
  (let [rows #js [#js {:_id "stored-only-id" :source_node_id "a" :target_node_id "b"
                       :relation_kind "supports" :status "supported" :confidence 0.8
                       :tenant_id "tenant-a"}]
        result (mongo/project-mongo-edge-claims-js rows #js {:now "2026-01-01T00:00:00.000Z"})]
    (is (= "stored-only-id" (aget result "edges" 0 "claim_id")))
    (is (= "tenant-a" (aget result "edges" 0 "scope" "tenant_id")))))

(deftest mongo-object-id-never-replaces-an-explicit-claim-identity
  (let [row #js {:_id #js {:objectId "mongo-storage-id"}
                 :claim_id "explicit-claim-id"
                 :source_node_id "a" :target_node_id "b"
                 :relation_kind "supports" :status "supported" :confidence 0.8}
        result (mongo/project-mongo-edge-claims-js #js [row] #js {:now "2026-01-01T00:00:00.000Z"})]
    (is (= 1 (aget result "stats" "edges")))
    (is (= "explicit-claim-id" (aget result "edges" 0 "claim_id")))))

(deftest nested-scope-identity-is-recursive-and-order-independent
  (let [base {:source_node_id "node:a" :target_node_id "node:b" :relation_kind "related_to"}
        id (fn [scope] (boundary/build-edge-claim-id (clj->js (assoc base :scope scope))))]
    (is (not= (id {:constraints {:tenant "a"}}) (id {:constraints {:tenant "b"}})))
    (is (= (id {:constraints {:b 2 :a 1} :values [{:y 2 :x 1}]})
           (id {:values [{:x 1 :y 2}] :constraints {:a 1 :b 2}})))))

(deftest unknown-lifecycle-actions-never-promote-a-claim
  (doseq [action ["supprt" "" nil "delete"]]
    (is (thrown? js/Error (lifecycle/transition-plan action #js {})))))

(deftest projection-validates-even-explicit-claim-identities
  (let [base {:claim-id "explicit" :source-node-id "a" :target-node-id "b"
              :relation-kind "supports" :direction :directed :scope-json "{}"
              :status :active :confidence 0.8}
        opts {:now-ms 1000}]
    (doseq [change [{:source-node-id nil} {:target-node-id "a"} {:confidence 2}
                    {:direction :unknown} {:scope-json nil} {:scope []}]]
      (is (nil? (claims/claim->projected-edge (merge base change) opts))))
    (doseq [row [#js {:_id "explicit" :target_node_id "b" :status "active"}
                 #js {:_id "explicit" :source_node_id "a" :target_node_id "a" :status "supported"}]]
      (is (empty? (array-seq (aget (mongo/project-mongo-edge-claims-js #js [row] #js {:now 1000}) "edges")))))))

(deftest confidence-defaults-preserve-explicit-zero
  (doseq [value [nil js/undefined]]
    (is (= 0.5 (aget (boundary/normalize-edge-claim-input-js #js {:confidence value}) "confidence")))
    (is (= 0.75 (:confidence (lifecycle/transition-plan "support" #js {:confidence value})))))
  (is (= 0 (aget (boundary/normalize-edge-claim-input-js #js {:confidence 0}) "confidence"))))

(deftest malformed-supplied-claim-expiration-is-never-unbounded
  (doseq [value ["invalid-time" (js/Date. "invalid") js/Infinity false]]
    (let [claim #js {:source_node_id "a" :target_node_id "b" :relation_kind "related" :status "active" :validUntil value}]
      (is (nil? (boundary/project-edge-claim-js claim #js {:now 1000})))
      (is (false? (aget (boundary/explain-edge-claim-js claim) "valid?"))))))

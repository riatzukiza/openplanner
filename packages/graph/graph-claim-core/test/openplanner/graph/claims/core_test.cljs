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


(deftest explicit-claim-scope-rejects-runtime-objects-before-identity
  (doseq [scope [(js/Date.) (js/Map.) (js/Set.) (js/RegExp. "x")]]
    (is (thrown-with-msg? js/Error #"scope.*plain|plain.*scope"
          (boundary/build-edge-claim-id #js {:source "a" :target "b" :scope scope}))))
  (let [scope (js/Object.create nil)]
    (aset scope "project" "owned")
    (is (= (boundary/build-edge-claim-id #js {:source "a" :target "b" :scope scope})
           (boundary/build-edge-claim-id #js {:source "a" :target "b" :scope #js {:project "owned"}})))))

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

(deftest graph-key-boundary-refuses-nonstring-identifiers
  (doseq [field ["source_node_id" "target_node_id" "relation_kind"]
          value [42 #js {:id "malformed"} true false #js ["malformed"]]]
    (let [claim #js {:claim_id "explicit" :source_node_id "a" :target_node_id "b"
                     :relation_kind "related" :status "active"}]
      (aset claim field value)
      (is (false? (aget (boundary/explain-edge-claim-js claim) "valid?")))
      (is (nil? (boundary/project-edge-claim-js claim #js {:now 1000})))))
  (is (= "related_to" (aget (boundary/normalize-edge-claim-input-js
                            #js {:source_node_id " a " :target_node_id " b "}) "relation_kind"))))


(deftest supplied-claim-identities-are-never-replaced-by-generated-identities
  (doseq [field ["claimId" "claim_id"]
          value [42 #js {:id "malformed"} true false #js ["malformed"] nil js/undefined "" "  "]]
    (let [claim #js {:source_node_id "a" :target_node_id "b" :relation_kind "related" :status "active"}]
      (aset claim field value)
      (is (false? (aget (boundary/explain-edge-claim-js claim) "valid?")))
      (is (nil? (boundary/project-edge-claim-js claim #js {:now 1000})))) )
  (let [claim #js {:claimId false :claim_id "valid-other-alias" :source_node_id "a"
                   :target_node_id "b" :relation_kind "related" :status "active"}]
    (is (false? (aget (boundary/explain-edge-claim-js claim) "valid?")))
    (is (nil? (boundary/project-edge-claim-js claim #js {:now 1000}))))
  (let [claim #js {:source_node_id "a" :target_node_id "b" :relation_kind "related" :status "active"}]
    (is (re-matches #"edge_claim:[a-f0-9]{24}" (aget (boundary/project-edge-claim-js claim #js {:now 1000}) "claim_id")))) )


(deftest mongo-normalization-preserves-explicit-malformed-identity-presence
  (doseq [field ["claim_id" "claimId"] value [false nil]]
    (let [row #js {:_id "storage-fallback" :source_node_id "a" :target_node_id "b"
                   :relation_kind "related" :status "active"}]
      (aset row field value)
      (is (zero? (aget (mongo/project-mongo-edge-claims-js #js [row] #js {:now 1000}) "stats" "edges")))))
  (let [row #js {:source_node_id "a" :target_node_id "b" :relation_kind "related" :status "active"}
        result (mongo/project-mongo-edge-claims-js #js [row] #js {:now 1000})
        edge (aget (aget result "edges") 0)
        claim-id (when edge (aget edge "claim_id"))]
    (is (and (string? claim-id) (re-matches #"edge_claim:[a-f0-9]{24}" claim-id)))))


(deftest conflicting-explicit-claim-aliases-remain-ambiguous
  (let [base #js {:claimId "camel" :claim_id "snake" :source_node_id "a"
                  :target_node_id "b" :relation_kind "related" :status "active"}]
    (is (false? (aget (boundary/explain-edge-claim-js base) "valid?")))
    (is (nil? (boundary/project-edge-claim-js base #js {:now 1000})))
    (is (zero? (aget (mongo/project-mongo-edge-claims-js #js [base] #js {:now 1000}) "stats" "edges"))))
  (let [base #js {:claimId "same" :claim_id "same" :source_node_id "a"
                  :target_node_id "b" :relation_kind "related" :status "active"}]
    (is (= "same" (aget (boundary/project-edge-claim-js base #js {:now 1000}) "claim_id")))))

(deftest lifecycle-evidence-retains-only-nonblank-string-identities
  (doseq [action ["support" "refute"]]
    (is (thrown-with-msg? js/Error #"evidence"
          (lifecycle/transition-plan-js action
            #js {:event_ids #js [42 #js {:id "malformed"} nil true #js ["array"]
                                "" "  " " valid " "valid" "other"]})))
    (let [result (lifecycle/transition-plan-js action
                  #js {:event_ids #js [" valid " "valid" "other"]})]
      (is (= ["valid" "other"] (js->clj (aget result "eventIds")))))))

(deftest default-policy-and-projection-share-expiration-boundary
  (let [old-now (.-now js/Date)]
    (try
      (set! (.-now js/Date) (fn [] 1000))
      (doseq [status ["active" "supported"]
              [expires kind] [[999 "reject"] [1000 "reject"] [1001 "accept"] [nil "accept"]]]
        (let [claim #js {:claim_id "timed" :source_node_id "a" :target_node_id "b"
                         :relation_kind "related" :status status :validUntil expires}]
          (is (= kind (aget (boundary/evaluate-edge-claim-js claim) "kind")))
          (is (= (= "accept" kind) (some? (boundary/project-edge-claim-js claim #js {:now 1000}))))))
      (finally (set! (.-now js/Date) old-now)))))

(deftest pure-default-policy-requires-explicit-time-for-expiring-claims
  (let [claim {:claim-id "edge_claim:explicit-clock"
               :source-node-id "node:a" :target-node-id "node:b"
               :relation-kind "supports" :direction :directed :scope-json "{}"
               :scope {} :confidence 0.8 :status :active :valid-until-ms 1000}]
    (is (= :missing-evaluation-time (:decision/reason (policy/evaluate-claim claim))))
    (is (= :accept (:decision/kind (policy/evaluate-claim claim [] {:now-ms 999}))))
    (is (= :expired-claim (:decision/reason (policy/evaluate-claim claim [] {:now-ms 1000}))))
    (is (= :reject (:decision/kind (policy/evaluate-claim claim [] {:now-ms 1001}))))))

(deftest every-supplied-graph-key-alias-must-agree
  (doseq [[camel snake] [["sourceNodeId" "source_node_id"]
                         ["targetNodeId" "target_node_id"]
                         ["relationKind" "relation_kind"]]]
    (let [base #js {:claim_id "edge_claim:key-alias" :source_node_id "a"
                    :target_node_id "b" :relation_kind "supports" :status "active"}
          invalid (js/Object.assign #js {} base)
          valid (js/Object.assign #js {} base)]
      (aset invalid camel "different")
      (aset valid camel (aget base snake))
      (is (false? (aget (boundary/explain-edge-claim-js invalid) "valid?")))
      (is (nil? (boundary/project-edge-claim-js invalid)))
      (is (= 0 (aget (aget (mongo/project-mongo-edge-claims-js #js [invalid]) "stats") "edges")))
      (is (true? (aget (boundary/explain-edge-claim-js valid) "valid?"))))))

(deftest all-undirected-projection-paths-use-canonical-endpoints
  (let [reverse #js {:source_node_id "z" :target_node_id "a" :relation_kind "supports"
                     :direction "undirected" :status "active"}
        forward #js {:source_node_id "a" :target_node_id "z" :relation_kind "supports"
                     :direction "undirected" :status "active"}
        left (boundary/project-edge-claim-js reverse)
        right (boundary/project-edge-claim-js forward)
        stored (aget (aget (mongo/project-mongo-edge-claims-js #js [reverse]) "edges") 0)]
    (is (= "a" (aget left "source")))
    (is (= "z" (aget left "target")))
    (is (= (js->clj right) (js->clj left)))
    (is (= (js->clj left) (js->clj stored)))))

(deftest every-supplied-lifecycle-evidence-alias-must-agree
  (doseq [[action specific] [["support" "support_event_ids"] ["refute" "refute_event_ids"]]
          [left right] [["event_ids" "eventIds"] ["event_ids" specific] ["eventIds" specific]]]
    (let [bad #js {} good #js {}]
      (aset bad left #js [])
      (aset bad right #js ["event:1"])
      (aset good left #js [" event:1 " "event:1"])
      (aset good right #js ["event:1"])
      (is (thrown-with-msg? js/Error #"evidence.*aliases|aliases.*evidence"
            (lifecycle/transition-plan-js action bad)))
      (is (= ["event:1"] (js->clj (aget (lifecycle/transition-plan-js action good) "eventIds")))))))


(deftest mongo-camel-expiration-matches-direct-projection
  (doseq [status ["supported" "active"]]
    (let [row #js {:claimId "edge_claim:camel-expiry" :sourceNodeId "node:a"
                   :targetNodeId "node:b" :relationKind "supports" :direction "directed"
                   :status status :confidence 0.8 :validUntil "2025-01-01T00:00:00.000Z"}
          opts #js {:now "2026-01-01T00:00:00.000Z"}
          direct (boundary/project-edge-claims-js #js [row] opts)
          from-mongo (mongo/project-mongo-edge-claims-js #js [row] opts)]
      (is (= 0 (alength (aget direct "edges"))))
      (is (= 0 (alength (aget from-mongo "edges"))))
      (is (= (aget (aget direct "stats") "edges")
             (aget (aget from-mongo "stats") "edges")))
      (aset row "validUntil" "2099-01-01T00:00:00.000Z")
      (is (= 1 (alength (aget (mongo/project-mongo-edge-claims-js #js [row] opts) "edges")))))))

(deftest expiration-wire-null-matches-typescript-contract
  (let [wire (boundary/normalize-edge-claim-input-js
               #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"})]
    (is (.hasOwn js/Object wire "valid_until_ms"))
    (is (nil? (aget wire "valid_until_ms")))))

;; Actual native Codex5481490146/4239864679 and4239864689, current2b source.
(deftest explicit-expiration-aliases-must-agree
  (doseq [[camel snake] [["2099-01-01T00:00:00Z" "2025-01-01T00:00:00Z"]
                        [nil "2099-01-01T00:00:00Z"]
                        ["2099-01-01T00:00:00Z" nil]]]
    (let [claim #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"
                     :status "active" :validUntil camel :valid_until snake}]
      (is (false? (aget (boundary/explain-edge-claim-js claim) "valid?")))
      (is (nil? (boundary/project-edge-claim-js claim #js {:now "2026-01-01"})))
      (is (zero? (alength (aget (mongo/project-mongo-edge-claims-js #js [claim] #js {:now "2026-01-01"}) "edges"))))))
  (let [claim #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"
                   :status "active" :validUntil "2099-01-01T00:00:00Z"
                   :valid_until (js/Date.parse "2099-01-01T00:00:00Z")}]
    (is (some? (boundary/project-edge-claim-js claim #js {:now "2026-01-01"})))))

(deftest malformed-explicit-projection-statuses-never-become-defaults
  (let [claim #js {:source_node_id "a" :target_node_id "b" :relation_kind "related" :status "supported"}]
    (doseq [statuses ["active" nil true #js {:status "active"} #js ["active" "unknown"]]]
      (let [opts #js {:statuses statuses :now 1000}]
        (is (thrown-with-msg? js/Error #"statuses" (boundary/project-edge-claim-js claim opts)))
        (is (thrown-with-msg? js/Error #"statuses" (boundary/project-edge-claims-js #js [claim] opts)))
        (is (thrown-with-msg? js/Error #"statuses" (mongo/project-mongo-edge-claims-js #js [claim] opts)))))
    (is (nil? (boundary/project-edge-claim-js claim #js {:statuses #js [] :now 1000})))
    (is (nil? (boundary/project-edge-claim-js claim #js {:statuses #js ["active"] :now 1000})))
    (is (some? (boundary/project-edge-claim-js claim #js {:now 1000})))))

;; Actual native Codex5481523582 lifecycle evidence and projection clock contracts.
(deftest every-explicit-lifecycle-evidence-value-is-admitted-completely
  (doseq [[action fields] [["support" ["event_ids" "eventIds" "support_event_ids"]]
                           ["refute" ["event_ids" "eventIds" "refute_event_ids"]]]
          field fields
          value ["event:1" nil #js ["event:1" 17] #js ["event:1" " "]]]
    (let [body #js {}]
      (aset body field value)
      (is (thrown-with-msg? js/Error #"evidence" (lifecycle/transition-plan-js action body)))))
  (is (= [] (js->clj (aget (lifecycle/transition-plan-js "support" #js {}) "eventIds")))))

(deftest explicit-projection-time-never-falls-back-to-the-clock
  (let [claim #js {:source_node_id "a" :target_node_id "b" :relation_kind "related" :status "active"}]
    (doseq [now ["not-a-date" nil false js/NaN js/Infinity]]
      (let [opts #js {:now now}]
        (is (thrown-with-msg? js/Error #"projection.*time|time.*projection" (boundary/project-edge-claim-js claim opts)))
        (is (thrown-with-msg? js/Error #"projection.*time|time.*projection" (mongo/project-mongo-edge-claims-js #js [claim] opts)))))
    (is (some? (boundary/project-edge-claim-js claim #js {:now 0})))
    (is (some? (boundary/project-edge-claim-js claim #js {})))))
;; Native Codex5481938999: complete JSON scope admission and injective tuple identity.
(deftest nested-runtime-scope-values-never-acquire-claim-authority
  (let [cycle #js {}]
    (aset cycle "again" cycle)
    (doseq [value [(js/Date.) (js/Map.) (js/Set.) (js/RegExp. "x")
                   #js {:missing js/undefined} #js {:method (fn [] true)}
                   #js {:number js/NaN} #js {:number js/Infinity}
                   #js {:symbol (js/Symbol "scope")} cycle
                   #js [#js {:nested (js/Date.)}]]]
      (let [claim #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"
                       :status "active" :scope #js {:nested value}}]
        (is (thrown-with-msg? js/Error #"scope.*JSON|JSON.*scope"
              (boundary/build-edge-claim-id-js claim)))
        (is (thrown-with-msg? js/Error #"scope.*JSON|JSON.*scope"
              (boundary/normalize-edge-claim-input-js claim)))
        (is (thrown-with-msg? js/Error #"scope.*JSON|JSON.*scope"
              (boundary/project-edge-claim-js claim)))))))

(deftest valid-nested-json-scope-preserves-values-and-key-order-independent-identity
  (let [a #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"
                :scope #js {:nested #js {:z #js [nil true 2 "x"] :a #js {:x "y"}}}}
        b #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"
                :scope #js {:nested #js {:a #js {:x "y"} :z #js [nil true 2 "x"]}}}]
    (is (= (boundary/build-edge-claim-id-js a) (boundary/build-edge-claim-id-js b)))
    (is (= (js->clj (aget a "scope"))
           (js->clj (aget (boundary/normalize-edge-claim-input-js a) "scope"))))))

(deftest unrestricted-claim-strings-cannot-collide-through-tuple-delimiters
  (let [a #js {:source_node_id "a\nb" :target_node_id "c" :relation_kind "related"}
        b #js {:source_node_id "a" :target_node_id "b\nc" :relation_kind "related"}]
    (is (not= (boundary/build-edge-claim-id-js a) (boundary/build-edge-claim-id-js b))))
  (let [a {:source-node-id "a" :target-node-id "b\nc" :relation-kind "d"
           :direction :directed :scope-json "{}"}
        b (assoc a :target-node-id "b" :relation-kind "c\nd")]
    (is (not= (claims/claim-id-material a) (claims/claim-id-material b)))))

(deftest claim-timestamps-reject-non-iso-and-impossible-calendar-values
  (doseq [value ["2021" "02/01/2021" "2021-02-30" "2021-02-30T00:00:00Z"
                 "2023-02-29T00:00:00Z" "2021-04-31T00:00:00Z"
                 "2021-01-01T24:00:00Z" "2021-01-01T00:60:00Z"
                 "2021-01-01T00:00:60Z" "2021-01-01T00:00:00+24:00"
                 "2021-01-01T00:00:00+00:60" "2021-01-01T00:00:00"]]
    (let [claim #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"
                     :status "active" :validUntil value}]
      (is (false? (aget (boundary/explain-edge-claim-js claim) "valid?")))
      (is (nil? (boundary/project-edge-claim-js claim #js {:now 0})))
      (is (thrown-with-msg? js/Error #"valid timestamp"
            (boundary/projection-options-from-js #js {:now value})))
      (is (thrown-with-msg? js/Error #"valid timestamp"
            (mongo/project-mongo-edge-claims-js #js [claim] #js {:now value}))))))

(deftest claim-timestamps-preserve-iso-dates-instants-and-numeric-date-inputs
  (doseq [value ["2024-02-29" "2024-02-29T12:34:56Z"
                 "2024-02-29T12:34:56.123Z" "2024-02-29T12:34:56+05:30"
                 "+002024-02-29T12:34:56Z" 1000 (js/Date. 1000)]]
    (let [ms (if (string? value) (js/Date.parse value)
              (if (number? value) value (.getTime value)))
          claim #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"
                     :status "active" :validUntil value}]
      (is (true? (aget (boundary/explain-edge-claim-js claim) "valid?")))
      (is (= ms (:now-ms (boundary/projection-options-from-js #js {:now value}))))
      (is (some? (boundary/project-edge-claim-js claim #js {:now (dec ms)})))
      (is (nil? (boundary/project-edge-claim-js claim #js {:now ms}))))))

(deftest explicit-top-level-scope-preserves-json-values-without-broadening
  (let [base #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"}
        unscoped (boundary/build-edge-claim-id-js base)]
    (doseq [value [nil "" "   " false 0]]
      (let [claim (js/Object.assign #js {} base #js {:scope #js {:tenant_id value}})
            normalized (boundary/normalize-edge-claim-input-js claim)]
        (is (not= unscoped (boundary/build-edge-claim-id-js claim)))
        (is (some? (aget normalized "scope")))
        (when (some? (aget normalized "scope"))
          (is (js/Object.hasOwn (aget normalized "scope") "tenant_id"))
          (is (= value (aget (aget normalized "scope") "tenant_id"))))))
    (let [invalid (js/Object.assign #js {} base #js {:scope #js {:tenant_id js/undefined}})]
      (is (thrown-with-msg? js/Error #"scope.*JSON|JSON.*scope"
            (boundary/build-edge-claim-id-js invalid)))
      (is (thrown-with-msg? js/Error #"scope.*JSON|JSON.*scope"
            (boundary/normalize-edge-claim-input-js invalid))))
    (is (= unscoped (boundary/build-edge-claim-id-js
                     (js/Object.assign #js {} base #js {:tenant_id js/undefined}))))
    (is (= unscoped (boundary/build-edge-claim-id-js
                     (js/Object.assign #js {} base #js {:tenant_id "inferred" :scope #js {}}))))))


(deftest mongo-and-direct-inferred-scope-share-canonical-identity
  (doseq [field ["tenant_id" "org_id" "project" "lake" "graph_version"]
          value [nil "" "   " false 0 "owned"]]
    (let [row #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"
                   :status "active" :confidence 0.8}
          _ (aset row field value)
          opts #js {:now 1000}
          direct (boundary/project-edge-claims-js #js [row] opts)
          stored (mongo/project-mongo-edge-claims-js #js [row] opts)]
      (is (= 1 (alength (aget direct "edges"))))
      (is (= (js->clj (aget direct "edges")) (js->clj (aget stored "edges")))
          (str "inferred " field "=" (pr-str value))))))

(deftest mongo-explicit-scope-presence-does-not-fall-back-to-inference
  (doseq [scope [nil #js {} #js {:tenant_id nil} #js {:tenant_id ""}
                #js {:tenant_id false} #js {:tenant_id 0}]]
    (let [row #js {:source_node_id "a" :target_node_id "b" :relation_kind "related"
                   :status "active" :tenant_id "must-not-be-inferred" :scope scope}
          opts #js {:now 1000}]
      (is (= (js->clj (aget (boundary/project-edge-claims-js #js [row] opts) "edges"))
             (js->clj (aget (mongo/project-mongo-edge-claims-js #js [row] opts) "edges")))))))


(deftest mongo-confidence-preserves-explicit-false-and-alias-presence
  (let [row {:source_node_id "a" :target_node_id "b" :relation_kind "supports"
             :status "active" :confidence false}
        direct (boundary/edge-claim-from-js (clj->js row))
        normalized (mongo/edge-claim-from-mongo row)]
    (is (= 0 (:confidence direct)))
    (is (= (:confidence direct) (:confidence normalized)))
    (is (= 0 (:confidence (mongo/edge-claim-from-mongo (assoc row "confidence" 0.9)))))
    (is (= 0.5 (:confidence (mongo/edge-claim-from-mongo (assoc row :confidence nil "confidence" 0.9)))))
    (is (= 0 (aget (first (array-seq (aget (mongo/project-mongo-edge-claims-js
                                         #js [(clj->js row)] #js {:now 1000}) "edges")))
                  "confidence")))))

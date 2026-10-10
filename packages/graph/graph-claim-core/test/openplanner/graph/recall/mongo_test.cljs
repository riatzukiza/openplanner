(ns openplanner.graph.recall.mongo-test
  "Actual owning adapter over held SDK collection ports, with no live services."
  (:require [cljs.test :refer [deftest is]]
            [clojure.string :as str]
            [openplanner.graph.recall.core-test :as fixture]
            [openplanner.graph.recall.mongo :as mongo]))

(def authority
  {:scope fixture/scope :project "creator-local"
   :records [{:id "seed" :text "outside harbor encounter"}
             {:id "neighbor" :text "GRAPH ONLY: bells under the harbor"}]})

(def indices
  [{:source_text_hash_sha256 "55cc7a7b15a1bfd946f2d9322c99a3474997884641378e21cbb1db6276fa11c2" :_id "index:seed" :node_id "seed" :source_event_id "seed" :project "creator-local"
    :embedding_model "held-model" :embedding_dimensions 2 :embedding [1.0 0.0] :chunk_index 0}
   {:source_text_hash_sha256 "24f2f62df26cba43e9b24ae5d3b2a211fd1b72c28af9d8c6321eb1757f8c224c" :_id "index:neighbor" :node_id "neighbor" :source_event_id "neighbor" :project "creator-local"
    :embedding_model "held-model" :embedding_dimensions 2 :embedding [0.0 1.0] :chunk_index 0}])

(def edges
  [{:_id "edge:seed:neighbor" :source_node_id "seed" :target_node_id "neighbor"
    :project "creator-local" :data {:scoped_recall {:version 1 :org_id "org-local"
                                                   :project "creator-local" :character_id "creator"
                                                   :provenance_event_ids ["seed" "neighbor"] :cost 1}}}])

(defn- collection [state name rows*]
  #js {:find (fn [filter]
               (swap! (:reads* state) conj {:collection name :filter (js->clj filter :keywordize-keys true)})
               (let [cursor #js {} maximum* (atom nil)
                     query (js->clj filter :keywordize-keys true)]
                 (aset cursor "sort" (fn [_order] cursor))
                 (aset cursor "limit" (fn [limit] (reset! maximum* limit) (swap! (:limits* state) conj limit) cursor))
                 (aset cursor "maxTimeMS" (fn [limit] (swap! (:timeouts* state) conj limit) cursor))
                 (aset cursor "toArray" (fn []
                                         (when-let [error @(:error* state)] (throw error))
                                         (when-let [hook (get @(:read-hooks* state) name)] (hook))
                                         (if (not= :none @(:rejection* state))
                                           (js/Promise.reject @(:rejection* state))
                                           ;; Held cursor recognizes this one query profile;
                                           ;; real Mongo predicate compatibility is a separate gate.
                                           (let [ids (get-in query [:data.scoped_recall.provenance_event_ids :$not :$elemMatch :$nin])
                                                 rows (if ids
                                                        (filterv #(let [evidence (get-in % [:data :scoped_recall :provenance_event_ids])]
                                                                    (and (vector? evidence) (seq evidence)
                                                                         (every? (set ids) evidence))) @rows*) @rows*)]
                                             (js/Promise.resolve
                                              (clj->js (vec (take @maximum*
                                                                (if (= {:$eq ["$node_id" "$source_event_id"]} (:$expr query))
                                                                  (filterv #(= (:node_id %) (:source_event_id %)) rows)
                                                                  rows)))))))))
                 cursor))
       :updateOne (fn [& _arguments] (swap! (:writes* state) inc) (throw (js/Error. "No writes allowed")))
       :insertOne (fn [& _arguments] (swap! (:writes* state) inc) (throw (js/Error. "No writes allowed")))})

(defn- state []
  {:authority* (atom authority) :authority-calls* (atom 0)
   :indices* (atom indices) :edges* (atom edges) :reads* (atom [])
   :read-hooks* (atom {}) :embedding-hook* (atom nil)
   :rejection* (atom :none) :model-scopes* (atom []) :model-overrides* (atom {})
   :limits* (atom []) :timeouts* (atom []) :embeddings* (atom []) :writes* (atom 0) :error* (atom nil)})

(defn- reader [state]
  (mongo/create-scoped-mongo-recall-js
   #js {:mongo #js {:graphNodeEmbeddings (collection state :indices (:indices* state))
                   :graphEdges (collection state :edges (:edges* state))}
        :embeddingRuntime #js {:hot #js {:getModel (fn [raw-scope]
                                                   (let [scope (js->clj raw-scope :keywordize-keys true)]
                                                     (swap! (:model-scopes* state) conj scope)
                                                     (get @(:model-overrides* state) (:source scope) "held-model")))
                                        :getEmbeddingFunctionForModel
                                          (fn [model]
                                            #js {:generate (fn [texts]
                                                             (swap! (:embeddings* state) conj
                                                                    {:model model :texts (js->clj texts)})
                                                             (when-let [hook @(:embedding-hook* state)] (hook))
                                                             (js/Promise.resolve #js [#js [1.0 0.0]]))})}}}
   (fn [] (swap! (:authority-calls* state) inc) (js/Promise.resolve (clj->js @(:authority* state))))
   (fn [query] (str "configured query: " (str/trim query)))))

(defn- ^:async recall! [reader request]
  (js->clj (await (reader (clj->js request))) :keywordize-keys true))

(deftest ^:async consuming-wire-boundaries-validate-the-owning-result-shape
  (let [result (await (recall! (reader (state)) fixture/request))]
    (is (mongo/valid-scoped-result-js? (clj->js result)))
    (doseq [invalid [nil {} (assoc result :scope fixture/scope)
                     (assoc-in result [:selection :status] "unknown")
                     (update-in result [:selection :hits 0] dissoc :path)
                     (assoc-in result [:selection :feedback :completed] 1)]]
      (is (false? (mongo/valid-scoped-result-js? (clj->js invalid)))))))

(deftest ^:async fresh-authority-precedes-scoped-storage-ranking-and-traversal
  (let [state (state) result (await (recall! (reader state) fixture/request))
        selection (:selection result)]
    (is (= "completed" (:status selection)))
    (is (= ["seed" "neighbor"] (mapv :id (:hits selection))))
    (is (= ["seed" "neighbor"] (get-in selection [:hits 1 :path])))
    (is (= ["edge:seed:neighbor"] (get-in selection [:hits 1 :path-edge-ids])))
    (is (= false (get-in selection [:hits 1 :seed?])))
    (is (= 4 @(:authority-calls* state)) "Recheck after each index, embedding and graph await")
    (is (= #{"seed" "neighbor"}
           (set (get-in @(:reads* state) [0 :filter :source_event_id :$in]))))
    (is (= [{:model "held-model" :texts ["configured query: harbor"]}] @(:embeddings* state)))
    (is (= "not-loaded" (:field-status result)) "No invented physical field proof")
    (is (= 0 @(:writes* state)))
    (is (= 0 (get-in selection [:feedback :completed])))))

(deftest ^:async each-call-resolves-current-authority-without-reusing-old-grants
  (let [state (state) read! (reader state)
        before (await (recall! read! fixture/request))]
    (reset! (:authority* state) (update authority :records #(vec (take 1 %))))
    (let [after (await (recall! read! fixture/request))]
      (is (= 8 @(:authority-calls* state)))
      (is (= 2 (count (get-in before [:selection :hits]))))
      (is (= ["seed"] (mapv :id (get-in after [:selection :hits]))))
      (is (not (str/includes? (pr-str after) "GRAPH ONLY"))))))

(deftest ^:async payload-authority-is-rejected-before-any-host-or-storage-call
  (let [state (state) result (await (recall! (reader state) (assoc fixture/request :scope fixture/scope)))]
    (is (= "invalid-request" (get-in result [:selection :failure :code])))
    (is (= 0 @(:authority-calls* state)))
    (is (= [] @(:reads* state)))
    (is (= [] @(:embeddings* state)))))

(deftest ^:async denied-principal-never-opens-graph-storage-or-embeddings
  (let [state (state)]
    (reset! (:authority* state) nil)
    (let [result (await (recall! (reader state) fixture/request))]
      (is (= "denied" (get-in result [:selection :status])))
      (is (= [] (get-in result [:selection :hits])))
      (is (= [] @(:reads* state)))
      (is (= [] @(:embeddings* state))))))

(deftest ^:async event-admission-is-not-index-readiness
  (let [state (state)]
    (reset! (:indices* state) [])
    (let [result (await (recall! (reader state) fixture/request))]
      (is (= "indexing-pending" (get-in result [:selection :status])))
      (is (= [] (get-in result [:selection :hits])))
      (is (= [] @(:embeddings* state)))
      (is (= 0 @(:writes* state))))))

(deftest ^:async graph-storage-failure-stays-failed-without-private-error-text
  (let [state (state)]
    (reset! (:error* state) (js/Error. "PRIVATE_CREDENTIAL_AND_SOURCE_TEXT"))
    (let [result (await (recall! (reader state) fixture/request))]
      (is (= "failed" (get-in result [:selection :status])))
      (is (= "transport-error" (get-in result [:selection :failure :code])))
      (is (= [] (get-in result [:selection :hits])))
      (is (not (str/includes? (pr-str result) "PRIVATE_CREDENTIAL")))
      (is (= 0 @(:writes* state))))))

(deftest ^:async foreign-indices-cannot-change-ranking-or-open-a-model-route
  (let [state (state) read! (reader state)
        before (await (recall! read! fixture/request))]
    (swap! (:indices* state) conj
           (assoc (first indices) :_id "foreign-index" :node_id "foreign"
                  :source_event_id "foreign" :embedding_model "FOREIGN_MODEL"
                  :embedding [js/NaN js/NaN]))
    (let [after (await (recall! read! fixture/request))]
      (is (= before after))
      (is (= ["held-model" "held-model"] (mapv :model @(:embeddings* state))))
      (is (not (str/includes? (pr-str after) "FOREIGN_MODEL"))))))

(deftest ^:async hidden-edge-provenance-never-connects-admitted-endpoints
  (let [state (state)]
    (reset! (:edges* state) [(assoc-in (first edges) [:data :scoped_recall :provenance_event_ids] ["foreign"])])
    (let [result (await (recall! (reader state) fixture/request))]
      (is (= ["seed"] (mapv :id (get-in result [:selection :hits]))))
      (is (= 0 (get-in result [:selection :diagnostics :admitted-edges])))
      (is (= 0 @(:writes* state))))))

(deftest ^:async ambiguous-current-identity-fails-before-storage
  (let [state (state)]
    (swap! (:authority* state) update :records conj (first (:records authority)))
    (let [result (await (recall! (reader state) fixture/request))]
      (is (= "invalid-authority" (get-in result [:selection :failure :code])))
      (is (= [] @(:reads* state)))
      (is (= [] @(:embeddings* state))))))

(deftest ^:async storage-overflow-is-not-a-partial-successful-snapshot
  (let [state (state)]
    (reset! (:indices* state) (vec (repeat 1537 (first indices))))
    (let [result (await (recall! (reader state) fixture/request))]
      (is (= "storage-limit-exceeded" (get-in result [:selection :failure :code])))
      (is (= [] (get-in result [:selection :hits])))
      (is (= [] @(:embeddings* state))))))

(deftest ^:async cross-id-indices-cannot-consume-the-admitted-row-budget
  (let [state (state) read! (reader state)
        before (await (recall! read! fixture/request))
        excluded (mapv (fn [dimension]
                         {:_id (str "neighbor::held-model::" dimension "::0")
                          :node_id "neighbor" :source_event_id "seed" :project "creator-local"
                          :embedding_model "held-model" :embedding_dimensions dimension
                          :embedding (into [1.0] (repeat (dec dimension) 0.0)) :chunk_index 0})
                       (range 3 1540))]
    (is (= 1537 (count (set (map :_id excluded)))) "Distinct materialized SDK identities")
    (swap! (:indices* state) into excluded)
    (let [after (await (recall! read! fixture/request))]
      (is (= before after) "Excluded node/event bindings cannot change an admitted result")
      (is (= {:$eq ["$node_id" "$source_event_id"]}
             (get-in @(:reads* state) [2 :filter :$expr]))))))

(deftest ^:async revocation-during-query-embedding-prevents-graph-access
  (let [state (state)]
    (reset! (:embedding-hook* state) #(reset! (:authority* state) nil))
    (let [result (await (recall! (reader state) fixture/request))]
      (is (= "authority-changed" (get-in result [:selection :failure :code])))
      (is (= [] (get-in result [:selection :hits])))
      (is (= [:indices] (mapv :collection @(:reads* state))))
      (is (not (str/includes? (pr-str result) "GRAPH ONLY"))))))

(deftest ^:async current-record-change-during-graph-read-prevents-stale-selection
  (let [state (state)]
    (swap! (:read-hooks* state) assoc :edges
           #(swap! (:authority* state) update :records (fn [records] (vec (take 1 records)))))
    (let [result (await (recall! (reader state) fixture/request))]
      (is (= "authority-changed" (get-in result [:selection :failure :code])))
      (is (= [] (get-in result [:selection :hits])))
      (is (not (str/includes? (pr-str result) "GRAPH ONLY"))))))

(deftest ^:async revocation-during-index-read-prevents-even-pending-success
  (let [state (state)]
    (reset! (:indices* state) [])
    (swap! (:read-hooks* state) assoc :indices #(reset! (:authority* state) nil))
    (let [result (await (recall! (reader state) fixture/request))]
      (is (= "authority-changed" (get-in result [:selection :failure :code])))
      (is (= [] (get-in result [:selection :hits])))
      (is (= [] @(:embeddings* state))))))

(deftest ^:async denied-provenance-cannot-consume-the-admitted-edge-budget
  (let [state (state) read! (reader state)
        before (await (recall! read! fixture/request))]
    (swap! (:edges* state) into
           (mapv #(-> (first edges) (assoc :_id (str "foreign:" %))
                      (assoc-in [:data :scoped_recall :provenance_event_ids] ["foreign"])) (range 4097)))
    (let [after (await (recall! read! fixture/request))]
      (is (= before after))
      (is (= {:$type "array" :$ne [] :$not {:$elemMatch {:$nin ["seed" "neighbor"]}}}
             (get-in @(:reads* state) [3 :filter :data.scoped_recall.provenance_event_ids]))))))

(deftest ^:async ready-graph-embeddings-use-the-indexers-model-scope
  (let [state (state)]
    (reset! (:model-overrides* state) {"knoxx" "another-event-model" "graph-event" "held-model"})
    (let [result (await (recall! (reader state) fixture/request))]
      (is (= "completed" (get-in result [:selection :status])))
      (is (= ["seed" "neighbor"] (mapv :id (get-in result [:selection :hits]))))
      (is (= [{:source "graph-event" :kind "graph.node" :project "creator-local"}] @(:model-scopes* state))))))

(deftest ^:async arbitrary-nullish-storage-rejections-remain-bounded-failures
  (doseq [rejection [nil js/undefined]]
    (let [state (state)]
      (reset! (:rejection* state) rejection)
      (let [result (await (recall! (reader state) fixture/request))]
        (is (= "failed" (get-in result [:selection :status])))
        (is (= "transport-error" (get-in result [:selection :failure :code])))
        (is (= [] (get-in result [:selection :hits])))))))

(deftest ^:async stale-or-unbound-embedding-content-is-pending-before-query-ranking
  (doseq [change [(fn [row] (assoc row :source_text_hash_sha256 (apply str (repeat 64 "0"))))
                  (fn [row] (dissoc row :source_text_hash_sha256))]]
    (let [s (state)
          _ (swap! (:indices* s) update 0 change)
          result (await (recall! (reader s) fixture/request))]
      (is (= "indexing-pending" (get-in result [:selection :status])))
      (is (empty? @(:embeddings* s)))
      (is (= [:indices] (mapv :collection @(:reads* s)))))))

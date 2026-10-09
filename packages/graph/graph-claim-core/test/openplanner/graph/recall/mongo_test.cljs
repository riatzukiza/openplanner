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
  [{:_id "index:seed" :node_id "seed" :source_event_id "seed" :project "creator-local"
    :embedding_model "held-model" :embedding_dimensions 2 :embedding [1.0 0.0] :chunk_index 0}
   {:_id "index:neighbor" :node_id "neighbor" :source_event_id "neighbor" :project "creator-local"
    :embedding_model "held-model" :embedding_dimensions 2 :embedding [0.0 1.0] :chunk_index 0}])

(def edges
  [{:_id "edge:seed:neighbor" :source_node_id "seed" :target_node_id "neighbor"
    :project "creator-local" :data {:scoped_recall {:version 1 :org_id "org-local"
                                                   :project "creator-local" :character_id "creator"
                                                   :provenance_event_ids ["seed" "neighbor"] :cost 1}}}])

(defn- collection [state name rows*]
  #js {:find (fn [filter]
               (swap! (:reads* state) conj {:collection name :filter (js->clj filter :keywordize-keys true)})
               (let [cursor #js {}]
                 (aset cursor "sort" (fn [_order] cursor))
                 (aset cursor "limit" (fn [limit] (swap! (:limits* state) conj limit) cursor))
                 (aset cursor "maxTimeMS" (fn [limit] (swap! (:timeouts* state) conj limit) cursor))
                 (aset cursor "toArray" (fn []
                                         (when-let [error @(:error* state)] (throw error))
                                         (js/Promise.resolve (clj->js @rows*))))
                 cursor))
       :updateOne (fn [& _arguments] (swap! (:writes* state) inc) (throw (js/Error. "No writes allowed")))
       :insertOne (fn [& _arguments] (swap! (:writes* state) inc) (throw (js/Error. "No writes allowed")))})

(defn- state []
  {:authority* (atom authority) :authority-calls* (atom 0)
   :indices* (atom indices) :edges* (atom edges) :reads* (atom [])
   :limits* (atom []) :timeouts* (atom []) :embeddings* (atom []) :writes* (atom 0) :error* (atom nil)})

(defn- reader [state]
  (mongo/create-scoped-mongo-recall-js
   #js {:mongo #js {:graphNodeEmbeddings (collection state :indices (:indices* state))
                   :graphEdges (collection state :edges (:edges* state))}
        :embeddingRuntime #js {:hot #js {:getEmbeddingFunctionForModel
                                          (fn [model]
                                            #js {:generate (fn [texts]
                                                             (swap! (:embeddings* state) conj
                                                                    {:model model :texts (js->clj texts)})
                                                             (js/Promise.resolve #js [#js [1.0 0.0]]))})}}}
   (fn [] (swap! (:authority-calls* state) inc) (js/Promise.resolve (clj->js @(:authority* state))))))

(defn- ^:async recall! [reader request]
  (js->clj (await (reader (clj->js request))) :keywordize-keys true))

(deftest ^:async fresh-authority-precedes-scoped-storage-ranking-and-traversal
  (let [state (state) result (await (recall! (reader state) fixture/request))
        selection (:selection result)]
    (is (= "completed" (:status selection)))
    (is (= ["seed" "neighbor"] (mapv :id (:hits selection))))
    (is (= ["seed" "neighbor"] (get-in selection [:hits 1 :path])))
    (is (= ["edge:seed:neighbor"] (get-in selection [:hits 1 :path-edge-ids])))
    (is (= false (get-in selection [:hits 1 :seed?])))
    (is (= 1 @(:authority-calls* state)))
    (is (= #{"seed" "neighbor"}
           (set (get-in @(:reads* state) [0 :filter :source_event_id :$in]))))
    (is (= [{:model "held-model" :texts ["harbor"]}] @(:embeddings* state)))
    (is (= "not-loaded" (:field-status result)) "No invented physical field proof")
    (is (= 0 @(:writes* state)))
    (is (= 0 (get-in selection [:feedback :completed])))))

(deftest ^:async each-call-resolves-current-authority-without-reusing-old-grants
  (let [state (state) read! (reader state)
        before (await (recall! read! fixture/request))]
    (reset! (:authority* state) (update authority :records #(vec (take 1 %))))
    (let [after (await (recall! read! fixture/request))]
      (is (= 2 @(:authority-calls* state)))
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

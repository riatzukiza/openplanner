(ns openplanner.graph.recall.mongo
  "Trusted SDK/Mongo boundary. Request JSON has no authority port or scope.
   LGPL-3.0-or-later."
  (:require [goog.crypt :as crypt]
            [goog.crypt.Sha256]
            [openplanner.graph.recall.contract :as recall-contract]
            [openplanner.graph.recall.core :as recall]
            [openplanner.graph.recall.mongo-contract :as contract]
            [openplanner.graph.recall.ranking :as ranking]))

(defn- fail! [code]
  (throw (ex-info "Scoped graph recall boundary failed" {:recall-code code})))

(defn- unique? [values] (= (count values) (count (set values))))

(defn- digest [value]
  (let [hash (goog.crypt.Sha256.)]
    (.update hash (crypt/stringToUtf8ByteArray (pr-str (ranking/canonical-data value))))
    (crypt/byteArrayToHex (.digest hash))))

(defn- ^:async current-authority! [resolve-current-authority]
  (let [authority (try (js->clj (await (resolve-current-authority)) :keywordize-keys true)
                       (catch :default _error (fail! :authority-unavailable)))]
    (when (some? authority)
      (when-not (and (contract/valid-authority? authority)
                     (unique? (map :id (:records authority)))) (fail! :invalid-authority)))
    authority))

(defn- authority-binding [authority]
  (when authority
    [(:scope authority) (:project authority) (vec (sort-by :id (:records authority)))]))

(defn- ^:async assert-current! [resolve-current-authority authority]
  ;; Provider and database awaits cannot preserve a grant. Refuse the whole
  ;; snapshot if any current principal, policy, admitted record or text changed.
  (when-not (= (authority-binding authority)
               (authority-binding (await (current-authority! resolve-current-authority))))
    (fail! :authority-changed)))

(defn- native-path [object keys]
  (reduce (fn [value key] (when (some? value) (aget value key))) object keys))

(defn- keyword-value [value] (if (string? value) (keyword value) value))

(defn- decode-selection [selection]
  (if-not (map? selection) selection
    (cond-> (update selection :status keyword-value)
      (map? (:feedback selection)) (update-in [:feedback :status] keyword-value)
      (map? (:failure selection)) (update :failure #(-> % (update :stage keyword-value) (update :code keyword-value)))
      (vector? (:hits selection)) (update :hits #(mapv (fn [hit] (if (map? hit) (update hit :reason keyword-value) hit)) %)))))

(defn valid-scoped-result-js?
  "Check the owning result contract at a consuming wire boundary; grants no authority."
  [raw-result]
  (let [result (js->clj raw-result :keywordize-keys true)
        result (if (map? result)
                 (-> result (update :field-status keyword-value) (update :selection decode-selection)) result)]
    (boolean (contract/valid-result? result))))

(defn- supported? [sdk]
  (every? #(fn? (native-path sdk %))
          [["mongo" "graphNodeEmbeddings" "find"] ["mongo" "graphEdges" "find"]
           ["embeddingRuntime" "hot" "getModel"]
           ["embeddingRuntime" "hot" "getEmbeddingFunctionForModel"]]))

(defn- ^:async rows! [^js collection filter maximum]
  (let [^js cursor (.find collection (clj->js filter))
        _ (.sort cursor #js {:_id 1})
        _ (.limit cursor (inc maximum))
        _ (.maxTimeMS cursor 5000)
        rows (js->clj (await (.toArray cursor)) :keywordize-keys true)]
    (when-not (vector? rows) (fail! :invalid-projection))
    (when (> (count rows) maximum) (fail! :storage-limit-exceeded))
    rows))

(defn- text-sha256 [text]
  (let [hash (goog.crypt.Sha256.)]
    (.update hash (crypt/stringToUtf8ByteArray text))
    (crypt/byteArrayToHex (.digest hash))))

(defn- decode-indices [authority model rows]
  (let [ids (set (map :id (:records authority)))
        texts (into {} (map (juxt :id :text) (:records authority)))
        ;; Exclude foreign/legacy compact rows before inspecting their content.
        bound (filterv #(and (ids (:source_event_id %)) (= (:node_id %) (:source_event_id %))
                              (= (:project authority) (:project %)) (= model (:embedding_model %))
                              (= (:source_text_hash_sha256 %) (text-sha256 (get texts (:source_event_id %))))) rows)
        decoded (mapv #(hash-map :id (:_id %) :event-id (:source_event_id %) :model (:embedding_model %)
                                 :dimensions (:embedding_dimensions %) :chunk (:chunk_index %)
                                 :embedding (:embedding %)) bound)]
    (when-not (and (every? contract/valid-index? decoded) (unique? (map :id decoded))
                   (unique? (map (juxt :event-id :model :chunk) decoded))
                   (every? #(= (:dimensions %) (count (:embedding %))) decoded))
      (fail! :invalid-embedding))
    (vec (sort-by :id decoded))))

(defn- decode-edges [authority rows]
  (let [ids (set (map :id (:records authority))) scope (:scope authority)
        admitted (filterv (fn [row]
                            (let [data (get-in row [:data :scoped_recall]) evidence (:provenance_event_ids data)]
                              (and (= (:project authority) (:project row))
                                   (= 1 (:version data)) (= (:org-id scope) (:org_id data))
                                   (= (:project authority) (:project data))
                                   (= (:actor-id scope) (:character_id data))
                                   (ids (:source_node_id row)) (ids (:target_node_id row))
                                   (vector? evidence) (seq evidence) (every? ids evidence)))) rows)
        edges (mapv (fn [row]
                      (let [data (get-in row [:data :scoped_recall])]
                        (when-not (contract/valid-projection? data) (fail! :invalid-projection))
                        {:id (:_id row) :source (:source_node_id row) :target (:target_node_id row)
                         :cost (:cost data) :provenance-node-ids (:provenance_event_ids data)})) admitted)]
    (vec (sort-by :id edges))))

(defn- snapshot [authority nodes indices edges]
  {:version 1 :scope (:scope authority) :index-status :ready
   :revision (str "scoped-snapshot:" (digest [(:scope authority) (:project authority)
                                             (sort-by :id (:records authority)) indices edges]))
   ;; This read profile does not load or integrate a physical field. The
   ;; explicit outer state keeps these sentinels from masquerading as its proof.
   :field-revision "not-loaded" :field-owner "not-loaded"
   :nodes nodes :edges edges :influences []})

(defn- select [authority snapshot request]
  (recall/recall-plan snapshot
                      (mapv #(hash-map :node-id (:id %) :event-id (:id %)
                                      :policy-revision (get-in authority [:scope :policy-revision])
                                      :allowed? true) (:records authority)) request))

(defn- ^:async query-embedding! [sdk model query format-query-text]
  (let [^js hot (native-path sdk ["embeddingRuntime" "hot"])
        ^js provider (.getEmbeddingFunctionForModel hot model)]
    (when-not (fn? (native-path provider ["generate"])) (fail! :unsupported-capability))
    (let [text (try (format-query-text query)
                    (catch :default _error (fail! :embedding-unavailable)))
          _ (when-not (and (string? text) (seq (.trim text))) (fail! :invalid-embedding))
          values (try (js->clj (await (.generate provider (clj->js [text]))))
                      (catch :default _error (fail! :embedding-unavailable)))]
      (when-not (and (vector? values) (= 1 (count values)) (contract/valid-embedding? (first values)))
        (fail! :invalid-embedding))
      (first values))))

(defn- ^:async stored-selection! [sdk resolve-current-authority authority request format-query-text]
  (let [ids (mapv :id (:records authority))
        scope (:scope authority)
        ^js hot (native-path sdk ["embeddingRuntime" "hot"])
        model (.getModel hot
                         (clj->js {:source "graph-event" :kind "graph.node" :project (:project authority)}))]
    (when-not (contract/valid-identity? model) (fail! :invalid-embedding))
    (if (empty? ids)
      (do (await (assert-current! resolve-current-authority authority))
          (select authority (snapshot authority [] [] []) request))
      (let [indices (decode-indices authority model
                                   (await (rows! (native-path sdk ["mongo" "graphNodeEmbeddings"])
                                                 {:project (:project authority) :source_event_id {:$in ids}
                                                  :node_id {:$in ids} :embedding_model model
                                                  :$expr {:$eq ["$node_id" "$source_event_id"]}} 1536)))]
        (await (assert-current! resolve-current-authority authority))
        (if-not (= (set ids) (set (map :event-id indices)))
          (select authority (assoc (snapshot authority [] indices []) :index-status :pending) request)
          (let [query-vector (await (query-embedding! sdk model (:query request) format-query-text))
                _ (await (assert-current! resolve-current-authority authority))
                nodes (ranking/rank-nodes (:records authority) indices query-vector)
                edges (decode-edges authority
                                    (await (rows! (native-path sdk ["mongo" "graphEdges"])
                                                  {:project (:project authority) :source_node_id {:$in ids}
                                                   :target_node_id {:$in ids} :data.scoped_recall.version 1
                                                   :data.scoped_recall.org_id (:org-id scope)
                                                   :data.scoped_recall.character_id (:actor-id scope)
                                                   :data.scoped_recall.project (:project authority)
                                                   ;; All provenance is admitted in Mongo before the
                                                   ;; retained-row budget, including nonempty array shape.
                                                   :data.scoped_recall.provenance_event_ids
                                                   {:$type "array" :$ne [] :$not {:$elemMatch {:$nin ids}}}} 4096)))]
            (await (assert-current! resolve-current-authority authority))
            (select authority (snapshot authority nodes indices edges) request)))))))

(defn create-scoped-mongo-recall-js
  "Bind a trusted SDK handle, fresh host callback and owning SDK query formatter;
   return a request-only
   reader. No callback or service credential can be supplied by request JSON.
   Read-only ordinary event profile; legacy aggregates/forces/REST are excluded."
  [sdk resolve-current-authority format-query-text]
  (^:async fn [raw-request]
    (let [result
          (try
            (let [request (js->clj raw-request :keywordize-keys true)
                  request (if (map? request) (update request :feedback #(if (string? %) (keyword %) %)) request)]
              (cond
                (not (recall-contract/valid-request? request)) (contract/failed :invalid-request)
                (not (and (supported? sdk) (fn? resolve-current-authority) (fn? format-query-text)))
                (contract/failed :unsupported-capability)
                :else
                (let [authority (await (current-authority! resolve-current-authority))]
                  (if (nil? authority)
                    {:version 1 :field-status :not-loaded
                     :selection {:status :denied :hits []
                                 :feedback {:status :not-requested :attempted 0 :completed 0}}}
                    {:version 1 :field-status :not-loaded
                     :selection (await (stored-selection! sdk resolve-current-authority authority request format-query-text))}))))
            (catch :default error
              (contract/failed (or (:recall-code (ex-data error))
                                   (when (= 50 (native-path error ["code"])) :timeout) :transport-error))))]
      (clj->js (if (contract/valid-result? result) result (contract/failed :invalid-projection))))))

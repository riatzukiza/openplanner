(ns openplanner.graph.claims.boundary
  "JavaScript/CLJS conversion boundary for edge-claim logic.

  All JS object field aliases, Date parsing, number coercion, JSON canonicalizing,
  and Node crypto calls live here. The pure core namespace receives normalized
  CLJS maps only."
  (:require [clojure.string :as str]
            [goog.crypt :as crypt]
            [openplanner.graph.claims.core :as claims]
            [openplanner.graph.claims.policy :as policy]
            [openplanner.graph.claims.schema :as schema])
  (:import [goog.crypt Sha256]))

(defn- js-object?
  [value]
  (and (some? value)
       (= "object" (goog/typeOf value))
       (not (array? value))))

(defn- jget
  [obj k]
  (when (js-object? obj)
    (aget obj k)))

(defn- nonblank
  [value]
  (when (string? value)
    (let [s (str/trim value)]
      (when-not (str/blank? s) s))))

(defn- agreeing-alias
  "Every explicitly supplied identity alias must be a matching nonblank string."
  [input fields fallback]
  (let [supplied (when (js-object? input)
                   (filter #(js/Object.hasOwn input %) fields))
        values (map #(nonblank (jget input %)) supplied)]
    (if (seq supplied)
      (when (and (every? some? values) (= 1 (count (set values)))) (first values))
      fallback)))

(defn- token-keyword
  [value]
  (some-> value str str/trim str/lower-case (str/replace #"-" "_") keyword))

(defn- wire-status-name
  [status]
  (some-> status name))

(defn- edge-claim-status-keyword
  [value]
  (let [status (token-keyword value)]
    (when (contains? claims/statuses status) status)))

(defn normalize-edge-claim-status
  ([value] (normalize-edge-claim-status value :proposed))
  ([value fallback]
   (or (edge-claim-status-keyword value)
       (edge-claim-status-keyword fallback)
       :proposed)))

(defn normalize-edge-claim-direction
  [value]
  (claims/normalize-direction (token-keyword value)))

(defn- plain-scope-entry?
  [[_ value]]
  (and (some? value) (not (str/blank? (str value)))))

(defn- json-scope-value?
  "Accept only finite JSON data, refusing runtime values and ancestor cycles."
  [value]
  (let [ancestors (js/WeakSet.)]
    (letfn [(valid? [v]
              (cond
                (identical? js/undefined v) false
                (nil? v) true
                (or (string? v) (boolean? v)) true
                (number? v) (js/Number.isFinite v)
                (or (array? v) (js-object? v))
                (and (not (.has ancestors v))
                     (or (array? v)
                         (let [prototype (js/Object.getPrototypeOf v)]
                           (and (or (nil? prototype) (identical? prototype (.-prototype js/Object)))
                                (zero? (alength (js/Object.getOwnPropertySymbols v))))))
                     (do (.add ancestors v)
                         (try
                           (every? valid? (if (array? v)
                                            (array-seq v)
                                            (map #(aget v %) (array-seq (js/Object.keys v)))))
                           (finally (.delete ancestors v)))))
                :else false))]
      (valid? value))))

(defn normalize-edge-claim-scope
  [value]
  (when (and (some? value)
             (not (and (js-object? value)
                       (let [prototype (js/Object.getPrototypeOf value)]
                         (or (nil? prototype) (identical? prototype (.-prototype js/Object)))))))
    (throw (ex-info "Explicit claim scope must be a plain object" {})))
  (when (js-object? value)
    (let [entries (->> (js/Object.entries value)
                       (array-seq)
                       (map (fn [entry] [(str (aget entry 0)) (aget entry 1)]))
                       (filter plain-scope-entry?))]
      (when-not (every? (fn [[_ v]] (json-scope-value? v)) entries)
        (throw (ex-info "Claim scope must contain only finite JSON values without cycles" {})))
      (when (seq entries)
        (into {} entries)))))

(defn- inferred-scope
  [input]
  (normalize-edge-claim-scope
    #js {:tenant_id (jget input "tenant_id")
         :org_id (jget input "org_id")
         :project (jget input "project")
         :lake (jget input "lake")
         :graph_version (jget input "graph_version")}))

(defn- stable-json
  "Preserve all nested JSON keys while sorting object keys at every depth."
  [value]
  (cond
    (array? value) (str "[" (str/join "," (map stable-json (array-seq value))) "]")
    (js-object? value) (str "{" (str/join "," (map (fn [k]
                                                   (str (js/JSON.stringify k) ":" (stable-json (aget value k))))
                                                 (sort (array-seq (js/Object.keys value))))) "}")
    :else (js/JSON.stringify value)))

(defn- canonical-scope-json
  [scope]
  (if (seq scope)
    (stable-json (clj->js scope))
    "{}"))

(defn edge-claim-key-from-js
  [input]
  (let [direction (normalize-edge-claim-direction (jget input "direction"))
        scope (or (normalize-edge-claim-scope (jget input "scope"))
                  (inferred-scope input))
        source-node-id (agreeing-alias input ["sourceNodeId" "source_node_id" "source-node-id" "source"] nil)
        target-node-id (agreeing-alias input ["targetNodeId" "target_node_id" "target-node-id" "target"] nil)
        relation-kind (agreeing-alias input ["relationKind" "relation_kind" "relation-kind" "kind"] "related_to")]
    {:source-node-id source-node-id
     :target-node-id target-node-id
     :relation-kind relation-kind
     :direction direction
     :scope scope
     :scope-json (canonical-scope-json scope)}))

(defn- sha256-hex-24
  [text]
  (let [sha (Sha256.)]
    (.update sha text)
    (subs (crypt/byteArrayToHex (.digest sha)) 0 24)))

(defn build-edge-claim-id
  [input]
  (let [claim-key (edge-claim-key-from-js input)]
    (when-not (claims/valid-claim-key? claim-key)
      (throw (ex-info "Invalid edge claim key"
                      {:claim-key claim-key})))
    (str "edge_claim:" (sha256-hex-24 (claims/claim-id-material claim-key)))))

(defn- parse-ms
  [value]
  (cond
    (nil? value) nil
    (number? value) value
    (string? value) (let [ms (.parse js/Date value)]
                      (when-not (js/Number.isNaN ms) ms))
    (and (some? value) (fn? (.-getTime value))) (let [ms (.getTime value)]
                                                  (when-not (js/Number.isNaN ms) ms))
    :else nil))

(defn- clamp-confidence
  [value fallback]
  (let [n (if (nil? value) fallback (js/Number value))]
    (if (js/Number.isFinite n)
      (max 0 (min 1 n))
      fallback)))

(defn- claim-expiration-ms
  [input]
  (let [supplied (when (js-object? input)
                   (filter #(js/Object.hasOwn input %) ["validUntil" "valid_until"]))
        values (map (fn [field]
                      (when-some [value (jget input field)]
                        (or (parse-ms value) js/NaN))) supplied)]
    (when (seq supplied)
      (if (apply = values) (first values) js/NaN))))

(defn- status-set-from-js
  [value]
  (when-not (array? value)
    (throw (js/Error. "projection statuses must be an array when supplied")))
  (let [statuses (map edge-claim-status-keyword (array-seq value))]
    (when-not (every? some? statuses)
      (throw (js/Error. "projection statuses contain an unsupported status")))
    (set statuses)))

(defn projection-options-from-js
  [opts]
  {:statuses (if (and (js-object? opts) (js/Object.hasOwn opts "statuses"))
               (status-set-from-js (jget opts "statuses"))
               claims/projectable-statuses)
   :include-expired? (true? (jget opts "includeExpired"))
   :now-ms (if (and (js-object? opts) (js/Object.hasOwn opts "now"))
             (let [ms (parse-ms (jget opts "now"))]
               (if (and (number? ms) (js/Number.isFinite ms))
                 ms
                 (throw (js/Error. "projection now must be a valid timestamp"))))
             (.now js/Date))})

(defn- claim-id-from-js
  "Generate only when neither alias was supplied; malformed explicit IDs stay invalid."
  [input fallback]
  (let [fields (when (js-object? input)
                 (filter #(js/Object.hasOwn input %) ["claimId" "claim_id"]))]
    (if (seq fields)
      (let [values (map #(nonblank (jget input %)) fields)]
        (when (and (every? some? values) (= 1 (count (set values))))
          (first values)))
      (fallback))))

(defn edge-claim-from-js
  [input]
  (let [key (edge-claim-key-from-js input)
        [source-node-id target-node-id] (claims/canonical-endpoints key)
        claim-id (claim-id-from-js input #(build-edge-claim-id input))]
    (assoc key
           :source-node-id source-node-id :target-node-id target-node-id
           :claim-id claim-id
           :status (normalize-edge-claim-status (jget input "status") :proposed)
           :confidence (clamp-confidence (jget input "confidence") 0.5)
           :valid-until-ms (claim-expiration-ms input))))

(defn- edge-claim-from-js-soft
  [input]
  (let [key (edge-claim-key-from-js input)
        [source-node-id target-node-id] (claims/canonical-endpoints key)
        canonical-key (assoc key
                             :source-node-id source-node-id
                             :target-node-id target-node-id)
        claim-id (claim-id-from-js input
                   #(when (claims/valid-claim-key? canonical-key)
                      (str "edge_claim:" (sha256-hex-24 (claims/claim-id-material canonical-key)))))]
    (assoc canonical-key
           :claim-id claim-id
           :status (normalize-edge-claim-status (jget input "status") :proposed)
           :confidence (clamp-confidence (jget input "confidence") 0.5)
           :valid-until-ms (claim-expiration-ms input))))

(defn- edge-claim->wire-js
  [claim]
  #js {:claim_id (:claim-id claim)
       :source_node_id (:source-node-id claim)
       :target_node_id (:target-node-id claim)
       :relation_kind (:relation-kind claim)
       :direction (name (:direction claim))
       :scope (clj->js (or (:scope claim) {}))
       :status (wire-status-name (:status claim))
       :confidence (:confidence claim)
       :valid_until_ms (:valid-until-ms claim)})

(defn- projected-edge->js
  [edge]
  (clj->js {:source (:source edge)
            :target (:target edge)
            :kind (:kind edge)
            :claim_id (:claim-id edge)
            :confidence (:confidence edge)
            :direction (name (:direction edge))
            :scope (:scope edge)
            :status (wire-status-name (:status edge))}))

(defn normalize-edge-claim-status-js
  ([value] (normalize-edge-claim-status-js value "proposed"))
  ([value fallback]
   (wire-status-name (normalize-edge-claim-status value fallback))))

(defn normalize-edge-claim-direction-js
  [value]
  (name (normalize-edge-claim-direction value)))

(defn normalize-edge-claim-scope-js
  [value]
  (some-> (normalize-edge-claim-scope value) clj->js))

(defn build-edge-claim-id-js
  [input]
  (build-edge-claim-id input))

(defn normalize-edge-claim-input-js
  [input]
  (edge-claim->wire-js (edge-claim-from-js-soft input)))

(defn claim-projectable-js
  ([claim] (claim-projectable-js claim #js {}))
  ([claim opts]
   (boolean (claims/projectable? (edge-claim-from-js claim) (projection-options-from-js opts)))))

(defn project-edge-claim-js
  ([claim] (project-edge-claim-js claim #js {}))
  ([claim opts]
   (some-> (claims/claim->projected-edge (edge-claim-from-js claim)
                                         (projection-options-from-js opts))
           projected-edge->js)))

(defn project-edge-claims-js
  ([claim-rows] (project-edge-claims-js claim-rows #js {}))
  ([claim-rows opts]
   (let [claims (if (array? claim-rows)
                  (mapv edge-claim-from-js (array-seq claim-rows))
                  [])
         result (claims/project-claims claims (projection-options-from-js opts))]
     #js {:edges (clj->js (mapv #(js->clj (projected-edge->js %) :keywordize-keys true) (:edges result)))
          :stats (clj->js (:stats result))})))

(defn explain-edge-claim-js
  [claim]
  (clj->js (schema/explain-edge-claim (edge-claim-from-js-soft claim))))

(defn- decision->js
  [decision]
  #js {:kind (name (:decision/kind decision))
       :reason (name (:decision/reason decision))
       :data (clj->js (:decision/data decision))})

(defn evaluate-edge-claim-js
  [claim]
  (decision->js (policy/evaluate-claim (edge-claim-from-js-soft claim) []
                                     {:now-ms (.now js/Date)})))

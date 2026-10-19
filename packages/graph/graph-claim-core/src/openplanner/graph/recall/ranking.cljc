(ns openplanner.graph.recall.ranking
  "Portable vector arithmetic over already admitted, validated records.
   LGPL-3.0-or-later. No storage, provider routing, policy or physical solver."
  (:require [openplanner.graph.recall.mongo-contract :as contract]))

(defn- norm [embedding]
  (#?(:clj Math/sqrt :cljs js/Math.sqrt) (reduce + 0.0 (map #(* % %) embedding))))

(defn cosine
  "Reject malformed dimensions or degenerate vectors before arithmetic."
  [left right]
  (when-not (and (contract/valid-embedding? left) (contract/valid-embedding? right)
                 (= (count left) (count right)) (pos? (norm left)) (pos? (norm right)))
    (throw (ex-info "Invalid admitted embedding" {:recall-code :invalid-embedding})))
  (max -1.0 (min 1.0 (/ (reduce + 0.0 (map * left right)) (* (norm left) (norm right))))))

(defn rank-nodes
  "Choose one deterministic semantic entry point; graph traversal supplies the
   remaining context. Stored chunks may score only their freshly admitted event."
  [records indices query-embedding]
  (let [by-event (group-by :event-id indices)
        scored (mapv (fn [record]
                       {:id (:id record) :event-id (:id record) :text (:text record) :seed? false
                        :score (apply max (map #(cosine query-embedding (:embedding %)) (get by-event (:id record))))})
                     (sort-by :id records))
        entry-id (:id (first (sort-by (juxt (comp - :score) :id) scored)))]
    (mapv #(assoc % :seed? (= entry-id (:id %))) scored)))

(defn canonical-data
  "Stable map key order for scoped snapshot hashing; vectors preserve identity order."
  [value]
  (cond (map? value) (into (sorted-map) (map (fn [[key item]] [key (canonical-data item)])) value)
        (sequential? value) (mapv canonical-data value)
        :else value))

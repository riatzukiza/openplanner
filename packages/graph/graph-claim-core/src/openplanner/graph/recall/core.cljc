(ns openplanner.graph.recall.core
  "Pure scoped graph selection. Storage, identity resolution and writes stay at
   the boundary; this module does not integrate motion or replace a field kernel.
   LGPL-3.0-or-later."
  (:require [clojure.set :as set]
            [openplanner.graph.recall.contract :as contract]))

(defn- failed [code]
  {:status :failed :hits [] :failure {:stage :graph :code code}
   :feedback {:status :not-requested :attempted 0 :completed 0}})

(defn- metadata-valid? [snapshot]
  ;; Content is validated only after admission. Hidden malformed values cannot
  ;; change a permitted result by inducing a global validation failure.
  (and (map? snapshot)
       (contract/valid-snapshot? (assoc snapshot :nodes [] :edges [] :influences []))
       (every? (fn [[key maximum]]
                 (and (vector? (get snapshot key)) (<= (count (get snapshot key)) maximum)))
               [[:nodes 2048] [:edges 4096] [:influences 4096]])))

(defn- bound-node? [revision decisions node]
  (let [matches (get decisions (:id node))
        decision (when (= 1 (count matches)) (first matches))]
    (and (contract/valid-decision? decision)
         (true? (:allowed? decision))
         (= revision (:policy-revision decision))
         (= (:event-id node) (:event-id decision)))))

(defn- admitted-node-ids [nodes]
  ;; Grow from ordinary admitted nodes. A compact row needs its own bound grant
  ;; and every transitive member. Unknown members and cycles never enter.
  (loop [admitted (set (map :id (remove #(contains? % :members) nodes)))]
    (let [next-admitted (into admitted
                              (keep (fn [{:keys [id members]}]
                                      (when (and (vector? members) (seq members)
                                                 (set/subset? (set members) admitted)) id)))
                              nodes)]
      (if (= admitted next-admitted) admitted (recur next-admitted)))))

(defn- evidence-admitted? [ids evidence]
  (and (vector? evidence) (seq evidence) (every? ids evidence)))

(defn- scoped-snapshot [snapshot decisions]
  (let [grouped (group-by :node-id decisions)
        bound (filterv #(bound-node? (get-in snapshot [:scope :policy-revision]) grouped %) (:nodes snapshot))
        ids (admitted-node-ids bound)
        nodes (filterv #(contains? ids (:id %)) bound)
        edges (filterv #(and (ids (:source %)) (ids (:target %))
                             (evidence-admitted? ids (:provenance-node-ids %))) (:edges snapshot))
        edge-ids (set (map :id edges))
        influences (filterv #(and (edge-ids (:edge-id %))
                                  (evidence-admitted? ids (:provenance-node-ids %))) (:influences snapshot))]
    (assoc snapshot :nodes nodes :edges edges :influences influences)))

(defn- unique-ids? [rows]
  (= (count rows) (count (set (map :id rows)))))

(defn- snapshot-valid? [snapshot]
  (and (contract/valid-snapshot? snapshot)
       (every? unique-ids? ((juxt :nodes :edges :influences) snapshot))))

(defn- ordered-seeds [nodes request]
  (->> nodes (filter :seed?) (sort-by (juxt (comp - :score) :id))
       (take (:fetch request)) (take (:k request))
       (mapv (fn [node] {:id (:id node) :cost 0 :score (:score node)
                        :path [(:id node)] :path-edge-ids [] :reason :semantic-seed}))))

(defn- projected-edge-costs [edges influences]
  (let [by-edge (group-by :edge-id influences)]
    (mapv (fn [edge]
            (assoc edge :cost (max 0 (+ (:cost edge)
                                       ;; Stable identities define reduction order on both hosts.
                                       (reduce + 0 (map :delta (sort-by :id (get by-edge (:id edge))))))))) edges)))

(defn- expand-path [path edges visited maximum]
  (reduce (fn [result edge]
            (let [cost (+ (:cost path) (:cost edge)) target (:target edge)]
              (cond
                (visited target) result
                (> cost maximum) (assoc result :cost-exhausted? true)
                :else (update result :paths conj
                              {:id target :cost cost :score (:score path)
                               :path (conj (:path path) target)
                               :path-edge-ids (conj (:path-edge-ids path) (:id edge))
                               :reason :graph-neighbor}))))
          {:paths [] :cost-exhausted? false} edges))

(defn- walk [snapshot request]
  (let [nodes (into {} (map (juxt :id identity)) (:nodes snapshot))
        outgoing (group-by :source (projected-edge-costs (:edges snapshot) (:influences snapshot)))]
    (loop [queue (ordered-seeds (:nodes snapshot) request) visited #{} hits [] cost-exhausted? false]
      (let [queue (remove #(visited (:id %)) queue)]
        (cond
          (empty? queue) {:hits hits :exhausted? cost-exhausted?}
          (= (count visited) (:max-nodes request)) {:hits hits :exhausted? true}
          :else
          (let [ordered (sort-by (juxt :cost (comp - :score) :id :path :path-edge-ids) queue)
                path (first ordered) id (:id path)
                visited (conj visited id)
                expanded (expand-path path (get outgoing id) visited (:max-cost request))]
            (recur (into (vec (rest ordered)) (:paths expanded)) visited
                   (conj hits (merge (select-keys (get nodes id) [:id :text :event-id :seed?]) path))
                   (or cost-exhausted? (:cost-exhausted? expanded)))))))))

(defn recall-plan
  "Select traced context from a storage snapshot and freshly bound decisions.
   Denied rows cannot influence seeds, paths, costs or feedback. This function
   performs no reads/writes and cannot turn caller data into current authority."
  [snapshot decisions request]
  (cond
    (not (contract/valid-request? request)) (failed :invalid-request)
    (not (metadata-valid? snapshot)) (failed :invalid-snapshot)
    (not (vector? decisions)) (failed :invalid-authority-decisions)
    :else
    (let [scoped (scoped-snapshot snapshot decisions)
          base {:hits [] :recall-id (:recall-id request)
                :graph-revision (:revision scoped) :field-revision (:field-revision scoped)
                :field-owner (:field-owner scoped) :policy-revision (get-in scoped [:scope :policy-revision])
                :budget (select-keys request [:k :fetch :max-nodes :max-cost])
                :diagnostics {:denied-nodes (- (count (:nodes snapshot)) (count (:nodes scoped)))
                              :authorized-nodes (count (:nodes scoped)) :admitted-edges (count (:edges scoped))}
                :feedback {:status :not-requested :attempted 0 :completed 0}}]
      (cond
        (not (snapshot-valid? scoped)) (failed :invalid-snapshot)
        (= :pending (:index-status scoped)) (assoc base :status :indexing-pending)
        (empty? (:nodes snapshot)) (assoc base :status :empty)
        (empty? (:nodes scoped)) (assoc base :status :denied)
        ;; Only the safe admission outcome can distinguish denial here;
        ;; properties of excluded nodes cannot select the public outcome.
        (and (pos? (get-in base [:diagnostics :denied-nodes]))
             (empty? (ordered-seeds (:nodes scoped) request)))
        (assoc base :status :denied)
        :else (let [{:keys [hits exhausted?]} (walk scoped request)]
                (assoc base :status (cond exhausted? :budget-exhausted (seq hits) :completed :else :empty)
                       :hits (vec (take (:k request) hits))
                       :visited-count (count hits)))))))

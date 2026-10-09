(ns openplanner.graph.recall.core-test
  "Frozen scoped graph laws, run on JVM and Node without database/model I/O."
  (:require #?(:clj [clojure.test :refer [deftest is run-tests]]
               :cljs [cljs.test :refer [deftest is run-tests]])
            #?(:clj [clojure.edn :as edn] :cljs [cljs.reader :as edn])
            [openplanner.graph.recall.contract :as contract]
            [openplanner.graph.recall.core :as recall]))

(def scope {:actor-id "creator" :org-id "org-local" :membership-id "member-local"
            :user-id "user-local" :policy-revision "stored-policy:7"})
(def request {:version 1 :recall-id "turn:17:recall" :query "harbor" :k 6 :fetch 18
              :max-nodes 64 :max-cost 4 :feedback :none})

(defn node [id seed? score]
  {:id id :event-id (str "event:" id) :text (str "memory:" id) :seed? seed? :score score})

(defn edge [id source target cost]
  {:id id :source source :target target :cost cost :provenance-node-ids [source target]})

(def snapshot
  {:version 1 :revision "graph:23" :field-revision "field:11" :field-owner "eros-eris-field"
   :scope scope :index-status :ready
   :nodes [(node "seed" true 0.8) (node "neighbor" false 0.1)]
   :edges [(edge "edge:seed:neighbor" "seed" "neighbor" 1)] :influences []})

(defn decisions-for [nodes]
  (mapv (fn [row] {:node-id (:id row) :event-id (:event-id row)
                  :policy-revision (:policy-revision scope) :allowed? true}) nodes))

(defn plan
  ([snapshot-value] (plan snapshot-value (decisions-for (:nodes snapshot-value))))
  ([snapshot-value decisions] (recall/recall-plan snapshot-value decisions request)))

(deftest schemas-are-named-serializable-and-executable
  (is (= contract/registry (edn/read-string (pr-str contract/registry))))
  (is (contract/valid-snapshot? snapshot))
  (is (contract/valid-request? request))
  (doseq [invalid [(assoc request :feedback :reinforce)
                   (assoc request :fetch 19) (assoc request :max-cost ##NaN)
                   (assoc request :max-cost ##Inf) (assoc request :actor-id "payload-forged")]]
    (is (not (contract/valid-request? invalid)))))

(deftest neighbor-is-reached-through-an-authorized-edge
  (let [result (plan snapshot)
        neighbor (first (filter #(= "neighbor" (:id %)) (:hits result)))]
    (is (= :completed (:status result)))
    (is (= ["seed" "neighbor"] (:path neighbor)))
    (is (= ["edge:seed:neighbor"] (:path-edge-ids neighbor)))
    (is (= :graph-neighbor (:reason neighbor)))
    (is (= "memory:neighbor" (:text neighbor)))
    (is (= "graph:23" (:graph-revision result)))
    (is (= "field:11" (:field-revision result)))))

(deftest six-denied-leading-candidates-do-not-displace-the-authorized-seventh
  (let [hidden (mapv #(node (str "hidden:" %) true 0.99) (range 6))
        expanded (update snapshot :nodes #(into hidden %))
        result (plan expanded (decisions-for (:nodes snapshot)))]
    (is (= ["seed" "neighbor"] (mapv :id (:hits result))))
    (is (= 6 (get-in result [:diagnostics :denied-nodes])))
    (is (= 18 (get-in result [:budget :fetch])))
    (is (not-any? #(contains? % :denied-ids) [result (:diagnostics result)]))))

(deftest stale-mismatched-and-ambiguous-decisions-fail-closed
  (doseq [decisions [[]
                     (mapv #(assoc % :policy-revision "old-policy") (decisions-for (:nodes snapshot)))
                     (mapv #(assoc % :event-id "another-event") (decisions-for (:nodes snapshot)))
                     (conj (decisions-for (:nodes snapshot)) (first (decisions-for (:nodes snapshot))))
                     (mapv #(assoc % :allowed? false) (decisions-for (:nodes snapshot)))]]
    (let [result (plan snapshot decisions)]
      (is (= [] (:hits result)))
      (is (= :denied (:status result)))
      (is (= 0 (get-in result [:feedback :completed]))))))

(deftest hidden-intermediates-and-hidden-edge-evidence-cannot-form-a-path
  (let [hidden (node "hidden" false 1)
        expanded (-> snapshot
                     (update :nodes conj hidden)
                     (assoc :edges [(edge "in" "seed" "hidden" 0)
                                    (edge "out" "hidden" "neighbor" 0)
                                    (assoc (first (:edges snapshot)) :provenance-node-ids ["hidden"])]))
        result (plan expanded (decisions-for (:nodes snapshot)))]
    (is (= ["seed"] (mapv :id (:hits result))))
    (is (= 0 (get-in result [:diagnostics :admitted-edges])))))

(deftest changing-hidden-force-and-trail-inputs-cannot-change-the-result
  (let [allowed (decisions-for (:nodes snapshot))
        result (plan snapshot allowed)]
    (doseq [kind [:force :trail :field] delta [-1 -0.5 0 0.5 1]]
      (let [changed (update snapshot :influences conj
                            {:id (str kind delta) :edge-id "edge:seed:neighbor" :kind kind
                             :delta delta :provenance-node-ids ["foreign"]})]
        (is (= result (plan changed allowed)))))))

(deftest mixed-or-cyclic-compact-nodes-are-not-admitted
  (doseq [members [["seed" "foreign"] ["compact"] ["unknown"]]]
    (let [compact (assoc (node "compact" true 0.99) :members members)
          changed (update snapshot :nodes conj compact)
          result (plan changed (decisions-for (:nodes changed)))]
      (is (= ["seed" "neighbor"] (mapv :id (:hits result)))))))

(deftest states-and-budgets-are-observable
  (is (= :indexing-pending (:status (plan (assoc snapshot :index-status :pending)))))
  (is (= :empty (:status (plan (assoc snapshot :nodes [] :edges [])))))
  (let [result (recall/recall-plan snapshot (decisions-for (:nodes snapshot)) (assoc request :max-nodes 1))]
    (is (= :budget-exhausted (:status result)))
    (is (= ["seed"] (mapv :id (:hits result))))))

(deftest malformed-or-conflicting-input-does-not-become-empty-success
  (doseq [changed [(assoc snapshot :scope {})
                   (update snapshot :nodes conj (first (:nodes snapshot)))
                   (assoc-in snapshot [:edges 0 :cost] ##NaN)]]
    (let [result (plan changed)]
      (is (= :failed (:status result)))
      (is (= :invalid-snapshot (get-in result [:failure :code])))
      (is (= [] (:hits result))))))

(deftest reads-never-implicitly-write-reinforcement
  (let [a (plan snapshot) b (plan snapshot)]
    (is (= a b))
    (is (= {:status :not-requested :attempted 0 :completed 0} (:feedback a)))))

(defn -main
  "Nonzero exit on failures on either host."
  [& _args]
  (let [result (run-tests 'openplanner.graph.recall.core-test)]
    (when (pos? (+ (:fail result) (:error result)))
      #?(:clj (System/exit 1) :cljs (js/process.exit 1)))))

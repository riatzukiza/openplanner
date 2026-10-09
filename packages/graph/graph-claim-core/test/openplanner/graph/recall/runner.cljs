(ns openplanner.graph.recall.runner
  "Run retained graph claim tests plus scoped recall laws and the real ESM codec."
  (:require [cljs.test :refer [deftest is run-tests]]
            [openplanner.graph.claims.core-test]
            [openplanner.graph.recall.boundary :as boundary]
            [openplanner.graph.recall.core-test :as fixture]
            [openplanner.graph.recall.mongo-test]))

(deftest native-boundary-keeps-trace-and-failure-outcomes
  (let [result (boundary/recall-plan-js (clj->js fixture/snapshot)
                                       (clj->js (fixture/decisions-for (:nodes fixture/snapshot)))
                                       (clj->js fixture/request))]
    (is (= "completed" (aget result "status")))
    (is (= "neighbor" (aget result "hits" 1 "id")))
    (is (= "seed" (aget result "hits" 1 "path" 0)))
    (is (= "not-requested" (aget result "feedback" "status"))))
  (doseq [invalid [nil #js [] #js {:version 1 :actorId "forged"}]]
    (is (= "failed" (aget (boundary/recall-plan-js nil #js [] invalid) "status")))))

(defn -main
  "Existing claim assertions stay included; any failure exits nonzero."
  []
  ;; The imported recall test namespace installs the real end-run reporter.
  ;; run-tests' return value is not a summary and cannot attest success.
  (run-tests 'openplanner.graph.claims.core-test
             'openplanner.graph.recall.core-test
             'openplanner.graph.recall.runner
             'openplanner.graph.recall.mongo-test))

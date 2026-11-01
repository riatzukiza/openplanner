(ns openplanner.contract.async-sequence-regression
  "Canary for nbb async sequencing, rejected promises, and awaited cleanup."
  (:require-macros [openplanner.support.serial :refer [serial]])
  (:require ["node:assert/strict" :as assert]
            ["node:child_process" :as child]
            ["node:path" :as path]))

(defn delay [label]
  (js/Promise. (fn [resolve _]
                 (js/setTimeout (fn [] (println label) (resolve label)) 20))))

(defn delayed-rejection []
  (js/Promise. (fn [_ reject]
                 (js/setTimeout (fn [] (reject (js/Error. "delayed rejection canary"))) 20))))

(defn ^:async child-run [mode]
  (try
    (serial
     (await (delay "BODY-DONE"))
     (case mode
       "assert" (assert/equal 1 2 "delayed assertion canary")
       "reject" (await (delayed-rejection))
       "loop-direct" (doseq [index [1 2 3]]
                       (await (delay (str "ITER-" index))))
       "loop" (doseq [index [1 2 3]]
                (serial (await (delay (str "ITER-" index)))
                        (assert/ok (<= index 3))))
       "loopreject" (doseq [index [1 2 3]]
                      (serial (await (delay (str "ITER-" index)))
                              (if (= 2 index) (await (delayed-rejection)) nil)))
       "success" (assert/equal 1 1)
       "close-reject" (assert/equal 1 1)
       "cleanup-reject" (assert/equal 1 1))
     (println "TEST-DONE"))
    (finally
      (case mode
        "close-reject" (try (let [_ (await (delayed-rejection))] nil)
                            (finally (let [_ (await (delay "CLEANUP-DONE"))] nil)))
        "cleanup-reject" (let [_ (await (delayed-rejection))] nil)
        (let [_ (await (delay "CLEANUP-DONE"))] nil)))))

(defn child-result [mode]
  (child/spawnSync (.-execPath js/process)
                   #js [(path/resolve "node_modules/nbb/cli.js")
                        "--classpath" "src/cljs:test/cljs"
                        (path/resolve "test/cljs/openplanner/contract/async_sequence_regression.cljs")
                        ]
                   #js {:encoding "utf8" :timeout 10000
                        :env (js/Object.assign #js {} js/process.env #js {:OPENPLANNER_ASYNC_CASE mode})}))

(defn run-driver []
  (doseq [mode ["assert" "reject" "loopreject" "close-reject" "cleanup-reject" "success" "loop" "loop-direct"]]
    (let [result (child-result mode)
          output (str (.-stdout result) (.-stderr result))]
      (assert/equal (.-signal result) nil (str mode " must not hang"))
      (assert/ok (.includes output "BODY-DONE") (str mode " delayed body completed"))
      (when-not (= mode "cleanup-reject")
        (assert/ok (.includes output "CLEANUP-DONE") (str mode " cleanup completed")))
      (if (contains? #{"success" "loop" "loop-direct"} mode)
        (do (assert/equal (.-status result) 0 "successful runner exits zero")
            (assert/ok (< (.indexOf output "BODY-DONE") (.indexOf output "TEST-DONE")))
            (assert/ok (< (.indexOf output "TEST-DONE") (.indexOf output "CLEANUP-DONE")))
            (when (contains? #{"loop" "loop-direct"} mode)
              (doseq [marker ["ITER-1" "ITER-2" "ITER-3"]]
                (assert/ok (.includes output marker) (str marker " completed")))
              (assert/ok (< (.indexOf output "ITER-1") (.indexOf output "ITER-2")
                            (.indexOf output "ITER-3") (.indexOf output "TEST-DONE")))))
        (do (assert/notEqual (.-status result) 0 (str mode " must fail process"))
            (if (= mode "close-reject")
              (do (assert/ok (.includes output "TEST-DONE") "body completed before close failed")
                  (assert/ok (< (.indexOf output "TEST-DONE") (.indexOf output "CLEANUP-DONE"))
                             "cleanup completes after close rejection"))
              (if (= mode "cleanup-reject")
                (assert/ok (.includes (.-stdout result) "TEST-DONE") "body completed before cleanup failed")
                (assert/equal (.includes (.-stdout result) "TEST-DONE") false "failure must stop subsequent work")))
            (assert/ok (.includes output (if (= mode "assert") "delayed assertion canary" "delayed rejection canary")))
            (when (= mode "loopreject")
              (assert/ok (.includes (.-stdout result) "ITER-1"))
              (assert/ok (.includes (.-stdout result) "ITER-2"))
              (assert/equal (.includes (.-stdout result) "ITER-3") false "rejection stops later iteration"))))))
  (println "Async sequencing, rejection and cleanup subprocess canaries passed"))

(let [mode (.-OPENPLANNER_ASYNC_CASE js/process.env)]
  (if mode (child-run mode) (run-driver)))

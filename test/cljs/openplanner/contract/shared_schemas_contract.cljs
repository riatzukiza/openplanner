(ns openplanner.contract.shared-schemas-contract
  "Reference-first contract for the exported API JSON Schemas.

  Tests exact declarations and their Fastify validation/serialization effects
  over loopback HTTP. Handlers are synthetic; this is not the Mongo-backed
  search or state endpoint, nor a JVM or static-checker result."
  (:require ["fastify" :as fastify]
            ["node:assert/strict" :as assert]
            ["node:fs" :as fs]
            ["node:path" :as path]
            ["node:url" :as url]
            ["nbb" :as nbb]))

(defn file-url [p] (str (url/pathToFileURL (path/resolve p))))

(defn ^:async reference-schemas []
  (let [openapi (await (js/import (file-url "src/routes/v1/openapi.ts")))
        state (await (js/import (file-url "src/routes/v1/state.ts")))]
    #js {:openApiSchemas (.-openApiSchemas openapi)
         :redactionStateSchemas (.-redactionStateSchemas state)}))

(defn vector-response [body]
  #js {:ok true :tier (.-tier body) :storageBackend "mongodb"
       :result #js {:ids #js [#js []] :documents #js [#js []]
                    :metadatas #js [#js []] :distances #js [#js []]
                    :include #js ["documents" "metadatas" "distances"]}
       :observed body})

(defn state-response [query]
  #js {:ok true :status "pass" :strict (.-strict query)
       :generatedAt "2026-01-01T00:00:00.000Z"
       :checks #js {:vectorRedaction #js {:ok true :redactedWithStoredText 0 :redactedMissingSourceRef 0}
                    :documentBacklog #js {:ok true :rehydratableEventsWithText 0 :redactedEvents 0}
                    :schemaVersions #js {:ok true :eventTarget 1 :vectorChunkTarget 1
                                         :eventsMissingOrBehind 0 :vectorsMissingOrBehind 0}}
       :collections #js []})

(defn ^:async run-contract []
  (let [selector (.-OPENPLANNER_SCHEMAS_IMPL js/process.env)
        _ (when-not (or (nil? selector) (= selector "reference") (= selector "nbb"))
            (throw (js/Error. (str "Unsupported OPENPLANNER_SCHEMAS_IMPL: " selector))))
        schemas (if (= selector "nbb")
                  (do (nbb/addClassPath (path/resolve "src/cljs"))
                      (await (nbb/loadFile (path/resolve "src/cljs/openplanner/extern/js/schemas.cljs"))))
                  (await (reference-schemas)))
        expected (js/JSON.parse (fs/readFileSync "tests/fixtures/schemas-07085d65.json" "utf8"))
        api (.-openApiSchemas schemas)
        state (.-redactionStateSchemas schemas)
        app (fastify/default #js {:logger false})]
    (assert/deepEqual schemas expected "all exported schema declarations")
    (try
      (.post app "/vector" #js {:schema #js {:body (.-vectorSearchRequestSchema api)
                                               :response #js {"200" (.-vectorSearchResponseSchema api)}}}
             (fn [request reply] (.send reply (vector-response (.-body request)))))
      (.get app "/bad-response" #js {:schema #js {:response #js {"200" (.-vectorSearchResponseSchema api)}}}
            (fn [_request reply] (.send reply #js {:ok true})))
      (.get app "/state" #js {:schema #js {:querystring (.-stateQuerySchema state)
                                              :response #js {"200" (.-redactionStateResponseSchema state)}}}
            (fn [request reply] (.send reply (state-response (.-query request)))))
      (await (.listen app #js {:host "127.0.0.1" :port 0}))
      (let [base (str "http://127.0.0.1:" (.-port (.address (.-server app))))
            post (fn [body] (js/fetch (str base "/vector")
                                      #js {:method "POST" :headers #js {"content-type" "application/json"}
                                           :body (js/JSON.stringify body)}))
            valid (await (post #js {:q "hello" :extra "removed"}))
            valid-body (await (.json valid))]
        (assert/equal (.-status valid) 200)
        (assert/equal (.. valid-body -observed -q) "hello")
        (assert/equal (.. valid-body -observed -k) 20 "request default")
        (assert/equal (.. valid-body -observed -tier) "both" "tier default")
        (assert/equal (js/Object.hasOwn (.-observed valid-body) "extra") false "additional property handling")
        (doseq [body [#js {} #js {:q ""} #js {:q "ok" :k 0}
                      #js {:q "ok" :k 201} #js {:q "ok" :tier "invalid"}
                      #js {:q "ok" :visibility "secret"}]]
          (let [response (await (post body))
                payload (await (.json response))]
            (assert/equal (.-status response) 400 "invalid vector request")
            (assert/equal (.-code payload) "FST_ERR_VALIDATION")))
        (let [valid-state (await (js/fetch (str base "/state?strict=true")))
              valid-body (await (.json valid-state))
              default-state (await (js/fetch (str base "/state")))
              default-body (await (.json default-state))
              invalid-state (await (js/fetch (str base "/state?strict=maybe")))
              bad-response (await (js/fetch (str base "/bad-response")))]
          (assert/equal (.-status valid-state) 200)
          (assert/equal (.-strict valid-body) true)
          (assert/equal (.-strict default-body) false)
          (assert/equal (.-status invalid-state) 400)
          (assert/equal (.-status bad-response) 500 "required response members enforced")))
      (println "Shared schema contract passed against" (or selector "reference"))
      (finally (await (.close app))))))

(run-contract)

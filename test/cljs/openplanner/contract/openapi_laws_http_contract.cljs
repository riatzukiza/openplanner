(ns openplanner.contract.openapi-laws-http-contract
  "Reference-first OpenAPI subset laws over a real loopback HTTP response."
  (:require ["fastify" :as fastify]
            ["@fastify/sensible" :as sensible]
            ["node:assert/strict" :as assert]
            ["node:fs" :as fs]
            ["node:path" :as path]
            ["node:url" :as url]
            ["nbb" :as nbb]
            [openplanner.law.openapi :as law]))

(defn file-url [p] (str (url/pathToFileURL (path/resolve p))))

(defn ^:async run-contract []
  (let [module-path (or (.-OPENPLANNER_OPENAPI_MODULE js/process.env)
                        "src/routes/v1/openapi.ts")
        route (if (.endsWith module-path ".cljs")
                (await (nbb/loadFile (path/resolve module-path)))
                (await (js/import (file-url module-path))))
        auth (await (js/import (file-url "src/plugins/auth.ts")))
        app (fastify/default #js {:logger false})]
    (try
      (await (.register app sensible/default))
      (await (.register app (.-authPlugin auth) #js {:apiKey "synthetic-law-contract-key"}))
      (await (.register app (.-openApiRoutes route) #js {:prefix "/v1"}))
      (await (.listen app #js {:host "127.0.0.1" :port 0}))
      (let [base (str "http://127.0.0.1:" (.-port (.address (.-server app))))
            response (await (js/fetch (str base "/v1/openapi.json")
                                     #js {:headers #js {:Authorization "Bearer synthetic-law-contract-key"}}))
            wire (await (.text response))
            parsed (js/JSON.parse wire)
            snapshot (js/JSON.parse (.readFileSync fs "tests/fixtures/openapi-07085d65.json" "utf8"))
            document (js->clj parsed)
            ref-path ["paths" "/search/vector" "post" "responses" "200" "content"
                      "application/json" "schema" "$ref"]]
        (assert/equal (.-status response) 200)
        (assert/deepEqual parsed snapshot "real route matches pinned snapshot")
        (assert/equal (:valid? (law/diagnose (js->clj snapshot))) true "original snapshot satisfies law")
        (assert/equal (:valid? (law/diagnose document)) true "selected HTTP document satisfies law")
        (assert/equal (:valid? (law/diagnose (dissoc document "info"))) false "missing info rejected")
        (assert/equal (:valid? (law/diagnose (assoc document "paths" {}))) false "empty paths rejected")
        (let [report (law/diagnose (assoc-in document ref-path "#/components/schemas/Missing"))]
          (assert/equal (:valid? report) false "dangling local ref rejected")
          (assert/deepEqual (clj->js (:in (first (:errors report)))) (clj->js ref-path)
                            "local ref diagnostic identifies data path")))
      (println "OpenAPI subset laws passed against" module-path)
      (finally (await (.close app))))))

(defn -main []
  ;; Run from test/malli so only this test resolves the Malli dependency.
  (js/process.chdir (path/resolve "../.."))
  (run-contract))

(ns openapi-http-contract
  "HTTP contract for the storage-independent OpenAPI endpoint.

  The real auth plugin and route are mounted on an isolated loopback Fastify
  server. OPENPLANNER_OPENAPI_MODULE selects an alternative module exporting
  openApiRoutes; the reference TS module is the default. No database is used."
  (:require ["fastify" :as fastify]
            ["@fastify/sensible" :as sensible]
            ["node:assert/strict" :as assert]
            ["node:fs" :as fs]
            ["node:crypto" :as crypto]
            ["node:path" :as path]
            ["node:url" :as url]
            ["nbb" :as nbb]))

(def fixture-path "tests/fixtures/openapi-07085d65.json")
(def test-key "synthetic-openapi-contract-key")

(defn file-url [p]
  (str (url/pathToFileURL (path/resolve p))))

(defn ^:async request [base suffix options]
  (await (js/fetch (str base suffix) options)))

(defn ^:async run-contract []
  (let [module-path (or (.-OPENPLANNER_OPENAPI_MODULE js/process.env)
                        "src/routes/v1/openapi.ts")
        route-module (if (.endsWith module-path ".cljs")
                       (do (nbb/addClassPath (path/resolve "src/nbb"))
                           (await (nbb/loadFile (path/resolve module-path))))
                       (await (js/import (file-url module-path))))
        auth-module (await (js/import (file-url "src/plugins/auth.ts")))
        app (fastify/default #js {:logger false})]
    (try
      (await (.register app sensible/default))
      (await (.register app (.-authPlugin auth-module) #js {:apiKey test-key}))
      (await (.register app (.-openApiRoutes route-module) #js {:prefix "/v1"}))
      (await (.listen app #js {:host "127.0.0.1" :port 0}))
      (let [address (.address (.-server app))
            base (str "http://127.0.0.1:" (.-port address))
            no-auth (await (request base "/v1/openapi.json" #js {}))
            bad-auth (await (request base "/v1/openapi.json" #js {:headers #js {:Authorization "Bearer wrong"}}))
            preflight (await (request base "/v1/openapi.json" #js {:method "OPTIONS" :headers #js {:Origin "https://example.invalid"}}))
            options #js {:headers #js {:Authorization (str "Bearer " test-key)}}
            good (await (request base "/v1/openapi.json" options))
            text (await (.text good))]
        (assert/equal (.-status no-auth) 401 "missing credentials are rejected")
        (assert/equal (.-status bad-auth) 401 "incorrect credentials are rejected")
        (assert/deepEqual (await (.json no-auth))
                          #js {:statusCode 401 :error "Unauthorized"
                               :message "Missing/invalid Authorization: Bearer <API_KEY>"})
        (assert/deepEqual (await (.json bad-auth))
                          #js {:statusCode 401 :error "Unauthorized"
                               :message "Missing/invalid Authorization: Bearer <API_KEY>"})
        (assert/equal (.-status preflight) 204 "preflight succeeds without credentials")
        (assert/equal (.get (.-headers preflight) "access-control-allow-origin") "https://example.invalid")
        (assert/equal (.get (.-headers preflight) "access-control-allow-methods") "GET,POST,PUT,PATCH,DELETE,OPTIONS")
        (assert/equal (.get (.-headers preflight) "access-control-allow-headers") "Authorization,Content-Type,X-Requested-With")
        (assert/equal (.get (.-headers preflight) "access-control-max-age") "86400")
        (assert/equal (.get (.-headers preflight) "vary") "Origin")
        (assert/equal (.-status good) 200)
        (assert/equal (.get (.-headers good) "content-type") "application/json; charset=utf-8")
        (assert/equal (.get (.-headers good) "content-length") "4946")
        (assert/equal (.digest (.update (crypto/createHash "sha256") text) "hex")
                      "745dbaa1873009d92b780c47c211e9c05c9c0ca82b10d642e9dc3637f7a0a702"
                      "wire JSON bytes match the reference")
        (assert/deepEqual (js/JSON.parse text)
                          (js/JSON.parse (.readFileSync fs fixture-path "utf8"))
                          "complete OpenAPI document matches the reference fixture")
        (let [again (await (request base "/v1/openapi.json?repeat=1" options))
              again-body (await (.json again))
              head (await (request base "/v1/openapi.json" #js {:method "HEAD" :headers #js {:Authorization (str "Bearer " test-key)}}))
              post (await (request base "/v1/openapi.json" #js {:method "POST" :headers #js {:Authorization (str "Bearer " test-key)}}))]
          (assert/equal (.-status again) 200 "repeat query request")
          (assert/deepEqual again-body (js/JSON.parse text) "repeat returns same document")
          (assert/equal (.-status head) 200 "Fastify automatically supports HEAD")
          (assert/equal (await (.text head)) "" "HEAD has no body")
          (assert/equal (.-status post) 404 "unsupported method"))
        (println "OpenAPI HTTP contract passed against" module-path))
      (finally (await (.close app))))))

(run-contract)

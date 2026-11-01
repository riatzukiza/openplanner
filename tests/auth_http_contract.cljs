(ns auth-http-contract
  "Loopback HTTP contract for the existing global authentication/CORS hook.

  Routes are synthetic: this tests the hook's effect on sibling routes, not
  the implementations behind public or protected application endpoints.
  OPENPLANNER_AUTH_MODULE selects a future nbb .cljs or ESM module exporting
  authPlugin; the unchanged TypeScript reference is the default."
  (:require ["fastify" :as fastify]
            ["@fastify/sensible" :as sensible]
            ["node:assert/strict" :as assert]
            ["node:path" :as path]
            ["node:url" :as url]
            ["nbb" :as nbb]))

(def test-key "Synthetic-Key-aB12")
(def unauthorized #js {:statusCode 401 :error "Unauthorized"
                       :message "Missing/invalid Authorization: Bearer <API_KEY>"})
(def success #js {:ok true})
(defn file-url [p] (str (url/pathToFileURL (path/resolve p))))
(defn ^:async request [base suffix options] (await (js/fetch (str base suffix) options)))

(defn ^:async assert-json [response status body label]
  (assert/equal (.-status response) status label)
  (assert/deepEqual (await (.json response)) body label))

(defn ^:async run-contract []
  (let [module-path (or (.-OPENPLANNER_AUTH_MODULE js/process.env) "src/plugins/auth.ts")
        auth-module (if (.endsWith module-path ".cljs")
                      (await (nbb/loadFile (path/resolve module-path)))
                      (await (js/import (file-url module-path))))
        app (fastify/default #js {:logger false})
        handler (fn [_request reply] (.send reply success))]
    (try
      (await (.register app sensible/default))
      (await (.register app (.-authPlugin auth-module) #js {:apiKey test-key}))
      ;; The routes are registered after auth as sibling routes. This detects a
      ;; candidate accidentally encapsulating its hook inside the plugin.
      (doseq [p ["/" "/v1/health" "/v1/health/" "/v1/metrics" "/v1/metrics/"
                 "/v1/public" "/v1/public/" "/v1/public/garden"
                 "/v1/publicity/garden" "/v1/private"]]
        (.route app #js {:method #js ["GET" "POST" "PUT" "PATCH" "DELETE"]
                         :url p :handler handler}))
      (await (.listen app #js {:host "127.0.0.1" :port 0}))
      (let [base (str "http://127.0.0.1:" (.-port (.address (.-server app))))
            good #js {:Authorization (str "Bearer " test-key)}]
        ;; Public matching uses the unmodified req.url, including query text.
        (doseq [p ["/" "/v1/health" "/v1/metrics" "/v1/public/"
                   "/v1/public/?query=1" "/v1/public/garden" "/v1/public/garden?query=1"]]
          (await (assert-json (await (request base p #js {})) 200 success (str "public " p))))
        (doseq [p ["/?q=1" "/v1/health?deep=true" "/v1/health/"
                   "/v1/metrics?x=1" "/v1/metrics/" "/v1/public"
                   "/v1/public?x=1" "/v1/publicity/garden" "/v1/private" "/no-such-route"]]
          (await (assert-json (await (request base p #js {})) 401 unauthorized (str "protected " p))))
        ;; Exact public paths and the public-prefix exception apply to methods
        ;; other than GET as well; they are not method-limited by this hook.
        (doseq [method ["POST" "PUT" "PATCH" "DELETE"]]
          (await (assert-json (await (request base "/v1/health" #js {:method method})) 200 success
                              (str "public method " method)))
          (await (assert-json (await (request base "/v1/private" #js {:method method})) 401 unauthorized
                              (str "private method " method)))
          (await (assert-json (await (request base "/v1/private" #js {:method method :headers good})) 200 success
                              (str "authorized method " method))))
        (doseq [[p status] [["/v1/health" 200] ["/v1/health?deep=true" 401]
                            ["/v1/private" 401]]]
          (let [response (await (request base p #js {:method "HEAD"}))]
            (assert/equal (.-status response) status (str "HEAD " p))
            (assert/equal (await (.text response)) "")))
        ;; Scheme prefix is case-insensitive; token contents are not, and no
        ;; extra whitespace is stripped from the extracted token.
        (doseq [value [(str "Bearer " test-key) (str "bearer " test-key) (str "bEaReR " test-key)]]
          (await (assert-json (await (request base "/v1/private" #js {:headers #js {:Authorization value}}))
                              200 success (str "valid authorization " value))))
        (doseq [value ["Basic Synthetic-Key-aB12" "Bearer" "Bearer wrong"
                       "Bearer synthetic-key-ab12" (str "Bearer  " test-key)]]
          (await (assert-json (await (request base "/v1/private" #js {:headers #js {:Authorization value}}))
                              401 unauthorized (str "invalid authorization " value))))
        (await (assert-json (await (request base "/v1/health?deep=true" #js {:headers good}))
                            200 success "authenticated query route"))
        ;; CORS headers are attached on successful and rejected requests.
        (doseq [options [#js {} #js {:headers #js {:Origin "https://example.invalid"}}
                         #js {:headers #js {:Origin "null" :Authorization (str "Bearer " test-key)}}]]
          (let [response (await (request base "/v1/private" options))
                origin (some-> options .-headers .-Origin)]
            (assert/equal (.get (.-headers response) "access-control-allow-origin") (or origin "*"))
            (assert/equal (.get (.-headers response) "vary") "Origin")
            (assert/equal (.get (.-headers response) "access-control-allow-methods") "GET,POST,PUT,PATCH,DELETE,OPTIONS")
            (assert/equal (.get (.-headers response) "access-control-allow-headers") "Authorization,Content-Type,X-Requested-With")
            (assert/equal (.get (.-headers response) "access-control-max-age") "86400")))
        ;; Even an unmatched route receives the global early preflight reply.
        (doseq [p ["/v1/private" "/no-such-route"]]
          (let [response (await (request base p #js {:method "OPTIONS"
                                                    :headers #js {:Origin "https://example.invalid"}}))]
            (assert/equal (.-status response) 204 (str "unauthenticated OPTIONS " p))
            (assert/equal (await (.text response)) "")
            (assert/equal (.get (.-headers response) "access-control-allow-origin") "https://example.invalid")))
        (let [unknown (await (request base "/no-such-route" #js {:headers good}))]
          (assert/equal (.-status unknown) 404 "authorized unknown route remains 404"))
        ;; Repeat to guard against per-request state leaking between credentials.
        (await (assert-json (await (request base "/v1/private" #js {:headers good})) 200 success "authorized repeat"))
        (await (assert-json (await (request base "/v1/private" #js {})) 401 unauthorized "unauthorized repeat"))
        (println "Auth HTTP contract passed against" module-path))
      (finally (await (.close app))))))

(run-contract)

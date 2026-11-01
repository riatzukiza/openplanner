(ns openplanner.contract.blob-http-contract
  "Isolated HTTP and filesystem contract for the reference blob routes.

  Runs a real loopback Fastify server with the real auth and multipart plugins.
  OPENPLANNER_BLOB_MODULE optionally selects a .cljs module loaded by nbb or
  an ESM/TS module. Both must export blobRoutes. No Mongo or live services."
  (:require ["fastify" :as fastify]
            ["@fastify/sensible" :as sensible]
            ["@fastify/multipart" :as multipart]
            ["node:assert/strict" :as assert]
            ["node:crypto" :as crypto]
            ["node:fs" :as fs]
            ["node:os" :as os]
            ["node:path" :as path]
            ["node:url" :as url]
            ["nbb" :as nbb]))

(def test-key "synthetic-blob-contract-key")
(def auth #js {:Authorization (str "Bearer " test-key)})
(defn file-url [p] (str (url/pathToFileURL (path/resolve p))))
(defn ^:async request [base suffix options] (await (js/fetch (str base suffix) options)))
(defn digest [buffer] (.digest (.update (crypto/createHash "sha256") buffer) "hex"))
(defn file-form [field name mime data]
  (let [form (js/FormData.)]
    (.append form field (js/Blob. #js [data] #js {:type mime}) name)
    form))
(defn ^:async post-form [base form]
  (await (request base "/v1/blobs" #js {:method "POST" :headers auth :body form})))
(defn ^:async body-bytes [response] (js/Buffer.from (await (.arrayBuffer response))))
(defn ^:async assert-json [response status expected label]
  (assert/equal (.-status response) status label)
  (assert/deepEqual (await (.json response)) expected label))

(defn ^:async run-contract []
  (let [module-path (or (.-OPENPLANNER_BLOB_MODULE js/process.env) "src/routes/v1/blobs.ts")
        route-module (if (.endsWith module-path ".cljs")
                       (await (nbb/loadFile (path/resolve module-path)))
                       (await (js/import (file-url module-path))))
        auth-module (await (js/import (file-url "src/plugins/auth.ts")))
        dir (fs/mkdtempSync (path/join (os/tmpdir) "openplanner-blob-contract-"))
        app (fastify/default #js {:logger false})]
    (try
      (.decorate app "openplannerConfig" #js {:dataDir dir})
      (await (.register app sensible/default))
      (await (.register app multipart/default #js {:limits #js {:fileSize (* 200 1024 1024)}}))
      (await (.register app (.-authPlugin auth-module) #js {:apiKey test-key}))
      (await (.register app (.-blobRoutes route-module) #js {:prefix "/v1"}))
      (await (.listen app #js {:host "127.0.0.1" :port 0}))
      (let [base (str "http://127.0.0.1:" (.-port (.address (.-server app))))
            no-auth (await (request base "/v1/blobs/anything" #js {}))
            denied (await (request base "/v1/blobs" #js {:method "POST"}))
            preflight (await (request base "/v1/blobs" #js {:method "OPTIONS" :headers #js {:Origin "https://example.invalid"}}))
            bad-id (await (request base "/v1/blobs/NOT-A-HASH" #js {:headers auth}))
            missing (await (request base (str "/v1/blobs/" (.repeat "0" 64)) #js {:headers auth}))]
        (await (assert-json no-auth 401 #js {:statusCode 401 :error "Unauthorized" :message "Missing/invalid Authorization: Bearer <API_KEY>"} "GET requires auth"))
        (await (assert-json denied 401 #js {:statusCode 401 :error "Unauthorized" :message "Missing/invalid Authorization: Bearer <API_KEY>"} "POST requires auth"))
        (assert/equal (.-status preflight) 204 "unauthenticated preflight")
        (assert/equal (.get (.-headers preflight) "access-control-allow-origin") "https://example.invalid")
        (await (assert-json bad-id 400 #js {:error "invalid sha256"} "invalid digest"))
        (await (assert-json missing 404 #js {:error "blob not found"} "valid but missing digest"))
        (let [input (js/Buffer.from #js [0 1 2 10 13 127 128 254 255])
              hash (digest input)
              upload (await (post-form base (file-form "file" "sample.bin" "application/x-contract" input)))
              upload-body (await (.json upload))
              location (path/join dir "blobs" "sha256" (.slice hash 0 2) (.slice hash 2 4) hash)]
          (assert/equal (.-status upload) 200 "upload succeeds")
          (assert/deepEqual upload-body #js {:ok true :sha256 hash :mime "application/x-contract" :name "sample.bin" :size (.-length input)} "upload metadata")
          (assert/deepEqual (fs/readFileSync location) input "content-addressed sharded storage")
          (let [download (await (request base (str "/v1/blobs/" hash) #js {:headers auth}))]
            (assert/equal (.-status download) 200)
            (assert/equal (.get (.-headers download) "content-type") "application/octet-stream")
            (assert/deepEqual (await (body-bytes download)) input "binary round trip"))
          ;; The original deliberately does not overwrite a file already at the
          ;; digest path, even if its bytes have changed outside this route.
          (let [sentinel (js/Buffer.from #js [9 8 7])]
            (fs/writeFileSync location sentinel)
            (let [again (await (post-form base (file-form "file" "again.bin" "application/octet-stream" input)))]
              (await (assert-json again 200 #js {:ok true :sha256 hash :mime "application/octet-stream" :name "again.bin" :size (.-length input)} "repeat metadata"))
              (assert/deepEqual (fs/readFileSync location) sentinel "repeat upload does not overwrite"))))
        (let [empty (js/Buffer.alloc 0)
              hash (digest empty)
              ;; req.file() accepts the first file irrespective of field name.
              upload (await (post-form base (file-form "different-field" "empty.txt" "text/plain" empty)))]
          (await (assert-json upload 200 #js {:ok true :sha256 hash :mime "text/plain" :name "empty.txt" :size 0} "empty file and alternate field name"))
          (let [download (await (request base (str "/v1/blobs/" hash) #js {:headers auth}))]
            (assert/equal (.-status download) 200)
            (assert/equal (.-length (await (body-bytes download))) 0 "empty file round trip")))
        (let [form (js/FormData.)]
          (.append form "text-only" "no file here")
          (await (assert-json (await (post-form base form)) 400 #js {:error "expected multipart field 'file'"} "multipart without file")))
        (let [not-multipart (await (request base "/v1/blobs" #js {:method "POST" :headers #js {:Authorization (str "Bearer " test-key) :Content-Type "application/json"} :body "{}"}))]
          (assert/equal (.-status not-multipart) 406 "non-multipart body"))
        (let [malformed (await (request base "/v1/blobs" #js {:method "POST" :headers #js {:Authorization (str "Bearer " test-key) :Content-Type "multipart/form-data"} :body "not multipart"}))]
          (await (assert-json malformed 500 #js {:statusCode 500 :error "Internal Server Error" :message "Multipart: Boundary not found"} "malformed multipart boundary")))
        (println "Blob HTTP contract passed against" module-path))
      (finally
        (await (.close app))
        (fs/rmSync dir #js {:recursive true :force true})))))

(run-contract)

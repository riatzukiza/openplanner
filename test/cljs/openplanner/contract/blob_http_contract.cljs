(ns openplanner.contract.blob-http-contract
  "Reference-first real HTTP/filesystem blob contract, with sequenced awaits."
  (:require ["fastify" :as fastify]
            ["@fastify/sensible" :as sensible]
            ["@fastify/multipart" :as multipart]
            ["node:assert/strict" :as assert]
            ["node:crypto" :as crypto]
            ["node:fs" :as fs]
            ["node:os" :as os]
            ["node:path" :as path]
            ["node:url" :as url]
            ["nbb" :as nbb]
            [openplanner.support.blob-cleanup :as cleanup]))

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
  (let [body (await (.json response))]
    (assert/equal (.-status response) status label)
    (assert/deepEqual body expected label)
    nil))

(defn ^:async run-access-cases [base]
  (let [unauthorized #js {:statusCode 401 :error "Unauthorized"
                          :message "Missing/invalid Authorization: Bearer <API_KEY>"}
        no-auth (await (request base "/v1/blobs/anything" #js {}))
        _ (await (assert-json no-auth 401 unauthorized "GET requires auth"))
        denied (await (request base "/v1/blobs" #js {:method "POST"}))
        _ (await (assert-json denied 401 unauthorized "POST requires auth"))
        preflight (await (request base "/v1/blobs" #js {:method "OPTIONS" :headers #js {:Origin "https://example.invalid"}}))
        _ (assert/equal (.-status preflight) 204 "unauthenticated preflight")
        _ (assert/equal (.get (.-headers preflight) "access-control-allow-origin") "https://example.invalid")
        bad-id (await (request base "/v1/blobs/NOT-A-HASH" #js {:headers auth}))
        _ (await (assert-json bad-id 400 #js {:error "invalid sha256"} "invalid digest"))
        missing (await (request base (str "/v1/blobs/" (.repeat "0" 64)) #js {:headers auth}))
        _ (await (assert-json missing 404 #js {:error "blob not found"} "missing digest"))]
    true))

(defn ^:async run-binary-cases [base dir]
  (let [input (js/Buffer.from #js [0 1 2 10 13 127 128 254 255])
        hash (digest input)
        upload (await (post-form base (file-form "file" "sample.bin" "application/x-contract" input)))
        upload-body (await (.json upload))
        location (path/join dir "blobs" "sha256" (.slice hash 0 2) (.slice hash 2 4) hash)
        _ (assert/equal (.-status upload) 200)
        _ (assert/deepEqual upload-body #js {:ok true :sha256 hash :mime "application/x-contract" :name "sample.bin" :size (.-length input)})
        _ (assert/deepEqual (fs/readFileSync location) input "sharded bytes")
        download (await (request base (str "/v1/blobs/" hash) #js {:headers auth}))
        downloaded (await (body-bytes download))
        _ (assert/equal (.-status download) 200)
        _ (assert/equal (.get (.-headers download) "content-type") "application/octet-stream")
        _ (assert/deepEqual downloaded input "binary round trip")
        sentinel (js/Buffer.from #js [9 8 7])
        _ (fs/writeFileSync location sentinel)
        again (await (post-form base (file-form "file" "again.bin" "application/octet-stream" input)))
        _ (await (assert-json again 200 #js {:ok true :sha256 hash :mime "application/octet-stream" :name "again.bin" :size (.-length input)} "repeat metadata"))
        _ (assert/deepEqual (fs/readFileSync location) sentinel "repeat does not overwrite")]
    true))

(defn ^:async run-input-cases [base]
  (let [empty (js/Buffer.alloc 0)
        empty-hash (digest empty)
        empty-upload (await (post-form base (file-form "different-field" "empty.txt" "text/plain" empty)))
        _ (await (assert-json empty-upload 200 #js {:ok true :sha256 empty-hash :mime "text/plain" :name "empty.txt" :size 0} "empty and alternate field"))
        empty-download (await (request base (str "/v1/blobs/" empty-hash) #js {:headers auth}))
        empty-bytes (await (body-bytes empty-download))
        _ (assert/equal (.-status empty-download) 200)
        _ (assert/equal (.-length empty-bytes) 0)
        form (js/FormData.)
        _ (.append form "text-only" "no file here")
        no-file (await (post-form base form))
        _ (await (assert-json no-file 400 #js {:error "expected multipart field 'file'"} "multipart without file"))
        not-multipart (await (request base "/v1/blobs" #js {:method "POST" :headers #js {:Authorization (str "Bearer " test-key) :Content-Type "application/json"} :body "{}"}))
        _ (assert/equal (.-status not-multipart) 406 "non-multipart body")
        malformed (await (request base "/v1/blobs" #js {:method "POST" :headers #js {:Authorization (str "Bearer " test-key) :Content-Type "multipart/form-data"} :body "not multipart"}))
        _ (await (assert-json malformed 500 #js {:statusCode 500 :error "Internal Server Error" :message "Multipart: Boundary not found"} "malformed boundary"))]
    true))

(defn ^:async run-cases [base dir]
  (let [_ (await (run-access-cases base))
        _ (await (run-binary-cases base dir))
        _ (await (run-input-cases base))]
    true))

(defn ^:async run-contract []
  (let [module-path (or (.-OPENPLANNER_BLOB_MODULE js/process.env) "src/routes/v1/blobs.ts")
        route-module (if (.endsWith module-path ".cljs")
                       (await (nbb/loadFile (path/resolve module-path)))
                       (await (js/import (file-url module-path))))
        auth-module (await (js/import (file-url "src/plugins/auth.ts")))
        dir (fs/mkdtempSync (path/join (os/tmpdir) "openplanner-blob-contract-"))
        app (fastify/default #js {:logger false})]
    (try
      (let [_ (.decorate app "openplannerConfig" #js {:dataDir dir})
            _ (await (.register app sensible/default))
            _ (await (.register app multipart/default #js {:limits #js {:fileSize (* 200 1024 1024)}}))
            _ (await (.register app (.-authPlugin auth-module) #js {:apiKey test-key}))
            _ (await (.register app (.-blobRoutes route-module) #js {:prefix "/v1"}))
            _ (await (.listen app #js {:host "127.0.0.1" :port 0}))
            base (str "http://127.0.0.1:" (.-port (.address (.-server app))))
            _ (await (run-cases base dir))]
        (println "Blob HTTP contract passed against" module-path))
      (finally
        (let [_ (await (cleanup/close-and-remove! app dir))]
          nil)))))

(run-contract)

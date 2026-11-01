(ns openplanner.contract.blob-http-edge-contract
  "Additional isolated branch coverage for the blob route; reference by default.
  Selector OPENPLANNER_BLOB_MODULE accepts the same TS/ESM or nbb adapter as
  blob_http_contract.cljs. All paths and environment overrides are synthetic."
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

(def test-key "synthetic-blob-edge-key")
(def auth #js {:Authorization (str "Bearer " test-key)})
(defn url-for [p] (str (url/pathToFileURL (path/resolve p))))
(defn digest [b] (.digest (.update (crypto/createHash "sha256") b) "hex"))
(defn store-path [dir hash] (path/join dir "blobs" "sha256" (.slice hash 0 2) (.slice hash 2 4) hash))
(defn add-file [form field name b] (.append form field (js/Blob. #js [b] #js {:type "application/octet-stream"}) name))
(defn form-with [name b] (let [form (js/FormData.)] (add-file form "file" name b) form))
(defn ^:async app-start [routes auth-plugin cfg max-size]
  (let [app (fastify/default #js {:logger false})]
    (when-not (undefined? cfg) (.decorate app "openplannerConfig" cfg))
    (await (.register app sensible/default))
    (await (.register app multipart/default #js {:limits #js {:fileSize max-size}}))
    (await (.register app auth-plugin #js {:apiKey test-key}))
    (await (.register app routes #js {:prefix "/v1"}))
    (await (.listen app #js {:host "127.0.0.1" :port 0}))
    app))
(defn address [app] (str "http://127.0.0.1:" (.-port (.address (.-server app)))))
(defn ^:async req [app method route body]
  (await (js/fetch (str (address app) "/v1" route) #js {:method method :headers auth :body body})))
(defn ^:async expect [response status label]
  (assert/equal (.-status response) status label)
  (await (.json response)))

(defn ^:async run-contract []
  (let [module-path (or (.-OPENPLANNER_BLOB_MODULE js/process.env) "src/routes/v1/blobs.ts")
        route-module (if (.endsWith module-path ".cljs")
                       (await (nbb/loadFile (path/resolve module-path)))
                       (await (js/import (url-for module-path))))
        auth-module (await (js/import (url-for "src/plugins/auth.ts")))
        routes (.-blobRoutes route-module)
        auth-plugin (.-authPlugin auth-module)
        root (fs/mkdtempSync (path/join (os/tmpdir) "openplanner-blob-edge-"))
        old-cwd (.cwd js/process)
        old-env (.-OPENPLANNER_DATA_DIR js/process.env)
        data (js/Buffer.from "edge-body")
        hash (digest data)
        max-size (* 200 1024 1024)]
    (try
      ;; The documented fallback order is explicit dataDir, then env, then
      ;; relative default. CWD is changed only to our isolated temp directory.
      (.chdir js/process root)
      (let [env-dir (path/join root "env")
            explicit-dir (path/join root "explicit")]
        (set! (.-OPENPLANNER_DATA_DIR js/process.env) env-dir)
        (doseq [[cfg expected] [[#js {:dataDir explicit-dir} explicit-dir]
                                [#js {:dataDir nil} env-dir]
                                [js/undefined env-dir]
                                [#js {:dataDir ""} root]]]
          (let [app (await (app-start routes auth-plugin cfg max-size))]
            (try
              (let [r (await (req app "POST" "/blobs" (form-with "fallback.bin" data)))]
                (assert/equal (.-status r) 200 "fallback upload")
                (await (.text r))
                (assert/deepEqual (fs/readFileSync (store-path expected hash)) data "selected storage directory"))
              (finally (await (.close app))))))
        (js-delete js/process.env "OPENPLANNER_DATA_DIR")
        (let [app (await (app-start routes auth-plugin js/undefined max-size))]
          (try
            (let [r (await (req app "POST" "/blobs" (form-with "default.bin" data)))]
              (assert/equal (.-status r) 200 "default path upload")
              (await (.text r))
              (assert/deepEqual (fs/readFileSync (store-path (path/join root "openplanner-lake") hash)) data))
            (finally (await (.close app))))))
      (let [data-dir (path/join root "ordinary")
            app (await (app-start routes auth-plugin #js {:dataDir data-dir} max-size))]
        (try
          (doseq [bad [(.repeat "a" 63) (.repeat "A" 64) (str "a" (.repeat "0" 62) "%2F")]]
            (let [body (await (expect (await (req app "GET" (str "/blobs/" bad) nil)) 400 "invalid hash variant"))]
              (assert/deepEqual body #js {:error "invalid sha256"})))
          (let [first-bytes (js/Buffer.from "first")
                second-bytes (js/Buffer.from "second")
                first-hash (digest first-bytes)
                second-hash (digest second-bytes)
                form (js/FormData.)]
            (add-file form "one" "first.bin" first-bytes)
            (add-file form "two" "second.bin" second-bytes)
            (let [body (await (expect (await (req app "POST" "/blobs" form)) 200 "multiple files"))]
              (assert/equal (.-name body) "first.bin")
              (assert/equal (.-sha256 body) first-hash)
              (assert/equal (fs/existsSync (store-path data-dir second-hash)) false))
            (let [head (await (req app "HEAD" (str "/blobs/" first-hash) nil))]
              (assert/equal (.-status head) 200 "automatic HEAD")
              (assert/equal (.get (.-headers head) "content-type") "application/octet-stream")
              (assert/equal (.get (.-headers head) "content-length") (str (.-length first-bytes)))
              (assert/equal (await (.text head)) "")))
          (let [u (await (expect (await (req app "POST" "/blobs" (form-with "résumé.txt" (js/Buffer.from "unicode-name")))) 200 "unicode filename"))]
            (assert/equal (.-name u) "résumé.txt" "unicode filename"))
          (let [bytes (js/Buffer.from "basename-case")
                body (await (expect (await (req app "POST" "/blobs" (form-with "../folder/name.bin" bytes))) 200 "filename handling"))]
            (assert/equal (.-name body) "name.bin" "multipart basename behavior")
            (assert/equal (.-sha256 body) (digest bytes)))
          ;; A directory at a syntactically valid digest path is not readable
          ;; as a blob and is mapped to the same 404 response.
          (let [fake-hash (.repeat "f" 64)
                as-dir (store-path data-dir fake-hash)]
            (fs/mkdirSync as-dir #js {:recursive true})
            (assert/deepEqual (await (expect (await (req app "GET" (str "/blobs/" fake-hash) nil)) 404 "path is directory")) #js {:error "blob not found"}))
          ;; A file that exists but is unreadable is also mapped to 404.
          (let [file-path (store-path data-dir (digest (js/Buffer.from "first")))]
            (try
              (fs/chmodSync file-path 0)
              (assert/deepEqual (await (expect (await (req app "GET" (str "/blobs/" (digest (js/Buffer.from "first"))) nil)) 404 "read denied")) #js {:error "blob not found"})
              (finally (fs/chmodSync file-path 384))))
          (finally (await (.close app)))))
      ;; mkdir and write errors are real filesystem errors, no mocked service.
      (let [not-dir (path/join root "not-a-directory")]
        (fs/writeFileSync not-dir "file")
        (let [app (await (app-start routes auth-plugin #js {:dataDir not-dir} max-size))]
          (try
            (let [body (await (expect (await (req app "POST" "/blobs" (form-with "x.bin" data))) 500 "mkdir failure"))]
              (assert/equal (.-code body) "ENOTDIR"))
            (finally (await (.close app))))))
      (let [data-dir (path/join root "no-write")
            parent-dir (path/dirname (store-path data-dir hash))]
        (fs/mkdirSync parent-dir #js {:recursive true})
        (let [app (await (app-start routes auth-plugin #js {:dataDir data-dir} max-size))]
          (try
            (fs/chmodSync parent-dir 365)
            (let [body (await (expect (await (req app "POST" "/blobs" (form-with "x.bin" data))) 500 "write denied"))]
              (assert/equal (.-code body) "EACCES"))
            (finally (fs/chmodSync parent-dir 493) (await (.close app))))))
      (let [app (await (app-start routes auth-plugin #js {:dataDir (path/join root "limited")} 3))]
        (try
          (let [body (await (expect (await (req app "POST" "/blobs" (form-with "large.bin" data))) 413 "multipart limit"))]
            (assert/equal (.-code body) "FST_REQ_FILE_TOO_LARGE"))
          (finally (await (.close app)))))
      (println "Blob edge HTTP contract passed against" module-path)
      (finally
        (.chdir js/process old-cwd)
        (if (undefined? old-env)
          (js-delete js/process.env "OPENPLANNER_DATA_DIR")
          (set! (.-OPENPLANNER_DATA_DIR js/process.env) old-env))
        (fs/rmSync root #js {:recursive true :force true})))))

(run-contract)

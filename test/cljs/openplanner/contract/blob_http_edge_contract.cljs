(ns openplanner.contract.blob-http-edge-contract
  "Reference-first blob HTTP edge contract with explicitly sequenced awaits."
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
(defn config-with-getter [dir]
  (let [config #js {}]
    (js/Object.defineProperty config "dataDir" #js {:get (fn [] dir)})
    config))
(defn ^:async app-start [routes auth-plugin cfg max-size]
  (let [app (fastify/default #js {:logger false})]
    (try
      (let [_ (when-not (undefined? cfg) (.decorate app "openplannerConfig" cfg))
            _ (await (.register app sensible/default))
            _ (await (.register app multipart/default #js {:limits #js {:fileSize max-size}}))
            _ (await (.register app auth-plugin #js {:apiKey test-key}))
            _ (await (.register app routes #js {:prefix "/v1"}))
            _ (await (.listen app #js {:host "127.0.0.1" :port 0}))]
        app)
      (catch :default error
        ;; Preserve the setup failure even if the best-effort close also fails.
        (let [_ (try (let [_ (await (.close app))] nil)
                     (catch :default _close-error nil))]
          (throw error))))))
(defn address [app] (str "http://127.0.0.1:" (.-port (.address (.-server app)))))
(defn ^:async req [app method route body]
  (await (js/fetch (str (address app) "/v1" route) #js {:method method :headers auth :body body})))
(defn ^:async expect [response status label]
  (let [body (await (.json response))]
    (assert/equal (.-status response) status label)
    body))
(defn ^:async each-seq! [items f]
  (when (seq items)
    (let [_ (await (f (first items)))
          _ (await (each-seq! (rest items) f))]
      true)))

(defn- record-close! [closed _app done]
  (reset! closed true)
  (done))

(defn- probe-auth-plugin [closed app _opts done]
  (.addHook app "onClose" (partial record-close! closed))
  (done))

(defn- failing-routes [_app _opts done]
  (done (js/Error. "synthetic setup failure")))

(defn- setup-failed [_error] :failed)
(defn- setup-unexpected [_app] :unexpected)

(defn ^:async verify-setup-cleanup! [max-size]
  (let [closed (atom false)
        result (await (.then (app-start failing-routes (partial probe-auth-plugin closed) js/undefined max-size)
                             setup-unexpected setup-failed))]
    (assert/ok (= result :failed))
    (assert/ok @closed "server closes if plugin registration fails")
    true))

(defn ^:async run-config-case [routes auth-plugin max-size data hash [cfg expected]]
  (let [app (await (app-start routes auth-plugin cfg max-size))]
    (try
      (let [r (await (req app "POST" "/blobs" (form-with "fallback.bin" data)))
            _ (assert/equal (.-status r) 200 "fallback upload")
            _ (await (.text r))]
        (assert/deepEqual (fs/readFileSync (store-path expected hash)) data "selected storage directory"))
      (finally (let [_ (await (.close app))] nil)))))

(defn ^:async invalid-hash-case [app bad]
  (let [response (await (req app "GET" (str "/blobs/" bad) nil))
        body (await (expect response 400 "invalid hash variant"))]
    (assert/deepEqual body #js {:error "invalid sha256"})))

(defn ^:async run-ordinary [routes auth-plugin root max-size]
  (let [data-dir (path/join root "ordinary")
        app (await (app-start routes auth-plugin #js {:dataDir data-dir} max-size))]
    (try
      (let [_ (await (each-seq! [(.repeat "a" 63) (.repeat "A" 64) (str "a" (.repeat "0" 62) "%2F")]
                                (fn [bad] (invalid-hash-case app bad))))
            first-bytes (js/Buffer.from "first")
            second-bytes (js/Buffer.from "second")
            first-hash (digest first-bytes)
            second-hash (digest second-bytes)
            form (js/FormData.)
            _ (add-file form "one" "first.bin" first-bytes)
            _ (add-file form "two" "second.bin" second-bytes)
            multiple (await (req app "POST" "/blobs" form))
            body (await (expect multiple 200 "multiple files"))
            _ (assert/equal (.-name body) "first.bin")
            _ (assert/equal (.-sha256 body) first-hash)
            _ (assert/equal (fs/existsSync (store-path data-dir second-hash)) false)
            head (await (req app "HEAD" (str "/blobs/" first-hash) nil))
            head-text (await (.text head))
            _ (assert/equal (.-status head) 200 "automatic HEAD")
            _ (assert/equal (.get (.-headers head) "content-type") "application/octet-stream")
            _ (assert/equal (.get (.-headers head) "content-length") (str (.-length first-bytes)))
            _ (assert/equal head-text "")
            unicode (await (req app "POST" "/blobs" (form-with "résumé.txt" (js/Buffer.from "unicode-name"))))
            unicode-body (await (expect unicode 200 "unicode filename"))
            _ (assert/equal (.-name unicode-body) "résumé.txt")
            basename-bytes (js/Buffer.from "basename-case")
            basename-response (await (req app "POST" "/blobs" (form-with "../folder/name.bin" basename-bytes)))
            basename-body (await (expect basename-response 200 "filename handling"))
            _ (assert/equal (.-name basename-body) "name.bin")
            _ (assert/equal (.-sha256 basename-body) (digest basename-bytes))
            fake-hash (.repeat "f" 64)
            _ (fs/mkdirSync (store-path data-dir fake-hash) #js {:recursive true})
            directory-response (await (req app "GET" (str "/blobs/" fake-hash) nil))
            directory-body (await (expect directory-response 404 "path is directory"))
            _ (assert/deepEqual directory-body #js {:error "blob not found"})
            file-path (store-path data-dir first-hash)
            _ (try
                (fs/chmodSync file-path 0)
                (let [r (await (req app "GET" (str "/blobs/" first-hash) nil))
                      body (await (expect r 404 "read denied"))]
                  (assert/deepEqual body #js {:error "blob not found"}))
                (finally (fs/chmodSync file-path 384)))]
        true)
      (finally (let [_ (await (.close app))] nil)))))

(defn ^:async run-error-case [routes auth-plugin root max-size data hash kind]
  (let [not-dir (path/join root "not-a-directory")
        data-dir (case kind
                   :mkdir not-dir
                   :write (path/join root "no-write")
                   :limit (path/join root "limited")
                   :invalid false)
        _ (when (= kind :mkdir) (fs/writeFileSync not-dir "file"))
        parent-dir (when (= kind :write) (path/dirname (store-path data-dir hash)))
        _ (when (= kind :write) (fs/mkdirSync parent-dir #js {:recursive true}))
        app (await (app-start routes auth-plugin #js {:dataDir data-dir} (if (= kind :limit) 3 max-size)))]
    (try
      (let [_ (when (= kind :write) (fs/chmodSync parent-dir 365))
            r (await (req app "POST" "/blobs" (form-with "x.bin" data)))
            body (await (expect r (if (= kind :limit) 413 500) (str kind " error")))
            _ (assert/equal (.-code body) (case kind :mkdir "ENOTDIR" :write "EACCES"
                                               :limit "FST_REQ_FILE_TOO_LARGE" :invalid "ERR_INVALID_ARG_TYPE"))
            _ (when (= kind :invalid)
                (assert/deepEqual body #js {:statusCode 500 :code "ERR_INVALID_ARG_TYPE"
                                            :error "Internal Server Error"
                                            :message "The \"path\" argument must be of type string. Received type boolean (false)"}))]
        true)
      (finally
        (when (= kind :write) (fs/chmodSync parent-dir 493))
        (let [_ (await (.close app))] nil)))))

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
      (let [_ (await (verify-setup-cleanup! max-size))
            _ (.chdir js/process root)
            env-dir (path/join root "env")
            explicit-dir (path/join root "explicit")
            _ (set! (.-OPENPLANNER_DATA_DIR js/process.env) env-dir)
            cases [[#js {:dataDir explicit-dir} explicit-dir]
                   [(config-with-getter explicit-dir) explicit-dir]
                   [#js {:dataDir nil} env-dir]
                   [js/undefined env-dir]
                   [#js {:dataDir ""} root]]
            _ (await (each-seq! cases (fn [v] (run-config-case routes auth-plugin max-size data hash v))))
            _ (js-delete js/process.env "OPENPLANNER_DATA_DIR")
            _ (await (run-config-case routes auth-plugin max-size data hash [js/undefined (path/join root "openplanner-lake")]))
            _ (await (run-ordinary routes auth-plugin root max-size))
            _ (await (each-seq! [:mkdir :write :limit :invalid]
                                (fn [kind] (run-error-case routes auth-plugin root max-size data hash kind))))]
        (println "Blob edge HTTP contract passed against" module-path))
      (finally
        (.chdir js/process old-cwd)
        (if (undefined? old-env)
          (js-delete js/process.env "OPENPLANNER_DATA_DIR")
          (set! (.-OPENPLANNER_DATA_DIR js/process.env) old-env))
        (fs/rmSync root #js {:recursive true :force true})))))

(run-contract)

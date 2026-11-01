(ns openplanner.contract.backend-slice-http-contract
  "Reference-first HTTP composition contract for auth, OpenAPI and blobs.

  This is a deliberately partial app, not buildApp or a full server parity
  claim. OPENPLANNER_SLICE_IMPL=nbb selects the candidate builder."
  (:require-macros [openplanner.support.serial :refer [serial]])
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

(def test-key "synthetic-slice-contract-key")
(defn file-url [p] (str (url/pathToFileURL (path/resolve p))))
(defn ^:async reference-app [config]
  (let [auth (await (js/import (file-url "src/plugins/auth.ts")))
        openapi (await (js/import (file-url "src/routes/v1/openapi.ts")))
        blobs (await (js/import (file-url "src/routes/v1/blobs.ts")))
        app (fastify/default #js {:logger false :bodyLimit (* 50 1024 1024) :pluginTimeout 120000})]
    (serial
      (.decorate app "openplannerConfig" config)
      (await (.register app sensible/default))
      (await (.register app multipart/default #js {:limits #js {:fileSize (* 200 1024 1024)}}))
      (await (.register app (.-authPlugin auth) config))
      (await (.register app (.-openApiRoutes openapi) #js {:prefix "/v1"}))
      (await (.register app (.-blobRoutes blobs) #js {:prefix "/v1"}))
      app)))

(defn ^:async run-contract []
  (let [selector (.-OPENPLANNER_SLICE_IMPL js/process.env)
        _ (when-not (or (nil? selector) (= selector "reference") (= selector "nbb"))
            (throw (js/Error. (str "Unsupported OPENPLANNER_SLICE_IMPL: " selector))))
        dir (fs/mkdtempSync (path/join (os/tmpdir) "openplanner-slice-contract-"))
        config #js {:apiKey test-key :dataDir dir}
        candidate? (= "nbb" selector)
        app* (atom nil)]
    (try
      (serial
        (let [app (if candidate?
                    (do
                      (nbb/addClassPath (path/resolve "src/cljs"))
                      (let [module (await (nbb/loadFile (path/resolve "test/cljs/openplanner/support/application_adapter.cljs")))]
                        (await ((.-buildSliceApp module) config))))
                    (await (reference-app config)))]
          (serial
            (reset! app* app)
            (await (.listen app #js {:host "127.0.0.1" :port 0}))
            (let [base (str "http://127.0.0.1:" (.-port (.address (.-server app))))
                  headers #js {:Authorization (str "Bearer " test-key)}
                  denied (await (js/fetch (str base "/v1/openapi.json")))
                  openapi (await (js/fetch (str base "/v1/openapi.json") #js {:headers headers}))
                  schema-text (await (.text openapi))
                  form (js/FormData.)
                  bytes (js/Buffer.from #js [0 7 42 128 255])]
              (serial
                (assert/equal (.-status denied) 401 "auth hook applies to sibling OpenAPI route")
                (assert/equal (.-status openapi) 200)
                (assert/equal (.digest (.update (crypto/createHash "sha256") schema-text) "hex")
                              "745dbaa1873009d92b780c47c211e9c05c9c0ca82b10d642e9dc3637f7a0a702")
                (.append form "file" (js/Blob. #js [bytes] #js {:type "application/octet-stream"}) "slice.bin")
                (let [denied-upload (await (js/fetch (str base "/v1/blobs") #js {:method "POST" :body (js/FormData.)}))
                      upload (await (js/fetch (str base "/v1/blobs") #js {:method "POST" :headers headers :body form}))
                      payload (await (.json upload))
                      download (await (js/fetch (str base "/v1/blobs/" (.-sha256 payload)) #js {:headers headers}))]
                  (serial
                    (assert/equal (.-status denied-upload) 401 "auth applies to sibling blob route")
                    (assert/equal (.-status upload) 200)
                    (assert/equal (.-status download) 200)
                    (assert/deepEqual (js/Buffer.from (await (.arrayBuffer download))) bytes)))))))
        (println "Backend slice composition contract passed against" (if candidate? "nbb" "TS reference")))
      (finally
        (try
          (let [_ (await (when @app* (.close @app*)))] nil)
          (finally (fs/rmSync dir #js {:recursive true :force true})))))))

(run-contract)

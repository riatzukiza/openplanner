(ns openplanner.contract.blob-http-error-inheritance-contract
  "Synthetic host-error inheritance regression over real loopback HTTP."
  (:require ["fastify" :as fastify]
            ["node:assert/strict" :as assert]
            ["node:fs" :as fs]
            ["node:os" :as os]
            ["node:path" :as path]
            ["node:url" :as url]
            ["nbb" :as nbb]
            [openplanner.support.blob-cleanup :as cleanup]))

(defn file-url [p] (str (url/pathToFileURL (path/resolve p))))

(defn synthetic-error [kind]
  (let [error? (not= kind :object)
        proto (js/Object.create (if error? (.-prototype js/Error) (.-prototype js/Object)))
        _ (if (= kind :accessor-error)
            (do
              (js/Object.defineProperty proto "statusCode" #js {:get (fn [] 418)})
              (js/Object.defineProperty proto "code" #js {:get (fn [] "SYNTHETIC_CODE")}))
            (js/Object.assign proto #js {:statusCode 418 :code "SYNTHETIC_CODE"}))
        value (if error? (js/Error. "synthetic error") #js {:message "synthetic error"})]
    (js/Object.setPrototypeOf value proto)))

(defn rejected-file [value]
  (fn [] (js/Promise.reject value)))

(defn ^:async run-case [routes kind]
  (let [dir (fs/mkdtempSync (path/join (os/tmpdir) "blob-inherited-error-"))
        app (fastify/default #js {:logger false})
        value (synthetic-error kind)]
    (try
      (let [_ (.decorate app "openplannerConfig" #js {:dataDir dir})
            _ (.decorateRequest app "file" (rejected-file value))
            _ (await (.register app routes))
            _ (await (.listen app #js {:host "127.0.0.1" :port 0}))
            base (str "http://127.0.0.1:" (.-port (.address (.-server app))))
            response (await (js/fetch (str base "/blobs") #js {:method "POST"}))
            body (await (.json response))
            expected (if (not= kind :object)
                       #js {:statusCode 418 :code "SYNTHETIC_CODE" :error "I'm a Teapot" :message "synthetic error"}
                       #js {:message "synthetic error"})]
        (assert/equal (.-status response) 418 (str kind " status"))
        (assert/deepEqual body expected (str kind " body")))
      (finally (let [_ (await (cleanup/close-and-remove! app dir))] nil)))))

(defn ^:async run-contract []
  (let [module-path (or (.-OPENPLANNER_BLOB_MODULE js/process.env) "src/routes/v1/blobs.ts")
        module (if (.endsWith module-path ".cljs")
                 (await (nbb/loadFile (path/resolve module-path)))
                 (await (js/import (file-url module-path))))
        _ (await (run-case (.-blobRoutes module) :error))
        _ (await (run-case (.-blobRoutes module) :accessor-error))
        _ (await (run-case (.-blobRoutes module) :object))]
    (println "Blob inherited host-error HTTP contract passed against" module-path)))

(run-contract)

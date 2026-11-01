(ns openplanner.extern.js.http.blob
  "Side-by-side nbb implementation of the filesystem-backed blob routes.

  The reference TypeScript module remains registered in production. These
  functions preserve its content-addressing, existing-file, and error behavior."
  (:require ["node:crypto" :as crypto]
            ["node:fs/promises" :as fs]
            ["node:path" :as path]))

(defn- nullish-or [value fallback]
  (if (nil? value) fallback value))

(defn- blobs-dir [app]
  (let [config (.-openplannerConfig app)
        configured (when (some? config) (.-dataDir config))
        data-dir (nullish-or configured
                             (nullish-or (.-OPENPLANNER_DATA_DIR js/process.env)
                                         "./openplanner-lake"))]
    (path/join data-dir "blobs" "sha256")))

(defn- blob-path [dir sha]
  (path/join dir (.slice sha 0 2) (.slice sha 2 4) sha))

(defn- sha256-bytes [buffer]
  (.digest (.update (crypto/createHash "sha256") buffer) "hex"))

(defn- ^:async post-blob [app request reply]
  (let [dir (blobs-dir app)]
    (await (fs/mkdir dir #js {:recursive true}))
    (let [part (await (.file request))]
      (if (nil? part)
        (.send (.status reply 400) #js {:error "expected multipart field 'file'"})
        (let [buffer (await (.toBuffer part))
              sha (sha256-bytes buffer)
              output (blob-path dir sha)]
          (await (fs/mkdir (path/dirname output) #js {:recursive true}))
          (try
            (await (fs/access output))
            (catch :default _error
              (await (fs/writeFile output buffer))))
          #js {:ok true :sha256 sha :mime (.-mimetype part)
               :name (.-filename part) :size (.-length buffer)})))))

(defn- ^:async get-blob [app request reply]
  (let [sha (.. request -params -sha256)]
    (if-not (re-matches #"^[a-f0-9]{64}$" sha)
      (.send (.status reply 400) #js {:error "invalid sha256"})
      (let [file (blob-path (blobs-dir app) sha)]
        (try
          (let [buffer (await (fs/readFile file))]
            (.header reply "Content-Type" "application/octet-stream")
            (.send reply buffer))
          (catch :default _error
            (.send (.status reply 404) #js {:error "blob not found"})))))))

(defn blob-routes
  "Fastify callback plugin; wrappers expose native JS-callable route handlers."
  [app _options done]
  (.post app "/blobs" (fn [request reply] (post-blob app request reply)))
  (.get app "/blobs/:sha256" (fn [request reply] (get-blob app request reply)))
  (done))

#js {:blobRoutes blob-routes}

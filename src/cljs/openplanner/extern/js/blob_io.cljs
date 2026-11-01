(ns openplanner.extern.js.blob-io
  "Node/Fastify operations returning Clojure data where host handles end.

  Upload and download buffers remain private to these awaited operations; they
  are neither converted into large persistent vectors nor exposed to domain
  functions. Host failures are decoded into tagged Clojure outcomes."
  (:require ["node:crypto" :as crypto]
            ["node:fs/promises" :as fs]
            ["node:http" :as http]
            ["node:path" :as path]
            ["node:util" :as util]))

(defn- error-property [error key]
  ;; Never invoke foreign getters: only read an own data property of a
  ;; non-Proxy value. Fastify's error objects are not always native Errors.
  ;; Node's intrinsic identifies Proxy objects, including revoked ones.
  (try
    (when-not (.. util -types (isProxy error))
      (let [descriptor (js/Object.getOwnPropertyDescriptor error key)]
        (when (and (some? descriptor) (js/Object.hasOwn descriptor "value"))
          (.-value descriptor))))
    (catch :default _error nil)))

(defn- safe-string? [value]
  (try (string? value) (catch :default _error false)))

(defn- safe-number? [value]
  (try (number? value) (catch :default _error false)))

(defn- error-data [error]
  (let [raw-status (error-property error "statusCode")
        status (if (and (safe-number? raw-status) (js/Number.isInteger raw-status)
                        (<= 400 raw-status 599))
                 raw-status
                 500)
        code (error-property error "code")
        message (error-property error "message")
        title (aget http/STATUS_CODES (str status))]
    {:category :host
     :status status
     :code (when (safe-string? code) code)
     :message (if (safe-string? message) message "Internal Server Error")
     :title (if (safe-string? title) title "Internal Server Error")}))

(defn- error-outcome [error]
  {:outcome :error :error (error-data error)})

(defn- success-outcome [value]
  {:outcome :ok :value value})

(defn- read-property [object key _]
  (aget object key))

(defn- field-success [value] {:ok? true :value value})
(defn- field-failure [_error] {:ok? false})

(defn- safe-field [object key]
  ;; Native Promise rejection callbacks catch getter/Proxy failures without
  ;; asking SCI to inspect the foreign thrown value.
  (.then (.then (js/Promise.resolve nil) (partial read-property object key))
         field-success field-failure))

(defn- error-instance [value _]
  (instance? js/Error value))

(defn- plain-message-body [value _]
  ;; The characterized non-Error case has one own enumerable string message.
  ;; Do not traverse unknown objects or invoke arbitrary nested accessors.
  (when-not (or (nil? value) (.. util -types (isProxy value)))
    (let [keys (js/Object.keys value)
          descriptor (js/Object.getOwnPropertyDescriptor value "message")]
      (when (and (= 1 (.-length keys))
                 (= "message" (aget keys 0))
                 (some? descriptor)
                 (js/Object.hasOwn descriptor "value")
                 (safe-string? (.-value descriptor)))
        {"message" (.-value descriptor)}))))

(defn- safe-call [f value]
  (.then (.then (js/Promise.resolve nil) (partial f value))
         field-success field-failure))

(defn- ^:async async-error-outcome [error]
  (let [status-field (await (safe-field error "statusCode"))
        code-field (await (safe-field error "code"))
        message-field (await (safe-field error "message"))
        is-error (await (safe-call error-instance error))
        raw-status (:value status-field)
        status (if (and (:ok? status-field) (safe-number? raw-status)
                        (js/Number.isInteger raw-status) (<= 400 raw-status 599))
                 raw-status 500)
        code (:value code-field)
        message (:value message-field)
        title (aget http/STATUS_CODES (str status))
        non-error? (and (:ok? is-error) (false? (:value is-error)))
        body (when non-error? (await (safe-call plain-message-body error)))]
    {:outcome :error
     :error (cond-> {:category :host
                     :status status
                     :code (when (and (:ok? code-field) (safe-string? code)) code)
                     :message (if (and (:ok? message-field) (safe-string? message))
                                message "Internal Server Error")
                     :title (if (safe-string? title) title "Internal Server Error")}
              (and non-error? (:ok? body) (some? (:value body)))
              (assoc :wire-value (:value body)))}))

(defn- reject-to-outcome [error]
  (async-error-outcome error))

(defn- settle [promise]
  ;; Attach rejection handling before SCI awaits the promise. In particular,
  ;; an adversarial rejected JS value need not be an Error object.
  (.then promise success-outcome reject-to-outcome))

(defn- decode-string [value]
  (cond
    (nil? value) {:kind :missing}
    (safe-string? value) {:kind :value :value value}
    :else {:kind :invalid}))

(defn- ^:async property-result [object key]
  ;; Native Promise dispatch catches even a getter that throws a revoked JS
  ;; Proxy before SCI's exception machinery can inspect the thrown value.
  (if (nil? object)
    {:outcome :ok :value nil}
    (await (settle (.then (js/Promise.resolve nil) (partial read-property object key))))))

(defn- ^:async nested-property [object outer inner]
  (let [parent (await (property-result object outer))]
    (if (= :error (:outcome parent))
      parent
      (await (property-result (:value parent) inner)))))

(defn ^:async configured-data-dir [app]
  (let [property (await (nested-property app "openplannerConfig" "dataDir"))]
    (if (= :error (:outcome property))
      {:kind :error}
      (let [value (:value property)
            decoded (decode-string value)]
        (if (= :invalid (:kind decoded))
          ;; Preserve Node's error classification/message without letting the
          ;; invalid host value leave this boundary.
          (try (path/join value "blobs" "sha256") decoded
               (catch :default error {:kind :invalid :error (error-data error)}))
          decoded)))))

(defn ^:async environment-data-dir []
  (let [property (await (nested-property js/process "env" "OPENPLANNER_DATA_DIR"))]
    (if (= :error (:outcome property)) {:kind :error} (decode-string (:value property)))))

(defn ^:async request-sha [request]
  (let [property (await (nested-property request "params" "sha256"))]
    (if (= :error (:outcome property)) {:kind :error} (decode-string (:value property)))))

(defn decode-failure []
  {:outcome :error
   :error {:category :decode :status 500 :code nil
           :message "Unable to decode blob request data"
           :title "Internal Server Error"}})

(defn blobs-dir [_app selected]
  ;; Decode Node's synchronous path failure here: SCI otherwise rewraps it
  ;; when it crosses a namespace boundary and drops the code on the wire.
  (case (:kind selected)
    :error (decode-failure)
    :invalid (if (:error selected) {:outcome :error :error (:error selected)} (decode-failure))
    (try {:outcome :ok :path (path/join (:value selected) "blobs" "sha256")}
         (catch :default error (error-outcome error)))))

(defn blob-path [dir sha]
  (try {:outcome :ok :path (path/join dir (.slice sha 0 2) (.slice sha 2 4) sha)}
       (catch :default error (error-outcome error))))

(defn ^:async ensure-dir! [dir]
  (try
    (let [result (await (settle (fs/mkdir dir #js {:recursive true})))]
      (if (= :error (:outcome result)) result {:outcome :ok}))
    (catch :default error (error-outcome error))))

(defn- metadata-field? [value]
  (try (or (nil? value) (safe-string? value))
       (catch :default _error false)))

(defn- ^:async metadata-outcome [part sha size]
  (let [mime-result (await (property-result part "mimetype"))]
    (if (= :error (:outcome mime-result))
      {:outcome :invalid-metadata :fields [:metadata]}
      (let [name-result (await (property-result part "filename"))]
        (if (= :error (:outcome name-result))
          {:outcome :invalid-metadata :fields [:metadata]}
          (let [mime (:value mime-result)
                name (:value name-result)
                invalid (cond-> []
                          (not (metadata-field? mime)) (conj :mime)
                          (not (metadata-field? name)) (conj :name))]
            (if (seq invalid)
              {:outcome :invalid-metadata :fields invalid}
              {:outcome :ok
               :metadata (cond-> (array-map :sha256 sha)
                           (not (undefined? mime)) (assoc :mime mime)
                           (not (undefined? name)) (assoc :name name)
                           true (assoc :size size))})))))))

(defn- ^:async persist-if-absent! [output buffer]
  (let [access (await (settle (fs/access output)))]
    (if (= :ok (:outcome access))
      {:outcome :ok}
      (let [write (await (settle (fs/writeFile output buffer)))]
        (if (= :error (:outcome write)) write {:outcome :ok})))))

(defn- invoke-buffer [part _]
  (.toBuffer part))

(defn- ^:async store-part! [part dir]
  (let [buffer-result (await (settle (.then (js/Promise.resolve nil) (partial invoke-buffer part))))]
    (if (= :error (:outcome buffer-result))
      buffer-result
      (let [buffer (:value buffer-result)
            sha (.digest (.update (crypto/createHash "sha256") buffer) "hex")
            path-result (blob-path dir sha)]
        (if (= :error (:outcome path-result))
          path-result
          (let [output (:path path-result)
                mkdir-result (await (ensure-dir! (path/dirname output)))]
            (if (= :error (:outcome mkdir-result))
              mkdir-result
              (let [saved (await (persist-if-absent! output buffer))]
                (if (= :error (:outcome saved))
                  saved
                  (await (metadata-outcome part sha (.-length buffer))))))))))))

(defn- invoke-file [request _]
  (.file request))

(defn ^:async store-upload!
  "Consume the first file and return a tagged result with Clojure metadata.

  The host buffer is held only in this call and no host object is returned."
  [request dir]
  (try
    (let [file-result (await (settle (.then (js/Promise.resolve nil) (partial invoke-file request))))]
      (if (= :error (:outcome file-result))
        file-result
        (let [part (:value file-result)]
          (if (nil? part)
            {:outcome :no-file}
            (await (store-part! part dir))))))
    (catch :default error (error-outcome error))))

(defn ^:async send-file!
  "Send a file with bytes private to this effect; return a tagged CLJ result.

  The catch scope matches the reference GET route's read/header/send block."
  [reply file]
  (try
    (let [read-result (await (settle (fs/readFile file)))]
      (if (= :error (:outcome read-result))
        (let [code (get-in read-result [:error :code])]
          {:outcome :unavailable
           :reason (case code "ENOENT" :missing "EACCES" :access "EISDIR" :directory :other)})
        (let [buffer (:value read-result)]
          (.header reply "Content-Type" "application/octet-stream")
          (.send reply buffer)
          {:outcome :sent})))
    (catch :default _error
      ;; A send/header failure is mapped to the same 404 as the reference.
      ;; Its opaque thrown value is neither inspected nor exposed.
      {:outcome :unavailable :reason :other})))

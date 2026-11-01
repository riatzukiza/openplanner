(ns openplanner.extern.js.http.blob
  "Fastify host boundary for the side-by-side filesystem blob routes.

  Host request, reply and app handles remain here. Node effects return Clojure
  metadata; private byte buffers never enter the portable blob domain."
  (:require [openplanner.domain.blob :as blob]
            [openplanner.extern.js.blob-io :as io]
            [openplanner.shape.http.blob-response :as response]))

(defn- ^:async blobs-dir [app]
  (let [configured (await (io/configured-data-dir app))
        environment (if (= :missing (:kind configured))
                      (await (io/environment-data-dir))
                      {:kind :missing})]
    (io/blobs-dir app (blob/data-dir configured environment))))

(defn- emit-action [reply action]
  (case (:action action)
    :reply (.send (.status reply (:status action)) (clj->js (:body action)))
    :return (clj->js (:body action))
    :sent nil
    (throw (ex-info "Invalid blob operation outcome" (:detail action)))))

(defn- ^:async post-blob [app request reply]
  (let [dir-result (await (blobs-dir app))]
    (if (= :error (:outcome dir-result))
      (emit-action reply (response/error-action dir-result))
      (let [dir (:path dir-result)
            ensured (await (io/ensure-dir! dir))]
        (if (= :error (:outcome ensured))
          (emit-action reply (response/error-action ensured))
          (let [result (await (io/store-upload! request dir))]
            (emit-action reply (response/upload-action result))))))))

(defn- ^:async get-blob [app request reply]
  (let [decoded (await (io/request-sha request))
        sha (:value decoded)]
    (cond
      (= :error (:kind decoded)) (emit-action reply (response/error-action (io/decode-failure)))
      (not (and (= :value (:kind decoded)) (blob/valid-sha? sha)))
      (emit-action reply (response/invalid-sha-action))
      :else
      (let [dir-result (await (blobs-dir app))]
        (if (= :error (:outcome dir-result))
          (emit-action reply (response/error-action dir-result))
          (let [path-result (io/blob-path (:path dir-result) sha)]
            (if (= :error (:outcome path-result))
              (emit-action reply (response/error-action path-result))
              (let [result (await (io/send-file! reply (:path path-result)))]
                (emit-action reply (response/download-action result))))))))))

(defn blob-routes
  "Fastify callback plugin; the host adapter owns request and reply handles."
  [app _options done]
  (.post app "/blobs" (fn [request reply] (post-blob app request reply)))
  (.get app "/blobs/:sha256" (fn [request reply] (get-blob app request reply)))
  (done))

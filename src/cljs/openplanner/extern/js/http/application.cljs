(ns openplanner.extern.js.http.application
  "Composable nbb backend slice, not a replacement for the full buildApp.

  This builder registers only the independently characterized auth, OpenAPI
  and blob routes. The remaining API, Mongo, Kafka, tenant handling and root
  endpoint are intentionally absent; do not select it as the production app."
  (:require ["fastify" :as fastify]
            ["@fastify/multipart" :as multipart]
            ["@fastify/sensible" :as sensible]
            [openplanner.extern.js.fastify.auth :as fastify-auth]
            [openplanner.extern.js.http.blob :as blobs]
            [openplanner.extern.js.http.openapi :as openapi]
            [openplanner.infra.http.auth :as auth]))

(defn ^:async build-slice-app
  "Build an opaque Fastify handle from normalized Clojure configuration.

  The handle is host-specific and must remain inside the JS HTTP boundary."
  [config]
  (let [config-js (clj->js config)
        app (fastify/default #js {:logger false
                                 :bodyLimit (* 50 1024 1024)
                                 :pluginTimeout 120000})]
    (.decorate app "openplannerConfig" config-js)
    ;; nbb 1.6.214 does not sequence a bare non-final await. Binding each
    ;; result preserves registration order and propagates a rejected promise.
    (let [_sensible (await (.register app sensible/default))
          _multipart (await (.register app multipart/default #js {:limits #js {:fileSize (* 200 1024 1024)}}))
          _auth (await (.register app (fastify-auth/auth-plugin auth/decide-auth) config-js))
          _openapi (await (.register app openapi/open-api-routes #js {:prefix "/v1"}))
          _blobs (await (.register app blobs/blob-routes #js {:prefix "/v1"}))]
      app)))

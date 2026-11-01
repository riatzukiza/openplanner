(ns openplanner.application
  "Composable nbb backend slice, not a replacement for the full buildApp.

  This builder registers only the independently characterized auth, OpenAPI
  and blob routes. The remaining API, Mongo, Kafka, tenant handling and root
  endpoint are intentionally absent; do not select it as the production app."
  (:require ["fastify" :as fastify]
            ["@fastify/multipart" :as multipart]
            ["@fastify/sensible" :as sensible]
            [openplanner.auth-plugin :as auth]
            [openplanner.blob-routes :as blobs]
            [openplanner.openapi-route :as openapi]))

(defn- ^:async build-slice-app [config]
  (let [app (fastify/default #js {:logger false
                                 :bodyLimit (* 50 1024 1024)
                                 :pluginTimeout 120000})]
    (.decorate app "openplannerConfig" config)
    (await (.register app sensible/default))
    (await (.register app multipart/default #js {:limits #js {:fileSize (* 200 1024 1024)}}))
    (await (.register app auth/auth-plugin config))
    (await (.register app openapi/open-api-routes #js {:prefix "/v1"}))
    (await (.register app blobs/blob-routes #js {:prefix "/v1"}))
    app))

;; An ordinary wrapper is a native JS function for nbb/loadFile consumers;
;; the internal ^:async function returns the app promise.
(defn build-slice-app-js [config]
  (build-slice-app config))

#js {:buildSliceApp build-slice-app-js}

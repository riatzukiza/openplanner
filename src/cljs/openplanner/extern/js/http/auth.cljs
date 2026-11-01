(ns openplanner.extern.js.http.auth
  "Side-by-side nbb implementation of the Fastify auth and CORS hook.

  Keep reference path matching and bearer parsing unchanged during parity.
  Production still registers the TypeScript plugin."
  (:require ["fastify-plugin" :as fastify-plugin]))

(def ^:private public-paths #{"/v1/health" "/v1/metrics" "/"})

(defn- auth-hook [config request reply done]
  (let [headers (.-headers request)
        origin (.-origin headers)
        request-url (.-url request)]
    (.header reply "Access-Control-Allow-Origin" (if (string? origin) origin "*"))
    (.header reply "Vary" "Origin")
    (.header reply "Access-Control-Allow-Methods" "GET,POST,PUT,PATCH,DELETE,OPTIONS")
    (.header reply "Access-Control-Allow-Headers" "Authorization,Content-Type,X-Requested-With")
    (.header reply "Access-Control-Max-Age" "86400")
    (cond
      (= "OPTIONS" (.-method request))
      (.send (.status reply 204))

      (or (contains? public-paths request-url)
          (.startsWith request-url "/v1/public/"))
      (done)

      :else
      (let [header (.-authorization headers)
            token (when (and (string? header)
                             (.startsWith (.toLowerCase header) "bearer "))
                    (.slice header 7))]
        (if (and (some? token) (not= token "")
                 (identical? token (.-apiKey config)))
          (done)
          (.unauthorized reply "Missing/invalid Authorization: Bearer <API_KEY>"))))))

(defn- register-auth [app config done]
  (.addHook app "onRequest" (fn [request reply next]
                              (auth-hook config request reply next)))
  (done))

;; fastify-plugin deliberately removes encapsulation, matching the original
;; global hook when routes are registered as siblings after this plugin.
(def auth-plugin (fastify-plugin/default register-auth))

#js {:authPlugin auth-plugin}

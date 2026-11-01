(ns openplanner.extern.js.http.openapi
  "Side-by-side nbb implementation of the static OpenAPI HTTP route.

  This intentionally does not replace openApiSchemas, which remains owned by
  the reference TypeScript module and is used by search. The document captures
  the reference route at the parity checkpoint; update only with contract review."
  (:require [openplanner.shape.openapi-document :as document]))

(def response-schema
  {"summary" "OpenAPI 3.1 schema for documented OpenPlanner surfaces"
   "response" {"200" {"type" "object"
                       "required" ["openapi" "info" "paths" "components"]
                       "properties" {"openapi" {"type" "string"}
                                     "info" {"type" "object" "additionalProperties" true}
                                     "paths" {"type" "object" "additionalProperties" true}
                                     "components" {"type" "object" "additionalProperties" true}}
                       "additionalProperties" true}}})

(defn open-api-routes
  "Fastify callback-style plugin, returning a native JS-callable function in nbb."
  [app _options done]
  (.get app "/openapi.json" (clj->js {"schema" response-schema})
        (fn [_request reply]
          (.send reply (clj->js document/openapi-document))))
  (done))

;; nbb's loadFile resolves to its last expression for JavaScript/ESM consumers.
#js {:openApiRoutes open-api-routes}

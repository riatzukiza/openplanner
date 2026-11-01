(ns openplanner.extern.js.http.openapi
  "Side-by-side nbb implementation of the static OpenAPI HTTP route.

  This intentionally does not replace openApiSchemas, which remains owned by
  the reference TypeScript module and is used by search. The document captures
  the reference route at the parity checkpoint; update only with contract review."
  (:require [openplanner.shape.api-schemas :as schemas]))

(def openapi-document
  {"openapi" "3.1.0"
   "info" {"title" "OpenPlanner API"
           "version" "0.3.0"
           "description" "OpenPlanner event ledger, graph/semantic search index, and source-reference hydration API. This document currently covers the source-text redaction parity surface and vector search hydration contract."}
   "paths" {"/state/redaction" {"get" {"summary" "Validate source-text redaction state"
                                       "operationId" "getRedactionState"
                                       "parameters" [{"name" "strict"
                                                      "in" "query"
                                                      "required" false
                                                      "schema" {"type" "boolean"
                                                                "default" false}
                                                      "description" "When true, document-event text awaiting Migration 2 apply mode fails validation instead of producing warn status."}]
                                       "responses" {"200" {"description" "Redaction state ledger"
                                                           "content" {"application/json" {"schema" {"$ref" "#/components/schemas/RedactionStateResponse"}}}}}}}
            "/search/vector" {"post" {"summary" "Vector search with redacted-hit hydration"
                                      "operationId" "postVectorSearch"
                                      "requestBody" {"required" true
                                                     "content" {"application/json" {"schema" {"$ref" "#/components/schemas/VectorSearchRequest"}}}}
                                      "responses" {"200" {"description" "Chroma-compatible vector query result; redacted source-backed hits are hydrated into documents[]."
                                                          "content" {"application/json" {"schema" {"$ref" "#/components/schemas/VectorSearchResponse"}}}}}}}}
   "components" {"securitySchemes" {"bearerAuth" {"type" "http"
                                                  "scheme" "bearer"}}
                 "schemas" {"RedactionStateResponse" schemas/redaction-state-response
                            "VectorSearchRequest" schemas/vector-search-request
                            "VectorSearchResponse" schemas/vector-search-response}}
   "servers" [{"url" "http://127.0.0.1:7777/v1"
               "description" "local production container"}
              {"url" "http://127.0.0.1:7778/v1"
               "description" "local development container"}]
   "security" [{"bearerAuth" []}]})

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
          (.send reply (clj->js openapi-document))))
  (done))

;; nbb's loadFile resolves to its last expression for JavaScript/ESM consumers.
#js {:openApiRoutes open-api-routes}

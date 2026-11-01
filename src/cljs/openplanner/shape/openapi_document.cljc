(ns openplanner.shape.openapi-document
  "Portable, non-transforming declaration of the reference OpenAPI document.

  Construction does not validate; the separate Malli law test checks the
  supported subset, while the HTTP parity fixture checks exact wire output."
  (:require [openplanner.shape.api-schemas :as schemas]
            #?(:clj [openplanner.macro.openapi :refer [defopenapi]]
               :org.babashka/nbb [openplanner.macro.openapi :refer [defopenapi]]))
  #?(:cljs (:require-macros [openplanner.macro.openapi :refer [defopenapi]])))

(defopenapi openapi-document
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

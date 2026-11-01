(ns openplanner.extern.js.schemas
  "JavaScript export boundary for the side-by-side portable schema data."
  (:require [openplanner.shape.api-schemas :as schemas]))

#js {:openApiSchemas #js {:vectorSearchRequestSchema (clj->js schemas/vector-search-request)
                          :vectorSearchResponseSchema (clj->js schemas/vector-search-response)}
     :redactionStateSchemas #js {:stateQuerySchema (clj->js schemas/state-query)
                                 :redactionStateResponseSchema (clj->js schemas/redaction-state-response)}}

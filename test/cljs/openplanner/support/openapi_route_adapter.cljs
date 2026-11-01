(ns openplanner.support.openapi-route-adapter
  "Test-only ABI for the unchanged TypeScript/nbb HTTP contract fixture."
  (:require [openplanner.extern.js.http.openapi :as openapi]))

#js {:openApiRoutes openapi/open-api-routes}

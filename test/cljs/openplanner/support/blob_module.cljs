(ns openplanner.support.blob-module
  "Test-only JS ABI shim for the unchanged reference/candidate HTTP fixture."
  (:require [openplanner.extern.js.http.blob :as blob]))

#js {:blobRoutes blob/blob-routes}

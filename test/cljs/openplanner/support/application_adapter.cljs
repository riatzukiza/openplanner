(ns openplanner.support.application-adapter
  "Test-only ABI bridge; the native Fastify handle stays at this boundary."
  (:require [openplanner.extern.js.http.application :as app]))

(defn build-slice-app [config]
  (app/build-slice-app (js->clj config :keywordize-keys true)))

#js {:buildSliceApp build-slice-app}

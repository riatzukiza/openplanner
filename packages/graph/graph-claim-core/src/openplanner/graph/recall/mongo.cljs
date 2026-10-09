(ns openplanner.graph.recall.mongo
  "Trusted SDK/Mongo boundary. Request JSON has no authority port or scope.
   LGPL-3.0-or-later."
  (:require [openplanner.graph.recall.mongo-contract :as contract]))

(defn create-scoped-mongo-recall-js
  "Bind an existing SDK handle and a fresh trusted authority callback.
   RED placeholder: it must never fall through to the legacy REST writer."
  [_sdk _resolve-current-authority]
  (^:async fn [_request]
    (clj->js (contract/failed :unsupported-capability))))

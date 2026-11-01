(ns openplanner.infra.http.auth
  "Compose the characterized authorization policy over normalized data."
  (:require [openplanner.domain.auth :as domain]))

(defn decide-auth [request]
  (domain/decide-auth request))

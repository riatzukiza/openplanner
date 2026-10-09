(ns openplanner.graph.recall.mongo-contract
  "Stored recall authority comes from a trusted host callback, never request data.
   LGPL-3.0-or-later."
  (:require [malli.core :as m]
            [openplanner.graph.recall.contract :as recall]))

(def registry
  "Serializable contracts for the owning Mongo adapter's admitted event set."
  {::record [:map {:closed true}
             [:id [:ref :openplanner.graph.recall.contract/identity]]
             [:text [:string {:max 16384}]]]
   ::authority [:map {:closed true}
                [:scope [:ref :openplanner.graph.recall.contract/scope]]
                [:project [:ref :openplanner.graph.recall.contract/identity]]
                [:records [:vector {:max 384} [:ref ::record]]]]
   ::failure [:map {:closed true} [:stage [:= :graph]]
              [:code [:enum :invalid-request :invalid-authority :unsupported-capability
                      :transport-error :timeout :invalid-projection :storage-limit-exceeded
                      :invalid-embedding]]]
   ::failed [:map {:closed true} [:status [:= :failed]] [:hits [:vector {:max 0} :any]]
             [:failure [:ref ::failure]]
             [:feedback [:ref :openplanner.graph.recall.contract/feedback]]]
   ::result [:map {:closed true}
             [:version [:= 1]] [:field-status [:= :not-loaded]]
             [:selection [:or [:ref :openplanner.graph.recall.contract/result] [:ref ::failed]]]]})

(defn validator
  "Validate the named adapter contract without granting access."
  [key]
  (m/validator [:ref key] {:registry (merge (m/default-schemas) recall/registry registry)}))

(def valid-authority? (validator ::authority))
(def valid-result? (validator ::result))

(defn failed
  "Return a bounded failure without credential-bearing exception text."
  [code]
  {:version 1 :field-status :not-loaded
   :selection {:status :failed :hits [] :failure {:stage :graph :code code}
               :feedback {:status :not-requested :attempted 0 :completed 0}}})

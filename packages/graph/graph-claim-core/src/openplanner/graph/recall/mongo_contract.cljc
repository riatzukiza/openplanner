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
   ::component [:and [:ref :openplanner.graph.recall.contract/number]
                [:>= -1.0e100] [:<= 1.0e100]]
   ::embedding [:vector {:min 1 :max 8192} [:ref ::component]]
   ::index [:map {:closed true}
            [:id [:ref :openplanner.graph.recall.contract/identity]]
            [:event-id [:ref :openplanner.graph.recall.contract/identity]]
            [:model [:ref :openplanner.graph.recall.contract/identity]]
            [:dimensions [:int {:min 1 :max 8192}]]
            [:chunk [:int {:min 0 :max 4095}]] [:embedding [:ref ::embedding]]]
   ::projection [:map {:closed true}
                 [:version [:= 1]] [:org_id [:ref :openplanner.graph.recall.contract/identity]]
                 [:project [:ref :openplanner.graph.recall.contract/identity]]
                 [:character_id [:ref :openplanner.graph.recall.contract/identity]]
                 [:provenance_event_ids [:vector {:min 1 :max 64}
                                        [:ref :openplanner.graph.recall.contract/identity]]]
                 [:cost [:ref :openplanner.graph.recall.contract/cost]]]
   ::denied [:map {:closed true} [:status [:= :denied]] [:hits [:vector {:max 0} :any]]
             [:feedback [:ref :openplanner.graph.recall.contract/feedback]]]
   ::failure [:map {:closed true} [:stage [:= :graph]]
              [:code [:enum :invalid-request :invalid-authority :unsupported-capability
                      :transport-error :timeout :invalid-projection :storage-limit-exceeded
                      :invalid-embedding :authority-unavailable :authority-changed :embedding-unavailable]]]
   ::failed [:map {:closed true} [:status [:= :failed]] [:hits [:vector {:max 0} :any]]
             [:failure [:ref ::failure]]
             [:feedback [:ref :openplanner.graph.recall.contract/feedback]]]
   ::result [:map {:closed true}
             [:version [:= 1]] [:field-status [:= :not-loaded]]
             [:selection [:or [:ref :openplanner.graph.recall.contract/result]
                          [:ref ::denied] [:ref ::failed]]]]})

(defn validator
  "Validate the named adapter contract without granting access."
  [key]
  (m/validator [:ref key] {:registry (merge (m/default-schemas) recall/registry registry)}))

(def valid-authority? (validator ::authority))
(def valid-identity? (recall/validator :openplanner.graph.recall.contract/identity))
(def valid-embedding? (validator ::embedding))
(def valid-index? (validator ::index))
(def valid-projection? (validator ::projection))
(def valid-result? (validator ::result))

(defn failed
  "Return a bounded failure without credential-bearing exception text."
  [code]
  {:version 1 :field-status :not-loaded
   :selection {:status :failed :hits [] :failure {:stage :graph :code code}
               :feedback {:status :not-requested :attempted 0 :completed 0}}})

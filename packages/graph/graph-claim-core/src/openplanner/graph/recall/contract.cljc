(ns openplanner.graph.recall.contract
  "Versioned data contracts for graph recall over an admitted storage snapshot.
   LGPL-3.0-or-later. Decisions come from a trusted storage/identity adapter;
   a JSON request cannot supply them or confer authority."
  (:require [malli.core :as m]))

(def registry
  "EDN-serializable shapes; validation does not establish authorization."
  {::identity [:string {:min 1 :max 512}]
   ::number [:or :int :double]
   ::cost [:and [:ref ::number] [:>= 0] [:<= 1000000]]
   ::score [:and [:ref ::number] [:>= -1] [:<= 1]]
   ::scope [:map {:closed true}
            [:actor-id [:ref ::identity]] [:org-id [:ref ::identity]]
            [:membership-id [:ref ::identity]] [:user-id [:ref ::identity]]
            [:policy-revision [:ref ::identity]]]
   ::request [:map {:closed true}
              [:version [:= 1]] [:recall-id [:ref ::identity]]
              [:query [:string {:min 1 :max 4096}]]
              [:k [:int {:min 1 :max 12}]]
              [:fetch [:int {:min 1 :max 18}]]
              [:max-nodes [:int {:min 1 :max 64}]]
              [:max-cost [:ref ::cost]] [:feedback [:= :none]]]
   ::decision [:map {:closed true}
               [:node-id [:ref ::identity]] [:event-id [:ref ::identity]]
               [:policy-revision [:ref ::identity]] [:allowed? :boolean]]
   ::node [:map {:closed true}
           [:id [:ref ::identity]] [:event-id [:ref ::identity]]
           [:text [:string {:max 16384}]] [:seed? :boolean] [:score [:ref ::score]]
           [:members {:optional true} [:vector {:min 1 :max 64} [:ref ::identity]]]]
   ::edge [:map {:closed true}
           [:id [:ref ::identity]] [:source [:ref ::identity]] [:target [:ref ::identity]]
           [:cost [:ref ::cost]]
           [:provenance-node-ids [:vector {:min 1 :max 64} [:ref ::identity]]]]
   ::influence [:map {:closed true}
                [:id [:ref ::identity]] [:edge-id [:ref ::identity]]
                [:kind [:enum :force :trail :field]] [:delta [:ref ::score]]
                [:provenance-node-ids [:vector {:min 1 :max 64} [:ref ::identity]]]]
   ::snapshot [:map {:closed true}
               [:version [:= 1]] [:revision [:ref ::identity]]
               [:field-revision [:ref ::identity]] [:field-owner [:ref ::identity]]
               [:scope [:ref ::scope]] [:index-status [:enum :ready :pending]]
               [:nodes [:vector {:max 2048} [:ref ::node]]]
               [:edges [:vector {:max 4096} [:ref ::edge]]]
               [:influences [:vector {:max 4096} [:ref ::influence]]]]
   ::feedback [:map {:closed true} [:status [:= :not-requested]]
               [:attempted [:= 0]] [:completed [:= 0]]]
   ::hit [:map {:closed true}
          [:id [:ref ::identity]] [:event-id [:ref ::identity]] [:text :string] [:seed? :boolean]
          [:cost [:ref ::cost]] [:score [:ref ::score]]
          [:path [:vector {:min 1 :max 64} [:ref ::identity]]]
          [:path-edge-ids [:vector {:max 63} [:ref ::identity]]]
          [:reason [:enum :semantic-seed :graph-neighbor]]]
   ::budget [:map {:closed true}
             [:k [:int {:min 1 :max 12}]] [:fetch [:int {:min 1 :max 18}]]
             [:max-nodes [:int {:min 1 :max 64}]] [:max-cost [:ref ::cost]]]
   ::failure [:map {:closed true} [:stage [:= :graph]]
              [:code [:enum :invalid-request :invalid-snapshot :invalid-authority-decisions :invalid-result]]]
   ::failed-result [:map {:closed true} [:status [:= :failed]] [:hits [:vector {:max 0} :any]]
                    [:failure [:ref ::failure]] [:feedback [:ref ::feedback]]]
   ::selection-result [:map {:closed true}
                       [:status [:enum :completed :empty :denied :indexing-pending :budget-exhausted]]
                       [:hits [:vector {:max 12} [:ref ::hit]]]
                       [:recall-id [:ref ::identity]] [:graph-revision [:ref ::identity]]
                       [:field-revision [:ref ::identity]] [:field-owner [:ref ::identity]]
                       [:policy-revision [:ref ::identity]] [:budget [:ref ::budget]]
                       [:diagnostics [:map {:closed true}
                                      [:denied-nodes [:int {:min 0}]]
                                      [:authorized-nodes [:int {:min 0}]]
                                      [:admitted-edges [:int {:min 0}]]]]
                       [:visited-count {:optional true} [:int {:min 0 :max 64}]]
                       [:feedback [:ref ::feedback]]]
   ::result [:or [:ref ::failed-result] [:ref ::selection-result]]})

(defn validator
  "Compile a named schema without embedding functions in schema data."
  [schema-key]
  (m/validator [:ref schema-key] {:registry (merge (m/default-schemas) registry)}))

(def valid-request? (validator ::request))
(def valid-snapshot? (validator ::snapshot))
(def valid-decision? (validator ::decision))
(def valid-result? (validator ::result))

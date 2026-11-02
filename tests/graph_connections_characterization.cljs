;; Run with NODE_OPTIONS='--import tsx' nbb tests/graph_connections_characterization.cljs
;; End-to-end route characterization against the existing TypeScript implementation.
(ns graph-connections-characterization)

(def failures (atom 0))
(defn check! [label expected actual]
  (if (= expected actual)
    (println "PASS" label)
    (do (swap! failures inc) (println "FAIL" label "expected" (pr-str expected) "got" (pr-str actual)))))

(defn cursor [rows]
  (let [state (atom rows) c (js-obj)]
    (aset c "limit" (fn [n] (swap! state #(vec (take n %))) c))
    (aset c "sort" (fn [_] c))
    (aset c "toArray" (fn [] (js/Promise.resolve (clj->js @state))))
    c))

(def message-id "discord:g:c:m")
(def user-id "discord:user:42")
(def profile-id "bluesky:user:alice.example")
(def edges [{:source_node_id message-id :target_node_id user-id :edge_kind "authored_by" :data {:guild_id "g" :channel_id "c"}}
            {:source_node_id message-id :target_node_id profile-id :edge_kind "mentions_bluesky_user" :data {:handle "alice.example"}}])
;; Small specification model: induced edges are retained for all selected nodes,
;; and a message's mentioned profile belongs to its authored-by bridge.
(defn reference-connections [selected relations]
  (let [selected (set selected)
        visible (filter #(and (selected (:source_node_id %)) (selected (:target_node_id %))) relations)
        authored (into {} (for [e visible :when (= "authored_by" (:edge_kind e))]
                            [(:source_node_id e) (:target_node_id e)]))]
    {:edge-count (count visible)
     :profiles (vec (for [e visible :when (and (= "mentions_bluesky_user" (:edge_kind e))
                                               (= user-id (authored (:source_node_id e))))]
                      (get-in e [:data :handle])))}))
(def reference (reference-connections [message-id user-id profile-id] edges))
(def event {:id message-id :ts "2026-01-01T00:00:00Z" :extra {:guild_id "g" :channel_id "c"}})

(defn graph-test [fastify routes]
  (let [app (fastify)]
    (.decorate app "mongo" #js {:graphEdges #js {:find (fn [_] (cursor edges))}
                               :events #js {:find (fn [filter]
                                                   (let [ids (or (some-> filter .-id .-$in)
                                                                 (some-> (aget filter "extra.node_id") .-$in))]
                                                     (cursor (if (and ids (some #{message-id} (js->clj ids))) [event] []))))}})
    (-> (.register app (.-graphRoutes routes))
        (.then (fn []
                 (-> (.inject app (clj->js {:method "POST" :url "/graph/connections"
                                            :payload {:seed_node_ids [message-id user-id profile-id] :max_nodes 3}}))
                     (.then (fn [response]
                              (check! "connections status" 200 (.-statusCode response))
                              (check! "edges retained at node cap" (:edge-count reference) (count (get (js->clj (.json response) :keywordize-keys true) :edges))))))))
        (.then (fn []
                 (-> (.inject app (clj->js {:method "POST" :url "/graph/connections"
                                            :payload {:seed_node_ids [message-id user-id profile-id] :max_nodes 4}}))
                     (.then (fn [response]
                              (let [result (js->clj (.json response) :keywordize-keys true)]
                                (check! "Bluesky profile attributed via authorship" (:profiles reference)
                                        (get-in result [:stats :peopleBridges 0 :blueskyProfiles]))))))))
        (.finally #(.close app)))))

(defn event-test [fastify routes]
  (let [app (fastify) node-ops (atom []) edge-ops (atom [])]
    (.decorate app "mongo" #js {:events #js {:insertOne (fn [_] (js/Promise.resolve nil))
                                              :findOne (fn [& _] (js/Promise.resolve nil))
                                              :bulkWrite (fn [ops & _] (swap! node-ops into (js->clj ops :keywordize-keys true)) (js/Promise.resolve nil))}
                               :graphEdges #js {:bulkWrite (fn [ops & _] (swap! edge-ops into (js->clj ops :keywordize-keys true)) (js/Promise.resolve nil))}})
    (.decorate app "kafkaEvents" #js {:publishRawEvents (fn [& _] (js/Promise.resolve #js {:ok true}))})
    (-> (.register app (.-eventRoutes routes))
        (.then (fn [] (.inject app (clj->js {:method "POST" :url "/events" :payload {:events [{:schema "openplanner.event.v1" :id message-id :ts "2026-01-01T00:00:00Z" :source "discord" :kind "discord.message"
                                                                                                  :extra {:guild_id "g" :channel_id "c" :previous_message_id "prior" :topics ["Orbit"] :attachments [{:id "a" :filename "a.png"}]}}]}}))))
        (.then (fn [response]
                 (check! "event status" 200 (.-statusCode response))
                 (let [kinds (set (map #(get-in % [:updateOne :update :$set :edge_kind]) @edge-ops))]
                   (doseq [kind ["preceded_by" "mentions_topic" "has_attachment"]]
                     (check! (str "textless event " kind) true (contains? kinds kind))))))
        (.finally #(.close app)))))

(-> (js/Promise.all #js [(js/import "fastify") (js/import "../src/routes/v1/graph.ts") (js/import "../src/routes/v1/events.ts")])
    (.then (fn [modules]
             (let [fastify (.-default (aget modules 0))]
               (-> (graph-test fastify (aget modules 1))
                   (.then #(event-test fastify (aget modules 2)))))))
    (.then (fn [] (when (pos? @failures) (set! (.-exitCode js/process) 1))))
    (.catch (fn [err] (js/console.error err) (set! (.-exitCode js/process) 1))))

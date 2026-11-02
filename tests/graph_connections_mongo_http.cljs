;; Disposable MongoDB and real loopback HTTP characterization; Kafka is a stub.
;; Set OPENPLANNER_E2E_MONGO_URI to an isolated disposable server before running.
(ns graph-connections-mongo-http)

(def uri (.-OPENPLANNER_E2E_MONGO_URI js/process.env))
(def db-name (str "openplanner5_e2e_" (.replaceAll (str (random-uuid)) "-" "")))
(def failures (atom 0))
(defn check! [label expected actual]
  (if (= expected actual)
    (println "PASS" label)
    (do (swap! failures inc) (println "FAIL" label "expected" (pr-str expected) "got" (pr-str actual)))))
(def message-id "discord:g:c:m")
(def user-id "discord:user:42")
(def profile-id "bluesky:user:alice.example")
(def relations [{:source_node_id message-id :target_node_id user-id :edge_kind "authored_by" :data {:guild_id "g" :channel_id "c"}}
                {:source_node_id message-id :target_node_id profile-id :edge_kind "mentions_bluesky_user" :data {:handle "alice.example"}}])
(defn reference [selected edges]
  (let [ids (set selected) visible (filter #(and (ids (:source_node_id %)) (ids (:target_node_id %))) edges)
        authors (into {} (for [e visible :when (= "authored_by" (:edge_kind e))] [(:source_node_id e) (:target_node_id e)]))]
    {:edge-count (count visible)
     :profiles (vec (for [e visible :when (and (= "mentions_bluesky_user" (:edge_kind e)) (= user-id (authors (:source_node_id e))))] (get-in e [:data :handle])))}))
(def expected (reference [message-id user-id profile-id] relations))
(defn post [url body]
  (-> (js/fetch url #js {:method "POST" :headers #js {"content-type" "application/json"} :body (js/JSON.stringify (clj->js body))})
      (.then (fn [response] (-> (.json response) (.then (fn [body] {:status (.-status response) :body (js->clj body :keywordize-keys true)})))))))

(if-not (and uri (re-matches #"mongodb://127\.0\.0\.1:[0-9]+/?" uri))
  (do (js/console.error "OPENPLANNER_E2E_MONGO_URI must name a disposable loopback MongoDB server") (set! (.-exitCode js/process) 2))
  (-> (js/Promise.all #js [(js/import "fastify") (js/import "mongodb") (js/import "../src/routes/v1/graph.ts") (js/import "../src/routes/v1/events.ts")])
      (.then (fn [modules]
               (let [fastify (.-default (aget modules 0)) client (new (.-MongoClient (aget modules 1)) uri)
                     app (fastify) db (.db client db-name)
                     events (.collection db "events") edges (.collection db "graph_edges")]
                 (-> (.connect client)
                     (.then (fn [] (.insertOne events (clj->js {:id message-id :ts (js/Date. "2026-01-01T00:00:00Z") :extra {:guild_id "g" :channel_id "c"}}))))
                     (.then (fn [] (.insertMany edges (clj->js relations))))
                     (.then (fn []
                              (.decorate app "mongo" #js {:events events :graphEdges edges})
                              (.decorate app "kafkaEvents" #js {:publishRawEvents (fn [& _] (js/Promise.resolve #js {:ok true}))})
                              (.register app (.-graphRoutes (aget modules 2)))
                              (.register app (.-eventRoutes (aget modules 3)))
                              (.listen app #js {:host "127.0.0.1" :port 0})))
                     (.then (fn [base]
                              (-> (post (str base "/graph/connections") {:seed_node_ids [message-id user-id profile-id] :max_nodes 3})
                                  (.then (fn [result] (check! "connections HTTP status" 200 (:status result)) (check! "edges retained at node cap" (:edge-count expected) (count (get-in result [:body :edges])))))
                                  (.then (fn [] (post (str base "/graph/connections") {:seed_node_ids [message-id user-id profile-id] :max_nodes 4})))
                                  (.then (fn [result] (check! "Bluesky profile attributed via authorship" (:profiles expected) (get-in result [:body :stats :peopleBridges 0 :blueskyProfiles]))))
                                  (.then (fn [] (post (str base "/events") {:events [{:schema "openplanner.event.v1" :id "discord:g:c:textless" :ts "2026-01-01T00:01:00Z" :source "discord" :kind "discord.message" :extra {:guild_id "g" :channel_id "c" :previous_message_id "m" :topics ["Orbit"] :attachments [{:id "a" :filename "a.png"}]}}]})))
                                  (.then (fn [result] (check! "event HTTP status" 200 (:status result)) (.toArray (.find edges #js {:source_node_id "discord:g:c:textless"}))))
                                  (.then (fn [found] (let [kinds (set (map :edge_kind (js->clj found :keywordize-keys true)))] (doseq [kind ["preceded_by" "mentions_topic" "has_attachment"]] (check! (str "textless event " kind) true (contains? kinds kind)))))))))
                     (.finally (fn [] (-> (.close app) (.then #(.dropDatabase db)) (.then (fn [dropped] (println "Database dropped:" db-name dropped) (.close client))))))))))
      (.then (fn [] (when (pos? @failures) (set! (.-exitCode js/process) 1))))
      (.catch (fn [err] (js/console.error err) (set! (.-exitCode js/process) 2)))))

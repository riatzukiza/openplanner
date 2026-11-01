(ns openplanner.contract.auth-deferred-effect-diagnostic
  "A synthetic invalid-policy diagnostic, not a production auth-parity claim.

  Fastify may reject a header only during response serialization, after the
  synchronous adapter operation returned. The framework then owns the error."
  (:require ["fastify" :as fastify]
            ["node:assert/strict" :as assert]
            [openplanner.extern.js.fastify.auth :as auth]))

(defn invalid-policy [_]
  {:headers [["bad\nname" "value"]] :decision :next})

(defn ^:async run-diagnostic []
  (let [app (fastify/default #js {:logger false})]
    (try
      (let [_ (await (.register app (auth/auth-plugin invalid-policy) #js {}))
            _ (.get app "/private" (fn [_ reply] (.send reply #js {:ok true})))
            response (await (.inject app #js {:method "GET" :url "/private"}))
            payload (js/JSON.parse (.-body response))]
        (assert/equal (.-statusCode response) 500)
        (assert/equal (.-code payload) "ERR_INVALID_HTTP_TOKEN")
        (println "Fastify deferred header error diagnostic passed"))
      (finally (let [_ (await (.close app))] nil)))))

(run-diagnostic)

(ns openplanner.extern.js.fastify.auth-test
  "Normalize Fastify auth inputs without allowing raw host objects into domain."
  (:require [openplanner.extern.js.fastify.auth :as extern]))

(defn check! [condition message]
  (when-not condition (throw (ex-info message {:test message}))))

(defn check-error! [input expected]
  (let [result (extern/request-snapshot #js {} input)]
    (check! (= {:outcome :error :error expected} result) (str "normalized error " expected))
    (check! (not (.includes (pr-str result) "secret-getter")) "host error detail not exposed")))

(defn -main []
  (let [req #js {:method "GET" :url "/v1/health?x=1"
                 :headers #js {:origin "" :authorization "Bearer key"}}
        result (extern/request-snapshot #js {:apiKey "key"} req)
        normalized (:request result)]
    (check! (= :ok (:outcome result)) "success tagged")
    (check! (= #{:method :url :origin :authorization :api-key} (set (keys normalized))) "snapshot keys")
    (check! (= "GET" (:method normalized)) "method preserved")
    (check! (= "/v1/health?x=1" (:url normalized)) "raw url preserved")
    (check! (= {:kind :string :value ""} (:origin normalized)) "empty string preserved")
    (check! (= {:kind :string :value "Bearer key"} (:authorization normalized)) "header preserved")
    (check! (= {:kind :string :value "key"} (:api-key normalized)) "key preserved"))
  (let [req #js {:method "GET" :url "/" :headers #js {:origin nil :authorization false}}
        normalized (:request (extern/request-snapshot #js {} req))]
    (check! (= {:kind :null} (:origin normalized)) "null distinct")
    (check! (= {:kind :boolean :value false} (:authorization normalized)) "boolean distinct")
    (check! (= {:kind :undefined} (:api-key normalized)) "undefined distinct"))
  (let [req #js {:method "GET" :url "/" :headers #js {:origin #js ["opaque"] :authorization 8}}
        normalized (:request (extern/request-snapshot #js {:apiKey #js {}} req))]
    (check! (= {:kind :other} (:origin normalized)) "object not leaked")
    (check! (= {:kind :number :value 8} (:authorization normalized)) "number preserved")
    (check! (= {:kind :other} (:api-key normalized)) "object key not leaked"))
  (check-error! #js {:method #js {} :url "/" :headers #js {}} {:code :invalid-field :field :method})
  (check-error! #js {:method "GET" :url nil :headers #js {}} {:code :invalid-field :field :url})
  (check-error! #js {:method "GET" :url "/" :headers nil} {:code :invalid-field :field :headers})
  (check-error! #js {:method "GET" :url "/"} {:code :invalid-field :field :headers})
  (doseq [bad [(js/Symbol "headers") (js/BigInt 1) (fn [] nil) #js []]]
    (check-error! #js {:method "GET" :url "/" :headers bad}
                  {:code :invalid-field :field :headers}))
  (let [revocable (js/Proxy.revocable #js {} #js {})]
    ((.-revoke revocable))
    (check-error! #js {:method "GET" :url "/" :headers (.-proxy revocable)}
                  {:code :property-access :field :headers}))
  (check-error! nil {:code :property-access :field :method})
  (let [request #js {:url "/" :headers #js {}}]
    (js/Object.defineProperty request "method" #js {:get (fn [] (throw (js/Error. "secret-getter")))})
    (check-error! request {:code :property-access :field :method}))
  (let [headers #js {}
        request #js {:method "GET" :url "/" :headers headers}]
    (js/Object.defineProperty headers "authorization" #js {:get (fn [] (throw (js/Error. "secret-getter")))})
    (check-error! request {:code :property-access :field :authorization}))
  (let [request #js {:method "GET" :url "/" :headers (js/Object.create nil)}]
    (check! (= :ok (:outcome (extern/request-snapshot #js {} request)))
            "null-prototype headers accepted"))
  (let [config #js {}
        request #js {:method "GET" :url "/" :headers (js/Object.create nil)}]
    (js/Object.defineProperty config "apiKey" #js {:get (fn [] (throw (js/Error. "secret-getter")))})
    (let [result (extern/request-snapshot config request)]
      (check! (= {:outcome :error :error {:code :property-access :field :api-key}} result)
              "config getter failure normalized")
      (check! (not (.includes (pr-str result) "secret-getter")) "config host error not exposed")))
  (let [revocable (js/Proxy.revocable #js {} #js {})]
    ((.-revoke revocable))
    (check-error! #js {:method "GET" :url "/" :headers #js {:origin (.-proxy revocable)}}
                  {:code :property-access :field :origin}))
  ;; An invalid input is mapped at the host boundary, never passed to domain.
  (let [hook (atom nil)
        decoded (atom [])
        captured (atom nil)
        app #js {:addHook (fn [_name callback] (reset! hook callback))}
        plugin (extern/auth-plugin (fn [value] (swap! decoded conj value) {}))]
    (plugin app #js {} (fn [] nil))
    (@hook #js {:method nil :url "/" :headers #js {}} #js {} (fn [error] (reset! captured error)))
    (check! (empty? @decoded) "domain is not called on decode failure")
    (check! (instance? js/Error @captured) "error mapped to Fastify callback")
    (check! (= "Invalid Fastify auth request" (.-message @captured)) "stable non-sensitive host error"))
  (let [reply #js {}
        calls (atom 0)]
    (aset reply "header" (fn [_ _] #js {:opaque true}))
    (aset reply "status" (fn [_] reply))
    (aset reply "send" (fn [] #js {:opaque true}))
    (aset reply "unauthorized" (fn [_] #js {:opaque true}))
    (doseq [decision [:next :preflight :unauthorized]]
      (let [result (extern/apply-decision! reply (fn [] (swap! calls inc) #js {:opaque true})
                                           {:headers [["x-test" "value"]] :decision decision})]
        (check! (= {:outcome :ok} result) (str "normalized effect result " decision))))
    (check! (= 1 @calls) "continue callback called once"))
  (let [reply #js {:header (fn [_ _] (throw (js/Error. "secret-effect")))}
        result (extern/apply-decision! reply (fn [] nil)
                                       {:headers [["x-test" "value"]] :decision :next})]
    (check! (= {:outcome :error :error {:code :effect-failed}} result) "reply exception normalized")
    (check! (not (.includes (pr-str result) "secret-effect")) "reply error detail not exposed"))
  (let [result (extern/apply-decision! #js {} (fn [] (throw (js/Error. "secret-next")))
                                       {:headers [] :decision :next})]
    (check! (= {:outcome :error :error {:code :effect-failed}} result) "next exception normalized")
    (check! (not (.includes (pr-str result) "secret-next")) "callback error detail not exposed"))
  (let [hook (atom nil)
        app #js {:addHook (fn [_name callback] (reset! hook callback))}
        plugin (extern/auth-plugin (fn [_] {:headers [["x-test" "value"]] :decision :next}))
        request #js {:method "GET" :url "/" :headers #js {}}
        reply #js {:header (fn [_ _] (throw (js/Error. "secret-host")))}
        calls (atom 0)]
    (plugin app #js {} (fn [] nil))
    (let [failure (try (@hook request reply (fn [& _] (swap! calls inc))) nil
                       (catch :default error error))]
      (check! (instance? js/Error failure) "effect failure reaches Fastify as safe error")
      (check! (= "Fastify auth response failed" (.-message failure)) "stable safe effect error")
      (check! (= 0 @calls) "failed header does not call next twice")))
  (println "Fastify auth snapshot boundary passed"))

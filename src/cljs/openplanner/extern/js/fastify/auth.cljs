(ns openplanner.extern.js.fastify.auth
  "Narrow Fastify auth/CORS boundary: decode input, apply output, register hook."
  (:require ["fastify-plugin" :as fastify-plugin]
            ["lodash/isObjectLike.js" :as object-like]))

(defn- field-value [value]
  (cond
    (undefined? value) {:kind :undefined}
    (nil? value) {:kind :null}
    (string? value) {:kind :string :value value}
    (boolean? value) {:kind :boolean :value value}
    (number? value) {:kind :number :value value}
    :else {:kind :other}))

(defn- invalid-field [field]
  {:outcome :error :error {:code :invalid-field :field field}})

(defn- read-property [object property field]
  (try
    {:outcome :ok :value (aget object property)}
    (catch :default _
      {:outcome :error :error {:code :property-access :field field}})))

(defn- read-required-string [state field]
  (let [result (read-property (:host-request state) (name field) field)]
    (cond
      (= :error (:outcome result)) result
      (string? (:value result)) (assoc-in state [:request field] (:value result))
      :else (invalid-field field))))

(defn- read-method [state] (read-required-string state :method))
(defn- read-url [state] (read-required-string state :url))

(defn- header-container? [value]
  ;; Native typeof accepts Node null-prototype header maps without admitting
  ;; scalar values, functions or arrays. Revoked proxies are caught by advance.
  (and (object-like/default value) (not (js/Array.isArray value))))

(defn- read-headers [state]
  (let [result (read-property (:host-request state) "headers" :headers)]
    (cond
      (= :error (:outcome result)) result
      (header-container? (:value result)) (assoc state :host-headers (:value result))
      :else (invalid-field :headers))))

(defn- read-tagged [state object property field]
  (let [result (read-property object property field)]
    (if (= :error (:outcome result))
      result
      (assoc-in state [:request field] (field-value (:value result))))))

(defn- read-origin [state]
  (read-tagged state (:host-headers state) "origin" :origin))

(defn- read-authorization [state]
  (read-tagged state (:host-headers state) "authorization" :authorization))

(defn- read-api-key [state]
  (read-tagged state (:host-config state) "apiKey" :api-key))

(defn- advance [state [field step]]
  (try
    (let [result (step state)]
      (if (= :error (:outcome result)) (reduced result) result))
    (catch :default _
      (reduced {:outcome :error :error {:code :property-access :field field}}))))

(defn request-snapshot
  "Decode Fastify inputs into tagged Clojure data or a safe failure.

  Fastify normally supplies method/url strings and a headers object. Property
  getter errors are caught without exposing host exceptions or their messages."
  [config request]
  (let [result (reduce advance
                       {:outcome :ok :host-config config :host-request request :request {}}
                       [[:method read-method] [:url read-url] [:headers read-headers]
                        [:origin read-origin] [:authorization read-authorization] [:api-key read-api-key]])]
    (if (= :error (:outcome result))
      (select-keys result [:outcome :error])
      (select-keys result [:outcome :request]))))

(defn apply-decision!
  "Perform synchronous Fastify effects and return a normalized outcome.

  Failures deferred by Fastify until later response serialization remain under
  framework control; this outcome only describes the synchronous calls."
  [reply done {:keys [headers decision]}]
  (try
    (doseq [[header value] headers] (.header reply header value))
    (case decision
      :preflight (.send (.status reply 204))
      :next (done)
      :unauthorized (.unauthorized reply "Missing/invalid Authorization: Bearer <API_KEY>"))
    {:outcome :ok}
    (catch :default _
      {:outcome :error :error {:code :effect-failed}})))

(defn- signal-decode-error! [next]
  (try
    (next (js/Error. "Invalid Fastify auth request"))
    nil
    (catch :default _
      (throw (js/Error. "Fastify auth response failed")))))

(defn- signal-effect-error! []
  ;; Never re-invoke `next`: an error may have arisen after it was called.
  (throw (js/Error. "Fastify auth response failed")))

(defn auth-plugin
  "Build the Fastify callback plugin from a pure decision function."
  [decide]
  (fastify-plugin/default
   (fn [app config done]
     (.addHook app "onRequest"
               (fn [request reply next]
                 (let [decoded (request-snapshot config request)]
                   (if (= :ok (:outcome decoded))
                     (let [result (apply-decision! reply next (decide (:request decoded)))]
                       (if (= :ok (:outcome result)) nil (signal-effect-error!)))
                     (signal-decode-error! next)))))
     (done))))

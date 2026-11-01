(ns openplanner.domain.auth
  "Pure decisions for the characterized auth/CORS boundary.

  Tagged inputs distinguish strings from missing, null and other JS values.
  This preserves the reference boundary; it is not a new authorization policy."
  (:require [clojure.string :as string]))

(def ^:private public-paths #{"/v1/health" "/v1/metrics" "/"})

(defn- string-value [field]
  (when (= :string (:kind field)) (:value field)))

(defn decide-auth [{:keys [method url origin authorization api-key]}]
  (let [origin-value (string-value origin)
        header (string-value authorization)
        key-value (string-value api-key)
        token (when (and (some? header)
                         (string/starts-with? (string/lower-case header) "bearer "))
                (subs header 7))]
    {:headers [["Access-Control-Allow-Origin" (if (some? origin-value) origin-value "*")]
               ["Vary" "Origin"]
               ["Access-Control-Allow-Methods" "GET,POST,PUT,PATCH,DELETE,OPTIONS"]
               ["Access-Control-Allow-Headers" "Authorization,Content-Type,X-Requested-With"]
               ["Access-Control-Max-Age" "86400"]]
     :decision (cond
                 (= "OPTIONS" method) :preflight
                 (or (contains? public-paths url)
                     (string/starts-with? url "/v1/public/")) :next
                 (and (some? token) (not= "" token) (= token key-value)) :next
                 :else :unauthorized)}))

(ns openplanner.domain.auth-test
  "Pure authorization/CORS decisions, independent of host HTTP objects."
  (:require [openplanner.domain.auth :as auth]))

(defn check! [condition message]
  (when-not condition (throw (ex-info message {:test message}))))

(defn field [value] {:kind :string :value value})
(def missing {:kind :undefined})
(def base {:method "GET" :url "/v1/private" :origin missing
           :authorization missing :api-key (field "Synthetic-Key")})

(defn run-tests! []
  (doseq [[input decision]
          [[base :unauthorized]
           [(assoc base :method "OPTIONS") :preflight]
           [(assoc base :url "/v1/health") :next]
           [(assoc base :url "/v1/health?deep=true") :unauthorized]
           [(assoc base :url "/v1/public/garden?x=1") :next]
           [(assoc base :url "/v1/public") :unauthorized]
           [(assoc base :authorization (field "bEaReR Synthetic-Key")) :next]
           [(assoc base :authorization (field "Bearer  Synthetic-Key")) :unauthorized]
           [(assoc base :authorization (field "Bearer synthetic-key")) :unauthorized]
           [(assoc base :authorization (field "Bearer ")) :unauthorized]
           [(assoc base :authorization {:kind :boolean :value true}) :unauthorized]
           [(assoc base :authorization (field "Bearer Synthetic-Key") :api-key {:kind :other}) :unauthorized]]]
    (check! (= decision (:decision (auth/decide-auth input))) (str "decision for " input)))
  (check! (= [["Access-Control-Allow-Origin" "*"]
              ["Vary" "Origin"]
              ["Access-Control-Allow-Methods" "GET,POST,PUT,PATCH,DELETE,OPTIONS"]
              ["Access-Control-Allow-Headers" "Authorization,Content-Type,X-Requested-With"]
              ["Access-Control-Max-Age" "86400"]]
             (:headers (auth/decide-auth base))) "default headers and order")
  (check! (= "null" (second (first (:headers (auth/decide-auth (assoc base :origin (field "null")))))))
          "literal origin null is not absent")
  (check! (= "" (second (first (:headers (auth/decide-auth (assoc base :origin (field "")))))))
          "empty origin is still a string")
  true)

(defn -main [] (run-tests!) (println "Pure auth decisions passed"))

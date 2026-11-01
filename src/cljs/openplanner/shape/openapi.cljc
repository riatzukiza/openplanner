(ns openplanner.shape.openapi
  "Malli shape for the OpenAPI subset constructed by OpenPlanner.

  This is not a complete OpenAPI specification. Maps remain open so extension
  fields and existing wire data are retained; validation does not transform it."
  (:require [clojure.string :as str]))

(defn nonblank-string? [x]
  (and (string? x) (not (str/blank? x))))

(defn path-key? [x]
  (and (string? x) (str/starts-with? x "/")))

(defn nonempty-map? [x]
  (and (map? x) (not (empty? x))))

(def operation
  [:map ["responses" [:and [:map-of :string :map] [:fn nonempty-map?]]]])

(def http-methods ["get" "put" "post" "delete" "options" "head" "patch" "trace"])

(def path-item
  (into [:and
         [:map
          ["get" {:optional true} operation]
          ["put" {:optional true} operation]
          ["post" {:optional true} operation]
          ["delete" {:optional true} operation]
          ["options" {:optional true} operation]
          ["head" {:optional true} operation]
          ["patch" {:optional true} operation]
          ["trace" {:optional true} operation]]]
        [[:fn (fn [value] (and (map? value) (boolean (some #(contains? value %) http-methods))))]]))

(def document
  [:map
   ["openapi" [:fn nonblank-string?]]
   ["info" [:map ["title" [:fn nonblank-string?]]
                  ["version" [:fn nonblank-string?]]]]
   ["paths" [:and [:map-of [:fn path-key?] path-item] [:fn nonempty-map?]]]])

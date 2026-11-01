(ns openplanner.law.openapi
  "Validation of the explicitly supported document shape and local map refs.

  This is not full OpenAPI or JSON Pointer conformance; in particular local
  references through arrays are not supported. External references are left for
  a separate resolver. Validation returns the original document unchanged."
  (:require [clojure.string :as str]
            [malli.core :as m]
            [openplanner.shape.openapi :as shape]))

(defn- decode-pointer-segment [segment]
  (-> segment (str/replace "~1" "/") (str/replace "~0" "~")))

(defn- resolved-map-pointer? [document reference]
  (cond
    (= reference "#") true
    (not (str/starts-with? reference "#/")) false
    :else
    (let [segments (str/split (subs reference 2) #"/" -1)]
      (and (every? #(not (re-find #"~(?:[^01]|$)" %)) segments)
           (loop [node document, remaining segments]
             (if-let [segment (first remaining)]
               (let [key (decode-pointer-segment segment)]
                 (and (map? node)
                      (contains? node key)
                      (recur (get node key) (next remaining))))
               true))))))

(defn- reference-errors [document]
  (letfn [(walk [node path]
            (cond
              (map? node)
              (mapcat (fn [[key value]]
                        (let [p (conj path key)]
                          (if (= "$ref" key)
                            (cond
                              (not (string? value)) [{:in p :message "$ref must be a string"}]
                              (and (str/starts-with? value "#")
                                   (not (resolved-map-pointer? document value)))
                              [{:in p :message "unresolved or unsupported local map reference"}]
                              :else [])
                            (walk value p))))
                      node)
              (sequential? node) (mapcat (fn [[index value]] (walk value (conj path index)))
                                         (map-indexed vector node))
              :else []))]
    (vec (walk document []))))

(defn diagnose
  "Return a contract-tagged report; :in identifies the failing data path."
  [document]
  (let [shape-report (m/explain shape/document document)
        shape-errors (mapv #(select-keys % [:in :schema :value]) (:errors shape-report))
        errors (into shape-errors (reference-errors document))]
    {:contract :openplanner/openapi-subset
     :valid? (empty? errors)
     :errors errors}))

(defn checked!
  "Validate once and return the identical value; throw diagnostics on failure."
  [document]
  (let [report (diagnose document)]
    (if (:valid? report)
      document
      (throw (ex-info "Invalid OpenPlanner OpenAPI document" report)))))

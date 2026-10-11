(ns openplanner.stores.cache.core
  (:require [clojure.string :as str]))

(defn now-ms [] (.now js/Date))
(defn promise [v] (js/Promise.resolve v))
(defn pthen [v f] (.then (promise v) f))

(defn obj?
  [x]
  (and (some? x) (= "object" (goog/typeOf x)) (not (array? x))))

(defn jget
  [obj k]
  (when (obj? obj)
    (aget obj k)))

(defn nonblank
  [v]
  (let [s (some-> v str str/trim)]
    (when-not (str/blank? s) s)))

(defn opts-map
  [opts]
  (if (obj? opts) (js->clj opts :keywordize-keys true) (or opts {})))

(defn- ttl-option
  [opts fields fallback]
  (if-let [field (first (filter #(if (map? opts)
                                 (contains? opts %)
                                 (and (obj? opts) (js/Object.hasOwn opts (name %)))) fields))]
    (if (map? opts) (get opts field) (jget opts (name field)))
    fallback))

(defn- checked-ttl-ms
  [value]
  (when-not (and (number? value) (js/Number.isFinite value) (<= 0 value))
    (throw (js/Error. "Cache TTL must be a finite non-negative number")))
  value)

(defn ttl-ms
  [opts default-ttl-ms]
  (checked-ttl-ms (ttl-option opts [:ttlMs :ttl-ms] (or default-ttl-ms 0))))

(defn default-ttl-ms
  [opts]
  (checked-ttl-ms (ttl-option opts [:defaultTtlMs :default-ttl-ms] (* 5 60 60 1000))))

(ns openplanner.stores.cache.js-compat
  "JavaScript object boundary; preserve Closure semantics on Shadow CLJS.

  Nbb uses Lodash's documented native-typeof object-like predicate, excluding
  arrays exactly as the existing Node-side Closure predicate does."
  #?(:org.babashka/nbb (:require ["lodash/isObjectLike.js" :as object-like])))

(defn js-object? [value]
  #?(:org.babashka/nbb
     (and (object-like/default value) (not (js/Array.isArray value)))
     :cljs
     (and (some? value)
          (= "object" (goog/typeOf value))
          (not (array? value)))
     :clj
     (throw (ex-info "JavaScript object classification requires a JS host" {}))))

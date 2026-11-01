(ns openplanner.macro.openapi
  "Declaration syntax only; validation belongs to the separate law tests.")

(defmacro defopenapi
  "Define NAME from DOCUMENT without evaluating it more than once."
  [name document]
  (when-not (symbol? name)
    (throw (ex-info "defopenapi requires a symbol as its name" {:name name})))
  `(def ~name ~document))

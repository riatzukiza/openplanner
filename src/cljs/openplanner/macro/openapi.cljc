(ns openplanner.macro.openapi
  "Declaration syntax for checked construction, not a static type checker.")

(defmacro defopenapi
  "Define NAME from DOCUMENT, checking the supported law at construction."
  [name document]
  `(def ~name (openplanner.law.openapi/checked! ~document)))

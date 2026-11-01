(ns openplanner.law.schema-laws-test
  "Small host-independent structural laws over the shared schema data.

  These do not replace JSON Schema validation, Malli laws, static type checking,
  authorization, or end-to-end search behavior."
  (:require [openplanner.shape.api-schemas :as schemas]))

(defn- require-law! [condition message]
  (when-not condition
    (throw (ex-info message {:law message}))))

(defn run-laws! []
  (let [request schemas/vector-search-request
        properties (get request "properties")
        response schemas/vector-search-response
        query schemas/state-query
        redaction schemas/redaction-state-response
        redaction-checks (get-in redaction ["properties" "checks"])]
    (require-law! (= "object" (get request "type")) "vector request is an object")
    (require-law! (= ["q"] (get request "required")) "vector request requires q")
    (require-law! (= 1 (get-in properties ["q" "minLength"])) "q is nonempty")
    (require-law! (= [1 200 20] (mapv #(get-in properties ["k" %]) ["minimum" "maximum" "default"]))
                  "k bounds and default are stable")
    (require-law! (= ["hot" "compact" "both"] (get-in properties ["tier" "enum"])) "tier values are stable")
    (require-law! (= "both" (get-in properties ["tier" "default"])) "tier default is both")
    (require-law! (= false (get request "additionalProperties")) "request is structurally closed")
    (require-law! (= #{"ok" "result" "tier" "storageBackend"} (set (get response "required")))
                  "vector response required members are stable")
    (require-law! (= "boolean" (get-in query ["properties" "strict" "type"])) "strict is boolean")
    (require-law! (= false (get-in query ["properties" "strict" "default"])) "strict defaults false")
    (require-law! (= #{"vectorRedaction" "documentBacklog" "schemaVersions"} (set (get redaction-checks "required")))
                  "redaction response check members are stable")
    true))

(defn -main []
  (run-laws!)
  (println "Portable schema structural laws passed"))

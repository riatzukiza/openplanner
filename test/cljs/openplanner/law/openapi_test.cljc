(ns openplanner.law.openapi-test
  (:require #?(:clj [clojure.test :as t :refer [deftest is testing]]
               :cljs [cljs.test :as t :refer [deftest is testing]])
            [openplanner.law.openapi :as law]
            #?(:clj [openplanner.macro.openapi :refer [defopenapi]]
               :org.babashka/nbb [openplanner.macro.openapi :refer [defopenapi]]))
  #?(:cljs (:require-macros [openplanner.macro.openapi :refer [defopenapi]])))

(def valid-document
  {"openapi" "3.1.0"
   "info" {"title" "Example" "version" "1"}
   "paths" {"/example" {"get" {"responses" {"200" {"description" "ok"
                                                            "schema" {"$ref" "#/components/schemas/Item"}}}}}}
   "components" {"schemas" {"Item" {"type" "string"}}}})

(defopenapi macro-document valid-document)

(deftest construction-laws
  (testing "validation is non-transforming and the macro declares a value"
    (is (identical? valid-document (law/checked! valid-document)))
    (is (identical? valid-document macro-document))
    (is (:valid? (law/diagnose valid-document))))
  (testing "missing and invalid shape members report a violation"
    (doseq [value [(dissoc valid-document "info")
                   (assoc valid-document "paths" {})
                   (assoc valid-document "paths" 42)
                   (assoc-in valid-document ["paths" "/example"] 42)
                   (assoc-in valid-document ["paths" "/example" "get" "responses"] 42)
                   (assoc valid-document "paths" {"not-a-path" {"get" {"responses" {"200" {}}}}})
                   (assoc-in valid-document ["paths" "/example" "get" "responses"] {})]]
      (let [report (law/diagnose value)]
        (is (false? (:valid? report)))
        (is (seq (:errors report)))
        (is (= :openplanner/openapi-subset (:contract report))))))
  (testing "dangling and malformed refs carry their data path"
    (let [p ["paths" "/example" "get" "responses" "200" "schema" "$ref"]]
      (doseq [reference ["#/components/schemas/Missing" "#/components/~2bad" 42]]
        (let [report (law/diagnose (assoc-in valid-document p reference))]
          (is (false? (:valid? report)))
          (is (= p (:in (first (:errors report)))))))))
  (testing "pointer escapes are decoded"
    (let [p ["paths" "/example" "get" "responses" "200" "schema" "$ref"]
          doc (-> valid-document
                  (assoc-in ["components" "schemas" "a/b~c"] {"type" "string"})
                  (assoc-in p "#/components/schemas/a~1b~0c"))]
      (is (:valid? (law/diagnose doc))))))

(defn -main []
  #?(:clj (let [result (t/run-tests 'openplanner.law.openapi-test)]
            (when (pos? (+ (:fail result) (:error result)))
              (throw (ex-info "OpenAPI laws failed" result))))
     :cljs (t/run-tests 'openplanner.law.openapi-test)))

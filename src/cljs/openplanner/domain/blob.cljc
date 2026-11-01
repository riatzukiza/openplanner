(ns openplanner.domain.blob
  "Pure decisions and response data for the characterized blob routes.")

(defn data-dir
  "Select tagged configuration without exposing foreign objects to the domain."
  [configured environment]
  (if (= :missing (:kind configured))
    (if (= :missing (:kind environment))
      {:kind :value :value "./openplanner-lake"}
      environment)
    configured))

(defn valid-sha? [value]
  (and (string? value) (boolean (re-matches #"^[a-f0-9]{64}$" value))))

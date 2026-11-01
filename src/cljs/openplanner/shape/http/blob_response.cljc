(ns openplanner.shape.http.blob-response
  "Pure mapping from typed blob outcomes to the reference HTTP wire shape.")

(defn error-response [{:keys [status code message title] :as error}]
  (if (contains? error :wire-value)
    (:wire-value error)
    (cond-> (array-map :statusCode status)
      (some? code) (assoc :code code)
      true (assoc :error title :message message))))

(defn upload-response
  "Keep absent optional metadata absent rather than turning undefined into null."
  [metadata]
  (cond-> (array-map :ok true :sha256 (:sha256 metadata))
    (contains? metadata :mime) (assoc :mime (:mime metadata))
    (contains? metadata :name) (assoc :name (:name metadata))
    true (assoc :size (:size metadata))))

(defn upload-action [result]
  (case (:outcome result)
    :no-file {:action :reply :status 400 :body {:error "expected multipart field 'file'"}}
    :ok {:action :return :body (upload-response (:metadata result))}
    :error {:action :reply :status (get-in result [:error :status])
            :body (error-response (:error result))}
    {:action :invalid :detail result}))

(defn invalid-sha-action []
  {:action :reply :status 400 :body {:error "invalid sha256"}})

(defn download-action [result]
  (case (:outcome result)
    :sent {:action :sent}
    :unavailable {:action :reply :status 404 :body {:error "blob not found"}}
    {:action :invalid :detail result}))

(defn error-action [result]
  {:action :reply :status (get-in result [:error :status])
   :body (error-response (:error result))})

(ns openplanner.stores.document-hydration-test
  (:require [cljs.test :as t :refer [async deftest is run-tests]]
            [openplanner.stores.cache.core :as core]
            [openplanner.stores.document-hydration :as hydration]))

(defmethod t/report [:cljs.test/default :summary] [m]
  (println "\nRan" (:test m) "tests containing" (+ (:pass m) (:fail m) (:error m)) "assertions.")
  (println (:fail m) "failures," (:error m) "errors.")
  (when (pos? (+ (:fail m) (:error m)))
    (.exit js/process 1)))

(defn- p->
  [p f]
  (.then (js/Promise.resolve p) f))

(deftest document-source-ref-and-hydration-test
  (let [row #js {:id "doc-1"
                 :kind "code"
                 :project "devel"
                 :message "example"
                 :text ""
                 :extra #js {:source_path "org/example/core.cljs"
                             :migration_2 #js {:text_hash_sha256 "abc123"}}}]
    (is (true? (hydration/document-needs-hydration row)))
    (is (= "openplanner:source-ref:v2:[\"devel\",[\"hash\",\"abc123\"]]"
           (hydration/document-cache-key row)))
    (let [result (hydration/hydrate-document-row row "source text")
          hydrated-row (.-row result)
          doc (hydration/row-to-document hydrated-row)]
      (is (true? (.-hydrated result)))
      (is (= "source text" (.-text hydrated-row)))
      (is (= "source text" (.-content doc)))
      (is (= "code" (.-kind doc))))))

(deftest memory-lru-cache-ttl-and-eviction-test
  (let [clock (atom 1000)]
    (with-redefs [core/now-ms #(deref clock)]
      (let [cache (hydration/create-memory-lru-cache #js {:maxEntries 1 :defaultTtlMs 5})]
        (hydration/cache-put-js cache "a" "A")
        (is (= "A" (hydration/cache-get-js cache "a")))
        (hydration/cache-put-js cache "b" "B")
        (is (nil? (hydration/cache-get-js cache "a")))
        (is (= "B" (hydration/cache-get-js cache "b")))
        (reset! clock 1008)
        (is (nil? (hydration/cache-get-js cache "b")))))))

(deftest layered-cache-promotes-lower-layer-hit-test
  (async done
    (let [hot (hydration/create-memory-lru-cache #js {:maxEntries 2})
          warm (hydration/create-memory-lru-cache #js {:maxEntries 2})
          layered (hydration/create-layered-cache #js [hot warm])]
      (hydration/cache-put-js warm "k" "v")
      (-> (hydration/cache-get-js layered "k")
          (p-> (fn [value]
                 (is (= "v" value))
                 (is (= "v" (hydration/cache-get-js hot "k")))
                 (done)))
          (.catch (fn [err]
                    (is false (str "layered cache failed: " err))
                    (done)))))))

(deftest lmdb-cache-adapter-expires-and-touches-test
  (let [clock (atom 100)
        store (atom {})
        db #js {:get (fn [k] (get @store k))
                :put (fn [k v]
                       (swap! store assoc k v)
                       true)
                :remove (fn [k]
                          (let [present? (contains? @store k)]
                            (swap! store dissoc k)
                            present?))}
        cache (hydration/create-lmdb-cache #js {:db db :prefix "l:" :defaultTtlMs 5})]
    (with-redefs [core/now-ms #(deref clock)]
    (is (true? (hydration/cache-put-js cache "a" "A")))
    (is (= "A" (hydration/cache-get-js cache "a")))
    (is (true? (hydration/cache-touch-js cache "a" 20)))
    (is (= "A" (hydration/cache-get-js cache "a")))
    (reset! clock 125)
    (is (nil? (hydration/cache-get-js cache "a"))))))

(deftest date-timestamps-use-the-stable-iso-representation
  (let [iso "2026-01-02T03:04:05.006Z"]
    (is (= iso (.-ts (hydration/row-to-document #js {:id "date" :ts (js/Date. iso)}))))
    (is (= iso (.-ts (hydration/row-to-document #js {:id "string" :ts iso}))))))

(deftest hostname-only-documents-never-share-a-hydration-key
  (let [a #js {:id "a" :project "lake" :text "" :extra #js {:hostname "same.example"}}
        b #js {:id "b" :project "lake" :text "" :extra #js {:hostname "same.example"}}]
    (is (not= (hydration/document-cache-key a) (hydration/document-cache-key b)))
    (is (nil? (hydration/document-cache-key #js {:project "lake" :extra #js {:hostname "same.example"}})))))

(deftest source-reference-key-components-cannot-alias
  (doseq [field ["source_path" "url"]]
    (let [a #js {:id "a" :project "a" :extra (doto #js {} (aset field "b:c"))}
          b #js {:id "b" :project "a:b" :extra (doto #js {} (aset field "c"))}]
      (is (not= (hydration/document-cache-key a) (hydration/document-cache-key b)))))
  (let [a #js {:project "lake" :extra #js {:source_path "same"}}
        b #js {:project "lake" :extra #js {:url "same"}}]
    (is (not= (hydration/document-cache-key a) (hydration/document-cache-key b)))))

(deftest mongo-document-identities-retain-storage-id-fallback
  (let [a (hydration/row-to-document #js {:_id "mongo-a"})
        b (hydration/row-to-document #js {:_id "mongo-b"})
        explicit (hydration/row-to-document #js {:id "public" :_id "storage"})]
    (is (= "mongo-a" (.-id a)))
    (is (= "mongo-b" (.-id b)))
    (is (= "public" (.-id explicit)))))


(deftest document-reference-identities-never-coerce-runtime-values
  (doseq [field ["content_hash" "source_path" "path" "url" "hostname" "lake"]
          value [#js {} #js []]]
    (let [extra #js {:source_path "/owned/source"} row #js {:id "owned" :text "" :extra extra}]
      (aset extra field value)
      (is (thrown-with-msg? js/Error #"identity.*string|string.*identity"
            (hydration/document-cache-key row)))
      (is (thrown-with-msg? js/Error #"identity.*string|string.*identity"
            (hydration/document-source-ref row))))))

(deftest document-source-path-aliases-preserve-provenance
  (doseq [extra [#js {:source_path "direct/path"}
                 #js {:path "direct/path"}
                 #js {:metadata #js {:path "direct/path"}}
                 #js {:metadata #js {:file_id "direct/path"}}]
          encoded? [false true]]
    (let [row #js {:id "owned-source" :project "lake" :text ""
                   :extra (if encoded? (js/JSON.stringify extra) extra)}
          ^js ref (hydration/document-source-ref row)
          hydrated (.-row (hydration/hydrate-document-row row "outside information"))]
      (is (= "direct/path" (.-sourcePath ref)))
      (is (= "direct/path" (.-sourcePath (hydration/row-to-document hydrated)))))))


(deftest bson-storage-identities-do-not-shadow-usable-source-identities
  (doseq [row [#js {:id "public" :_id #js {} :project "lake" :extra #js {:source_path "encounter.md"}}
              #js {:id #js {} :_id #js {} :project "lake" :extra #js {:source_path "encounter.md"}}
              #js {:id #js {} :_id "storage" :project "lake" :extra #js {:hostname "outside.example"}}
              #js {:id "   " :_id "storage" :project "lake" :extra #js {:hostname "outside.example"}}]]
    (let [result (try {:ref (hydration/document-source-ref row)}
                     (catch :default error {:error error}))]
      (is (nil? (:error result)) "a storage ObjectId must not reject a usable source")
      (when-not (:error result)
        (let [^js ref (:ref result)]
          (is (some? ref))
          (is (true? (hydration/document-needs-hydration row)))
          (is (= "outside" (.-text (.-row (hydration/hydrate-document-row row "outside")))))
          (is (= (if (and ref (.-sourcePath ref))
                   "openplanner:source-ref:v2:[\"lake\",[\"path\",\"encounter.md\"]]"
                   "openplanner:source-ref:v2:[\"lake\",[\"host-row\",\"outside.example\",\"storage\"]]")
                 (hydration/document-cache-key row)))))))
  (is (= {:ok nil}
         (try {:ok (hydration/document-cache-key #js {:id #js {} :_id #js {} :extra #js {:hostname "outside.example"}})}
              (catch :default error {:error error}))))
  (is (= "openplanner:source-ref:v2:[null,[\"host-row\",\"outside.example\",\"public\"]]"
         (hydration/document-cache-key #js {:id "public" :_id "storage" :extra #js {:hostname "outside.example"}}))))


(deftest migration-source-path-preserves-hydration-provenance
  (doseq [encoded? [false true]]
    (let [extra #js {:migration_2 #js {:source_path "legacy/path"}}
          row #js {:id "legacy" :project "lake" :text "" :extra (if encoded? (js/JSON.stringify extra) extra)}
          ^js ref (hydration/document-source-ref row)]
      (is (some? ref))
      (is (true? (hydration/document-needs-hydration row)))
      (is (= "legacy/path" (when ref (.-sourcePath ref))))
      (is (= "legacy/path" (.-sourcePath (hydration/row-to-document row))))))
  (doseq [value [#js {} #js [] 42]]
    (is (thrown-with-msg? js/Error #"identity.*string|string.*identity"
          (hydration/document-source-ref #js {:extra #js {:source_path "direct" :migration_2 #js {:source_path value}}})))))

(defn -main []
  (run-tests 'openplanner.stores.document-hydration-test))

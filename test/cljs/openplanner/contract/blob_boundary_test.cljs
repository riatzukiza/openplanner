(ns openplanner.contract.blob-boundary-test
  "Tests Clojure data returned by blob host operations, not full HTTP parity."
  (:require ["node:assert/strict" :as assert]
            ["node:fs" :as fs]
            ["node:os" :as os]
            ["node:path" :as path]
            [openplanner.domain.blob :as blob]
            [openplanner.extern.js.blob-io :as io]
            [openplanner.shape.http.blob-response :as response]
            [openplanner.support.blob-cleanup :as cleanup]))

(defn portable-data? [value]
  (cond
    (undefined? value) false
    (map? value) (every? (fn [[k v]] (and (portable-data? k) (portable-data? v))) value)
    (vector? value) (every? portable-data? value)
    :else (or (nil? value) (keyword? value) (string? value)
              (number? value) (boolean? value))))

(defn close-rejected [_error] :rejected)
(defn close-unexpected [_value] :unexpected)

(defn ^:async cleanup-failure-test []
  (let [dir (fs/mkdtempSync (path/join (os/tmpdir) "openplanner-blob-cleanup-"))
        app #js {:close (fn [] (js/Promise.reject (js/Error. "synthetic close failure")))}
        result (await (.then (cleanup/close-and-remove! app dir) close-unexpected close-rejected))]
    (assert/ok (= result :rejected))
    (assert/equal (fs/existsSync dir) false "cleanup remains effective when close fails")
    true))

(defn ^:async run-file-cases [dir contents request no-file invalid-file reply captured]
  (let [path-result (io/blobs-dir #js {} {:kind :value :value dir})
        _ (assert/ok (= (:outcome path-result) :ok))
        base (:path path-result)
        ensured (await (io/ensure-dir! base))
        missing-upload (await (io/store-upload! no-file base))
        invalid-upload (await (io/store-upload! invalid-file base))
        outcome (await (io/store-upload! request base))
        result (:metadata outcome)
        sha (:sha256 result)
        file-result (io/blob-path base sha)
        file (:path file-result)
        invalid-path (io/blob-path #js {:bad true} [])
        _ (assert/ok (string? base) "path function returns a CLJ string")
        _ (assert/ok (= ensured {:outcome :ok}))
        _ (assert/ok (= missing-upload {:outcome :no-file}))
        _ (assert/ok (= invalid-upload {:outcome :invalid-metadata :fields [:mime]}))
        _ (assert/ok (= (:outcome outcome) :ok))
        _ (assert/ok (map? result) "upload result is a Clojure map")
        _ (assert/ok (string? sha))
        _ (assert/equal (:size result) 5)
        _ (assert/ok (contains? result :mime) "explicit null metadata remains present")
        _ (assert/equal (:mime result) nil)
        _ (assert/equal (contains? result :name) false "undefined metadata remains absent")
        _ (assert/deepEqual (fs/readFileSync file) contents)
        sent (await (io/send-file! reply file))
        missing (await (io/send-file! reply (str file ".missing")))
        directory (await (io/send-file! reply base))
        wire (response/upload-response result)
        not-directory (path/join dir "not-directory")
        _ (fs/writeFileSync not-directory "x")
        mkdir-error (await (io/ensure-dir! (path/join not-directory "child")))
        rejected (doto (js/Error. "synthetic multipart rejection")
                   (aset "code" "SYNTHETIC_MULTIPART")
                   (aset "statusCode" 406))
        rejected-request #js {:file (fn [] (js/Promise.reject rejected))}
        rejected-result (await (io/store-upload! rejected-request base))
        revocable (js/Proxy.revocable #js {} #js {})
        proxy (.-proxy revocable)
        _ (.revoke revocable)
        hostile-error #js {:code proxy :message proxy :statusCode proxy}
        hostile-request #js {:file (fn [] (js/Promise.reject hostile-error))}
        hostile-result (await (io/store-upload! hostile-request base))
        rejected-proxy #js {:file (fn [] (js/Promise.reject proxy))}
        proxy-result (await (io/store-upload! rejected-proxy base))
        hostile-reply #js {:header (fn [_name _value] (throw proxy))}
        hostile-send (await (io/send-file! hostile-reply file))
        getter-error (js/Error. "synthetic guarded getter")
        _ (js/Object.defineProperty getter-error "code" #js {:get (fn [] (throw proxy))})
        getter-reply #js {:header (fn [_name _value] (throw getter-error))}
        getter-send (await (io/send-file! getter-reply file))
        getter-header-reply #js {}
        _ (js/Object.defineProperty getter-header-reply "header" #js {:get (fn [] (throw proxy))})
        getter-header-send (await (io/send-file! getter-header-reply file))
        getter-request #js {:file (fn [] (js/Promise.reject getter-error))}
        getter-result (await (io/store-upload! getter-request base))
        getter-file-request #js {}
        _ (js/Object.defineProperty getter-file-request "file" #js {:get (fn [] (throw proxy))})
        getter-file-result (await (io/store-upload! getter-file-request base))
        getter-part #js {:toBuffer (fn [] (js/Promise.resolve contents))}
        _ (js/Object.defineProperty getter-part "mimetype" #js {:get (fn [] (throw proxy))})
        getter-part-request #js {:file (fn [] (js/Promise.resolve getter-part))}
        getter-part-result (await (io/store-upload! getter-part-request base))
        getter-buffer-part #js {}
        _ (js/Object.defineProperty getter-buffer-part "toBuffer" #js {:get (fn [] (throw proxy))})
        getter-buffer-request #js {:file (fn [] (js/Promise.resolve getter-buffer-part))}
        getter-buffer-result (await (io/store-upload! getter-buffer-request base))
        bad-bytes #js {:file (fn [] (js/Promise.resolve
                                     #js {:toBuffer (fn [] (js/Promise.resolve #js {:notBytes true}))}))}
        hash-error (await (io/store-upload! bad-bytes base))
        buffer-rejection #js {:file (fn [] (js/Promise.resolve
                                            #js {:toBuffer (fn [] (js/Promise.reject (js/Error. "synthetic buffer error")))}))}
        buffer-error (await (io/store-upload! buffer-rejection base))]
    (assert/ok (every? portable-data? [path-result ensured missing-upload invalid-upload outcome file-result invalid-path
                                      sent missing directory wire mkdir-error rejected-result hostile-result proxy-result
                                      hostile-send getter-send getter-header-send getter-result getter-file-result getter-part-result
                                      getter-buffer-result hash-error buffer-error])
               "extern outcomes contain portable Clojure data, not host handles")
    (assert/ok (= sent {:outcome :sent}))
    (assert/ok (= (:outcome file-result) :ok))
    (assert/ok (= (:outcome invalid-path) :error))
    (assert/deepEqual @captured contents)
    (assert/ok (= missing {:outcome :unavailable :reason :missing}))
    (assert/ok (= directory {:outcome :unavailable :reason :directory}))
    (assert/ok (= (:outcome mkdir-error) :error))
    (assert/equal (get-in mkdir-error [:error :code]) "ENOTDIR")
    (assert/ok (= (:outcome rejected-result) :error))
    (assert/equal (get-in rejected-result [:error :code]) "SYNTHETIC_MULTIPART")
    (assert/equal (get-in rejected-result [:error :status]) 406)
    (assert/ok (= (:outcome hostile-result) :error))
    (assert/equal (get-in hostile-result [:error :status]) 500)
    (assert/equal (get-in hostile-result [:error :code]) nil)
    (assert/equal (get-in hostile-result [:error :message]) "Internal Server Error")
    (assert/ok (= (:outcome proxy-result) :error))
    (assert/equal (get-in proxy-result [:error :status]) 500)
    (assert/ok (= hostile-send {:outcome :unavailable :reason :other}))
    (assert/ok (= getter-send {:outcome :unavailable :reason :other}))
    (assert/ok (= getter-header-send {:outcome :unavailable :reason :other}))
    (assert/ok (= (:outcome getter-result) :error))
    (assert/equal (get-in getter-result [:error :code]) nil)
    (assert/ok (= (:outcome getter-file-result) :error))
    (assert/ok (= getter-part-result {:outcome :invalid-metadata :fields [:metadata]}))
    (assert/ok (= (:outcome getter-buffer-result) :error))
    (assert/ok (= (:outcome hash-error) :error) "crypto update failure remains tagged")
    (assert/ok (= (:outcome buffer-error) :error) "nested async rejection remains tagged")
    (assert/ok (map? wire))
    (assert/equal (:ok wire) true)
    (assert/equal (contains? wire :name) false)
    true))

(defn ^:async run-decode-tests []
  (let [hostile-config #js {}
        hostile-params #js {}
        revocable (js/Proxy.revocable #js {} #js {})
        revoked (.-proxy revocable)
        _ (.revoke revocable)
        revoked-config #js {}
        revoked-params #js {}
        valid-getter #js {}
        _ (js/Object.defineProperty hostile-config "dataDir"
                                    #js {:get (fn [] (throw (js/Error. "private config getter")))})
        _ (js/Object.defineProperty hostile-params "sha256"
                                    #js {:get (fn [] (throw (js/Error. "private params getter")))})
        _ (js/Object.defineProperty revoked-config "dataDir" #js {:get (fn [] (throw revoked))})
        _ (js/Object.defineProperty revoked-params "sha256" #js {:get (fn [] (throw revoked))})
        _ (js/Object.defineProperty valid-getter "dataDir" #js {:get (fn [] "from-getter")})
        missing (await (io/configured-data-dir #js {}))
        null-value (await (io/configured-data-dir #js {:openplannerConfig #js {:dataDir nil}}))
        undefined-value (await (io/configured-data-dir #js {:openplannerConfig #js {:dataDir js/undefined}}))
        empty-value (await (io/configured-data-dir #js {:openplannerConfig #js {:dataDir ""}}))
        false-app #js {:openplannerConfig #js {:dataDir false}}
        false-value (await (io/configured-data-dir false-app))
        foreign (await (io/configured-data-dir #js {:openplannerConfig #js {:dataDir #js {:opaque "x"}}}))
        valid (await (io/configured-data-dir #js {:openplannerConfig valid-getter}))
        sha (await (io/request-sha #js {:params #js {:sha256 "abc"}}))
        sha-object (await (io/request-sha #js {:params #js {:sha256 #js {:opaque true}}}))
        sha-missing (await (io/request-sha #js {:params #js {}}))
        hostile-cfg (await (io/configured-data-dir #js {:openplannerConfig hostile-config}))
        hostile-sha (await (io/request-sha #js {:params hostile-params}))
        revoked-cfg (await (io/configured-data-dir #js {:openplannerConfig revoked-config}))
        revoked-sha (await (io/request-sha #js {:params revoked-params}))
        revoked-app (await (io/configured-data-dir revoked))
        revoked-request (await (io/request-sha revoked))
        invalid (io/blobs-dir false-app false-value)]
    (assert/ok (every? portable-data? [missing null-value undefined-value empty-value false-value foreign valid
                                      sha sha-object sha-missing hostile-cfg hostile-sha revoked-cfg revoked-sha
                                      revoked-app revoked-request invalid]))
    (doseq [v [missing null-value undefined-value sha-missing]] (assert/ok (= v {:kind :missing})))
    (assert/ok (= empty-value {:kind :value :value ""}))
    (assert/ok (= (:kind false-value) :invalid))
    (assert/ok (= (:kind foreign) :invalid))
    (assert/ok (= valid {:kind :value :value "from-getter"}))
    (assert/ok (= sha {:kind :value :value "abc"}))
    (assert/ok (= sha-object {:kind :invalid}))
    (doseq [v [hostile-cfg hostile-sha revoked-cfg revoked-sha revoked-app revoked-request]]
      (assert/ok (= v {:kind :error})))
    (assert/ok (= (:outcome invalid) :error))
    (assert/equal (get-in invalid [:error :code]) "ERR_INVALID_ARG_TYPE")
    (assert/ok (string? (get-in invalid [:error :message])))
    true))

(defn ^:async run-tests []
  (let [dir (fs/mkdtempSync (path/join (os/tmpdir) "openplanner-blob-boundary-"))
        contents (js/Buffer.from #js [0 8 127 128 255])
        request #js {:file (fn [] (js/Promise.resolve
                                   #js {:mimetype nil
                                        :filename js/undefined
                                        :toBuffer (fn [] (js/Promise.resolve contents))}))}
        no-file #js {:file (fn [] (js/Promise.resolve nil))}
        invalid-file #js {:file (fn [] (js/Promise.resolve
                                        #js {:mimetype #js {:foreign true}
                                             :filename "valid.bin"
                                             :toBuffer (fn [] (js/Promise.resolve contents))}))}
        captured (atom nil)
        reply #js {:header (fn [_name _value] nil)
                   :send (fn [bytes] (reset! captured (js/Buffer.from bytes)))}]
    (try
      (assert/ok (= (blob/data-dir {:kind :missing} {:kind :missing})
                    {:kind :value :value "./openplanner-lake"}))
      (assert/ok (= (blob/data-dir {:kind :missing} {:kind :value :value "environment"})
                    {:kind :value :value "environment"}))
      (assert/ok (= (blob/data-dir {:kind :value :value ""} {:kind :value :value "environment"})
                    {:kind :value :value ""}))
      (assert/ok (= (blob/data-dir {:kind :invalid} {:kind :value :value "environment"})
                    {:kind :invalid}))
      (assert/equal (blob/valid-sha? (.repeat "a" 64)) true)
      (assert/equal (blob/valid-sha? (.repeat "A" 64)) false)
      (assert/ok (= (response/upload-action {:outcome :no-file})
                    {:action :reply :status 400 :body {:error "expected multipart field 'file'"}}))
      (assert/ok (= (response/download-action {:outcome :unavailable :reason :missing})
                    {:action :reply :status 404 :body {:error "blob not found"}}))
      (assert/ok (= (response/download-action {:outcome :sent}) {:action :sent}))
      (assert/ok (= (response/invalid-sha-action) {:action :reply :status 400 :body {:error "invalid sha256"}}))
      (assert/ok (= (:action (response/upload-action {:outcome :invalid-metadata :fields [:mime]}))
                    :invalid))
      (let [_ (await (run-decode-tests))
            _ (await (run-file-cases dir contents request no-file invalid-file reply captured))
            _ (await (cleanup-failure-test))]
        (println "Blob operation boundary tests passed"))
      (finally (fs/rmSync dir #js {:recursive true :force true})))))

(run-tests)

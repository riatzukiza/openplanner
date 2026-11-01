(ns openplanner.extern.js.closure-sha
  "Node-side candidate for Closure's legacy SHA-256 string-input behavior.

  Closure's Uint8Array path hashes the low byte of each UTF-16 code unit. This
  is intentionally NOT UTF-8 and must not silently change existing claim IDs.
  Keep this adapter separate until checked against the exact shadow dependency."
  (:require ["node:crypto" :as crypto]))

(defn sha256-string
  "Return a CLJS outcome map; never expose Node buffers or host exceptions."
  [text]
  (if-not (try (string? text)
               (catch :default _ false))
    {:outcome :error :error :invalid-input}
    (try
      (let [hasher (.createHash crypto "sha256")]
        (.update hasher (js/Buffer.from text "latin1"))
        {:outcome :ok :value (.digest hasher "hex")})
      (catch :default _
        {:outcome :error :error :digest-failed}))))

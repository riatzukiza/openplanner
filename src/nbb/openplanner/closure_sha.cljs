(ns openplanner.closure-sha
  "Node-side candidate for Closure's legacy SHA-256 string-input behavior.

  Closure's Uint8Array path hashes the low byte of each UTF-16 code unit. This
  is intentionally NOT UTF-8 and must not silently change existing claim IDs.
  Keep this adapter separate until checked against the exact shadow dependency."
  (:require ["node:crypto" :as crypto]))

(defn sha256-string [text]
  (let [hasher (crypto/createHash "sha256")]
    (.update hasher (js/Buffer.from text "latin1"))
    (.digest hasher "hex")))

#js {:sha256String sha256-string}

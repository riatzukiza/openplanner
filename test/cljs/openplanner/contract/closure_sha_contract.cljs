(ns openplanner.contract.closure-sha-contract
  "Characterization of the legacy Closure string-hash boundary.

  Reference vectors were captured with official google-closure-library npm
  20230802.0.0 on Node's Uint8Array path. This is not a claim about the exact
  shadow-resolved 2025 fork until that build is available for comparison."
  (:require ["node:assert/strict" :as assert]
            ["node:module" :as node-module]
            ["node:path" :as path]
            ["node:url" :as url]
            ["nbb" :as nbb]))

(def vectors
  [["empty" "" "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"]
   ["ascii" "abc" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"]
   ["latin1" "é" "de2e331d891ae267a7009cb45b4e8830f170e0c937288ea2731a1941c7a53b0d"]
   ["bmp" "中" "3973e022e93220f9212c18d0d0c543ae7c309e46640da93a4a0314de999f5112"]
   ["surrogate pair" "💥" "aaad3461686e68a093c8f13eef64161ec51d48ca7df04495de64daeb0a3f15bb"]
   ["embedded nul" (str "a" (js/String.fromCharCode 0) "b") "59b271ae1bbcb1d31d41929817f4b16fb439eb4f31520b5ad1d5ce98920a7138"]
   ["55 code units" (.repeat "é" 55) "4360c08f65b8587805be341bb5578175f084778be8516ca7d454bb777c392817"]
   ["56 code units" (.repeat "é" 56) "122c3722de62b26d87b9c9052858242407aa9bf046f765b4f068b91aac77810d"]
   ["63 code units" (.repeat "中" 63) "eaefc1b3cc3ddabef4e23608e5b80bd65ab201221db71c1c65e7719de8031ce9"]
   ["64 code units" (.repeat "中" 64) "d164134b9d5bc18eca918fc3d3d06df2f64b6417d99a5761b0d0e3e594d8d254"]
   ["66 code units" (.repeat "💥" 33) "34156c7c8ff51458ee44eaf5dd49f8323bde92b37e340f6b94fea08df52fdc1d"]
   ["lone high surrogate" (js/String.fromCharCode 0xd800) "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d"]
   ["lone low surrogate" (js/String.fromCharCode 0xdc00) "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d"]])

(defn load-reference [directory]
  (let [node-require (node-module/createRequire (str (url/pathToFileURL (path/resolve "package.json"))))
        version (.-version (node-require (path/resolve directory "package.json")))]
    (assert/equal version "20230802.0.0" "optional reference must be the characterized version")
    (node-require (path/resolve directory "closure/goog/bootstrap/nodejs.js"))
    (let [goog (.-goog js/globalThis)]
      (.require goog "goog.crypt.Sha256")
      (.require goog "goog.crypt")
      (fn [text]
        (let [sha (new (.. goog -crypt -Sha256))]
          (.update sha text)
          (.byteArrayToHex (.-crypt goog) (.digest sha)))))))

(defn ^:async run-contract []
  (let [reference (.-OPENPLANNER_CLOSURE_REFERENCE js/process.env)
        module (await (nbb/loadFile (path/resolve "src/cljs/openplanner/extern/js/closure_sha.cljs")))
        candidate (.-sha256String module)
        digest (if reference (load-reference reference) candidate)]
    (doseq [[label input expected] vectors]
      (assert/equal (candidate input) expected (str "candidate " label))
      (when reference (assert/equal (digest input) expected (str "reference " label))))
    (when reference
      ;; Deterministic UTF-16 units include unpaired surrogates and mixed input
      ;; across multiple SHA blocks. The official reference is optional and
      ;; separately identified because this environment lacks the 2025 fork.
      (loop [sample 0 state 1234567]
        (when (< sample 256)
          (let [n (mod (* sample 37) 193)
                [text next-state]
                (loop [i 0 seed state result ""]
                  (if (= i n)
                    [result seed]
                    (let [next-seed (mod (+ (* seed 1664525) 1013904223) 4294967296)]
                      (recur (inc i) next-seed
                             (str result (js/String.fromCharCode (mod next-seed 65536)))))))]
            (assert/equal (candidate text) (digest text) (str "differential sample " sample))
            (recur (inc sample) next-state)))))
    (println "Closure string-hash vectors passed against" (if reference reference "nbb adapter"))))

(run-contract)

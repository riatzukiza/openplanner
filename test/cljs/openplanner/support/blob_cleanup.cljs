(ns openplanner.support.blob-cleanup
  "Close a test app while always removing its isolated temporary directory."
  (:require ["node:fs" :as fs]))

(defn ^:async close-and-remove! [app dir]
  (try
    (let [_ (await (.close app))] true)
    (finally (fs/rmSync dir #js {:recursive true :force true}))))

(ns humandb.out
  (:require
    [clojure.string :as string]
    [yaml.core :as yaml]))

(defn replace
  "Replaces doc with the given doc at the specified location in body.
  Expects newline at end-of-body"
  [body location doc]
  (-> body
      (string/split #"---\n")
      (->> (remove string/blank?))
      vec
      (assoc-in location
                (yaml/generate-string doc :dumper-options {:flow-style :block}))
      (->> (string/join "---\n"))
      (->> (str "---\n"))
      (str "---\n")))

(ns humandb.out
  (:require
    [clojure.string :as string]
    [yaml.core :as yaml]))

(defn replace
  "Replaces doc with the given doc at the specified location in body.
  Expects newline at end-of-body"
  [body index doc]
  (-> body
      (string/split #"---\n")
      (->> (remove string/blank?))
      vec
      (assoc-in [index]
                (yaml/generate-string doc :dumper-options {:flow-style :block}))
      (->> (string/join "---\n"))
      (->> (str "---\n"))
      (str "---\n")))

(defn replace!
  [[file-path index] doc]
  (let [body (slurp file-path)
        new-body (replace body index doc)]
    (spit file-path new-body)))

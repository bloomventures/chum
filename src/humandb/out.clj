(ns humandb.out
  (:require
    [clojure.string :as string]
    [yaml.core :as yaml]
    [me.raynes.fs :as fs]))

(defn doc->yaml [doc]
  (yaml/generate-string doc :dumper-options {:flow-style :block}))

(defn replace-doc-in-stream
  "Replaces a doc at the specified location in body (yaml collection of doc).
  Expects newline at end-of-body"
  [body index doc]
  (-> body
      (string/split #"---\n")
      (->> (remove string/blank?))
      vec
      (assoc-in [index] (doc->yaml doc))
      (->> (string/join "---\n"))
      (->> (str "---\n"))))

(defn append-doc-to-stream
  [body doc]
  (str body "---\n" (doc->yaml doc)))

(defn create-stream-with-doc
  [doc]
  (str "---\n" (doc->yaml doc)))

(defn replace!
  [root-path [file-path index] doc]
  (let [path (str root-path file-path)]
    (let [stream (slurp path)
          new-body (replace-doc-in-stream stream index doc)]
      (spit path new-body))))

(defn insert!
  [file-path doc]
  (if (fs/exists? file-path)
    (spit file-path (append-doc-to-stream (slurp file-path) doc))
    (spit file-path (create-stream-with-doc doc))))


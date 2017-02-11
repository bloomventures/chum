(ns humandb.out
  (:require
    [clojure.string :as string]
    [yaml.core :as yaml]
    [me.raynes.fs :as fs]))

(defn doc->yaml [doc]
  (yaml/generate-string doc :dumper-options {:flow-style :block}))

(defn dissoc-v [v n]
  (into (subvec v 0 n) (subvec v (inc n))))

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

(defn remove-doc-from-stream
  [body index]
  (-> body
      (string/split #"---\n")
      (->> (remove string/blank?))
      vec
      (dissoc-v index)
      (->> (string/join "---\n"))
      (->> (str "---\n"))))

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
  [root-path [file-path] doc]
  (let [path (str root-path file-path)]
    (if (fs/exists? path)
      (spit path (append-doc-to-stream (slurp path) doc))
      (spit path (create-stream-with-doc doc)))))

(defn delete!
  [root-path [file-path index]]
  (let [path (str root-path file-path)
        stream (slurp path)]
    (spit path (remove-doc-from-stream stream index))))

(ns humandb.io
  (:require
    [yaml.core :as yaml]
    [me.raynes.fs :as fs]
    [clojure.string :as string]))

(defmulti parse-data-file
  (fn [path]
    (let [file (fs/file path)
          ext  (-> (fs/extension file)
                   (string/replace-first #"." ""))]
      (keyword ext))))

(defmethod parse-data-file :yaml
  [path]
  (let [file (fs/file path)
        data (slurp file)
        docs (-> data
                 (string/split #"---")
                 (->>
                   (remove string/blank?)
                   (map yaml/parse-string)))]
    docs))

(defn read-schema [root-path]
  (first (parse-data-file (str root-path "/schema.yaml"))))

(defn read-data
  "Given a directory, parses all files as yaml (including subfiles)"
  [root-path]
  (let [directory (fs/file (str root-path "/data"))
        files (->> (file-seq directory)
                   (filter fs/file?))]
    (mapcat parse-data-file files)))

(defn read-db [root-path]
  ; read the schema
  ; read the data
  ; import data
  ; do a little dance


  )

(ns humandb.io
  (:require
    [yaml.core :as yaml]
    [me.raynes.fs :as fs]
    [clojure.string :as string])
  (:import
    [java.nio.file Paths]
    [java.net URI]))

(defn relativize [path-1 path-2]
  (let [path-1 (Paths/get (URI. (str "file://" (.getPath (fs/file path-1)))))
        path-2 (Paths/get (URI. (str "file://" (.getPath (fs/file path-2)))))]
    (.toString (.relativize path-1 path-2))))

(defn annotate-with-src [doc src]
  (-> (reduce (fn [doc [k v]]
                (assoc doc k
                  (cond
                    (map? v)
                    (annotate-with-src v (conj src k))

                    (and (vector? v) (map? (first v)))
                    (map-indexed (fn [i doc]
                                   (annotate-with-src doc (conj src k i))) v)

                    :else
                    v)))
              {}
              doc)
      (assoc :db/src src)))

(defn parse-data-file
  [path root-path]
  (-> path
      fs/file
      slurp
      (string/split #"---")
      (->>
        (remove string/blank?)
        (map yaml/parse-string)
        (map-indexed (fn [i doc]
                       (annotate-with-src doc [(relativize root-path path) i]))))))

(defn parse-schema-file
  [path]
  (-> path
      fs/file
      slurp
      yaml/parse-string))

(defn read-data
  "Given a directory, parses all files as yaml (including subfiles)"
  [root-path]
  (let [data-dir (fs/file (str root-path "/data"))
        files (->> (file-seq data-dir)
                   (filter fs/file?))]
    (mapcat (fn [f] (parse-data-file f data-dir)) files)))

(defn read-schema [root-path]
  (parse-schema-file (str root-path "/schema.yaml")))

(defn initialize-db-folder! [root-path]
  (fs/mkdirs root-path)
  (fs/create (fs/file (str root-path "/schema.yaml")))
  (fs/mkdir (str root-path "/data"))
  (when (empty? (fs/find-files (str root-path "/data/") #".*\.yaml"))
    (fs/create (fs/file (str root-path "/data/entity.yaml")))))


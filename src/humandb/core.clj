(ns humandb.core
  (:require [yaml.core :as yaml]
            [me.raynes.fs :as fs]
            [datascript.core :as d]
            [clojure.string :as string]))

(defn tee [x]
  (println x)
  x)

(defn init! [schema]
  (d/create-conn schema))

(def q d/q)

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

(defn read-data [root-path]
  (let [directory (fs/file (str root-path "/data"))
        files (->> (file-seq directory)
                   (filter fs/file?))]
    (mapcat parse-data-file files)))

(defn import-doc [conn doc]
  (d/transact! conn [doc]))

(defn schema->lookup [schema]
  (reduce (fn [lookup [k _]]
            (let [ks (-> k
                         name
                         (string/split #"-")
                         (->> (map keyword)))]
              (-> lookup
                  (update-in [(first ks)] (fnil (fn [s] (conj s (last ks))) #{}))
                  (update-in [(last ks)] (fnil (fn [s] (conj s (first ks))) #{})))))
          {}
          schema))

(defn import-docs [conn docs]
  (doseq [doc docs]
    (import-doc conn doc)))

(defn relationships->schema [relationships]
  (reduce (fn [schema r]
            (let [sorted-keys (sort [(first r) (last r)])]
              (assoc schema
                   (keyword "rel" (string/join "-" sorted-keys))
                   {:db/cardinality :db.cardinality/many})))
          {}
          relationships))

(defn read-db [config] )

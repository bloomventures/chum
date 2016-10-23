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

(defn doc->raw-eav [doc]
  (reduce (fn [memo [k v]]
            (if (= k :id)
              memo
              (conj memo [(doc :id) k v])))
          []
          doc))

(defn rels->rel-key [rel-1 rel-2]
  (let [sorted-keys (sort [rel-1 rel-2])]
    (keyword "rel" (string/join "-" sorted-keys))))

(defn eavs->txs [eavs]
  (map (fn [[eid attr val]]
         [:db/add eid attr val]) eavs))

(defn doc->eav
  "given a key, returns the correspoding relationship key, if any
   ex. episode-id -> rel/episode-level"
  [relationships doc]

  (let [rel-keys (reduce (fn [memo [type attr rel-type]]
                           (if (= type (doc :type))
                             (assoc memo (keyword attr) rel-type)
                             memo))
                         {}
                         relationships)]
    (->> doc
         doc->raw-eav
         (mapcat (fn [[eid attr value]]
                   (if-let [rel-type (rel-keys attr)]
                     (cond
                       ; single object, ex. {:id 2}
                       (map? value)
                       (concat
                         [[eid (rels->rel-key (doc :type) rel-type) (value :id)]]
                         (doc->eav relationships value))

                       ; list of objects, ex [{:id 1} {:id 2} ...]
                       (and (coll? value) (map? (first value)))
                       (mapcat (fn [obj]
                                 (concat
                                   [[eid (rels->rel-key (doc :type) rel-type) (obj :id)]]
                                   (doc->eav relationships obj))) value)

                       ; list of ids, ex. [1 2 ...]
                       (coll? value)
                       (map (fn [rel-id]
                              [eid (rels->rel-key (doc :type) rel-type) rel-id])  value)

                       ; single id, ex. 2
                       :else
                       [[eid (rels->rel-key (doc :type) rel-type) value]])
                     [[eid attr value]]))))))

(defn docs->txs [relationships docs]
  (mapcat (comp eavs->txs (partial doc->eav relationships)) docs))

(defn import-docs [conn relationships docs]
  (d/transact! conn (docs->txs relationships docs)))

(defn relationships->schema [relationships]
  (reduce (fn [schema r]
            (assoc schema
              (rels->rel-key (first r) (last r))
              {:db/cardinality :db.cardinality/many}))
          {}
          relationships))

(defn read-db [config] )

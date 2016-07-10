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

(defn doc->eav [doc]
  (reduce (fn [memo [k v]]
            (if (= k :id)
              memo
              (conj memo [(doc :id) k v])))
          []
          doc))

(defn rels->rel-key [rel-1 rel-2]
  (let [sorted-keys (sort [rel-1 rel-2])]
    (keyword "rel" (string/join "-" sorted-keys))))

(defn update-rel-keys
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
         doc->eav
         (mapcat (fn [[eid attr value]]
                   (if-let [rel-type (rel-keys attr)]
                     (cond
                       ; single object, ex. {:id 2}
                       (map? value)
                       (concat
                         [[eid (rels->rel-key (doc :type) rel-type) (value :id)]]
                         (update-rel-keys relationships value))

                       ; list of objects, ex [{:id 1} {:id 2} ...]
                       (and (coll? value) (map? (first value)))
                       (mapcat (fn [obj]
                                 (concat
                                   [[eid (rels->rel-key (doc :type) rel-type) (obj :id)]]
                                   (update-rel-keys relationships obj))) value)

                       ; list of ids, ex. [1 2 ...]
                       (coll? value)
                       (map (fn [rel-id]
                              [eid (rels->rel-key (doc :type) rel-type) rel-id])  value)

                       ; single id, ex. 2
                       :else
                       [[eid (rels->rel-key (doc :type) rel-type) value]])
                     [[eid attr value]]))))))

(defn eavs->txs [eavs]
  (map (fn [[eid attr val]]
         [:db/add eid attr val]) eavs))

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

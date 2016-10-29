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

(defn ^:dynamic generate-id []
  (uuid/make-random))

(defn doc->raw-eav [doc]
  (let [id (generate-id)]
    (reduce (fn [memo [k v]]
              (conj memo [id k v]))
            []
            doc)))

(defn eavs->txs [eavs]
  (map (fn [[eid attr val]]
         [:db/add eid attr val]) eavs))

(defn doc->eav
  "given a document, returns the eav

  {:foo 1} -> [[123 :foo 1]]"
  [relationships doc]

  (let [attr-is-a-rel-key?
        (reduce (fn [memo [type attr rel-type]]
                  (if (= type (doc :type))
                    (assoc memo (keyword attr) rel-type)
                    memo))
                {}
                relationships)]
    (->> doc
         doc->raw-eav
         (mapcat (fn [[eid attr value]]
                   (if (attr-is-a-rel-key? attr)
                     (cond
                       ; single object, ex. {:id 2}
                       (map? value)
                       (concat
                         [[eid attr (value :id)]]
                         (doc->eav relationships value))

                       ; list of objects, ex [{:id 1} {:id 2} ...]
                       (and (coll? value) (map? (first value)))
                       (mapcat (fn [obj]
                                 (concat
                                   [[eid attr (obj :id)]]
                                   (doc->eav relationships obj))) value)

                       ; list of ids, ex. [1 2 ...]
                       (coll? value)
                       (map (fn [rel-id]
                              [eid attr rel-id]) value)

                       ; single id, ex. 2
                       :else
                       [[eid attr value]])
                     [[eid attr value]]))))))

(defn docs->txs [relationships docs]
  (mapcat (comp eavs->txs (partial doc->eav relationships)) docs))

(defn import-docs [conn relationships docs]
  (d/transact! conn (docs->txs relationships docs)))

(defn relationships->datascript-schema [relationships]
  (reduce (fn [schema r]
            (assoc schema
                   (keyword (second r))
                   {:db/cardinality :db.cardinality/many}))
          {}
          relationships))

(defn read-db [config] )

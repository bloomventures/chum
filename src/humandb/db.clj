(ns humandb.db
  (:require
    [datascript.core :as d]
    [humandb.io :as io]
    [humandb.import :as import]))

(defn relationships->datascript-schema [relationships]
  (reduce (fn [schema r]
            (assoc schema
                   (keyword (second r))
              {:db/cardinality :db.cardinality/many}))
    {}
    relationships))

(defn create-db
  "creates a datascript database that needs to be passed in other functions"
  [relationships root-path]
  {:conn (d/create-conn (relationships->datascript-schema relationships))
   :relationships relationships
   :root-path root-path})

(defn reload! [db]
  (d/reset-conn! (db :conn)
                 (d/init-db #{}
                            (relationships->datascript-schema (db :relationships))))
  (let [docs (io/read-data (db :root-path))]
    (import/import-docs db docs)))

(defn read-db [root-path]
  (io/initialize-db-folder! root-path)
  (let [schema-data (io/read-schema root-path)
        db (create-db (:relationships schema-data) root-path)
        docs (io/read-data root-path)]
    (import/import-docs db docs)
    db))

(ns humandb.core
  (:require
    [humandb.import :as import]
    [humandb.io :as io]
    [datascript.core :as d]))

(defn query [query db]
  (d/q query @(db :conn)))

(defn read-db [root-path]
  (let [schema-data (io/read-schema root-path)
        relationships (:relationships schema-data)
        docs (io/read-data root-path)
        schema (import/relationships->datascript-schema relationships)
        db {:conn (import/init! schema)
            :relationships relationships}]
    (import/import-docs db docs)
    db))


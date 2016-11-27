(ns humandb.core
  (:require
    [humandb.import :as import]
    [humandb.io :as io]
    [datascript.core :as d]
    [humandb.db :as db]))

(defn query [query db]
  (d/q query @(db :conn)))

(defn read-db [root-path]
  (let [schema-data (io/read-schema root-path)
        db (db/init! (:relationships schema-data))
        docs (io/read-data root-path)]
    (import/import-docs db docs)
    db))


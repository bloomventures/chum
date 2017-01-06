(ns humandb.core
  (:require
    [humandb.import :as import]
    [humandb.io :as io]
    [humandb.transact :as transact]
    [datascript.core :as d]
    [humandb.db :as db]))

(defn query [query db & args]
  (apply d/q query @(db :conn) args))

(def transact! transact/transact!)

(defn read-db [root-path]
  (io/initialize-db-folder! root-path)
  (let [schema-data (io/read-schema root-path)
        db (db/init! (:relationships schema-data) root-path)
        docs (io/read-data root-path)]
    (import/import-docs db docs)
    db))


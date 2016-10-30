(ns humandb.core
  (:require
    [humandb.import :as import]
    [humandb.io :as io]))

(def query import/q)

(defn read-db [root-path]
  (let [schema-data (io/read-schema root-path)
        relationships (:relationships schema-data)
        docs (io/read-data root-path)
        schema (import/relationships->datascript-schema relationships)
        conn (import/init! schema)]
    (import/import-docs conn relationships docs)
    conn))


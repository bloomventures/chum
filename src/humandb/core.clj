(ns humandb.core
  (:require
    [humandb.transact :as transact]
    [datascript.core :as d]
    [humandb.db :as db]))

(defn query [query db & args]
  (apply d/q query @(db :conn) args))

(def transact! transact/transact!)

(def read-db db/read-db)


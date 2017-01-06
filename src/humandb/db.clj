(ns humandb.db
  (:require
    [datascript.core :as d]))

(defn relationships->datascript-schema [relationships]
  (reduce (fn [schema r]
            (assoc schema
                   (keyword (second r))
              {:db/cardinality :db.cardinality/many}))
    {}
    relationships))

(defn init!
  "creates a datascript database that needs to be passed in other functions"
  [relationships root-path]
  {:conn (d/create-conn (relationships->datascript-schema relationships))
   :relationships relationships
   :root-path root-path})

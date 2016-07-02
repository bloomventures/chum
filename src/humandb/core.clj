(ns humandb.core
  (:require [clj-yaml.core :as yaml]
            [clojure.java.io :as io]))

(defn tee [x]
  (println x)
  x)

(defn- read-data-file [path]
  (yaml/parse-string (slurp (io/file path))))

(defn- read-schema [config]
  (read-data-file (str (config :path) "/schema.yaml")))

(defn- read-data [config]

  )

(defn- read-db [config]



  )

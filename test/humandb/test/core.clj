(ns humandb.test.core
  (:require [clojure.test :refer :all]
            [humandb.core :as db]))

(def db-config
  {:path "./resources/penyo-data"})

(deftest core-tests
  (testing "foobar"
    #_(db/query db-config "*")

    (is true)))

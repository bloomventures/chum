(ns humandb.test.io
  (:require
    [clojure.test :refer :all]
    [humandb.io :as io]))

(deftest parse-data-file
  (testing "parsing yaml file"
    (is (= [{:id "eloulk" :name "Kosso Eloul" :type "artist"}
            {:id "etrogs" :name "Sorol Etrog" :type "artist"}]
           (io/parse-data-file "./resources/test_data/data/artists.yaml")))))

(deftest read-data
  (testing "reading all data from a directory"
    (is (= [{:id "eloulk" :name "Kosso Eloul" :type "artist"}
            {:id "etrogs" :name "Sorol Etrog" :type "artist"}
            {:name "Sculpture 1" :artist-ids ["eloulk"] :type "sculpture"}
            {:name "Sculpture 2" :artist-ids ["eloulk" "etrogs"] :type "sculpture"}]
          (io/read-data "./resources/test_data")))))

(deftest read-schema
  (testing "reading schema from file"
    (is (= {:relationships [["sculpture", "artist-ids", "artist"]] }
          (io/read-schema "./resources/test_data")))))


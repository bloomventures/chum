(ns humandb.test.core
  (:require
    [clojure.test :refer :all]
    [humandb.core :as humandb]))

(deftest end-to-end-test
  (testing "reading test_data"
    (let [db (humandb/read-db "./resources/test_data")]
      (is (= (set ["Sculpture 1" "Sculpture 2"])
            (set (->> (humandb/query '[:find ?sculpture-name
                                       :where
                                       [?artist :name "Kosso Eloul"]
                                       [?artist :id ?artist-id]
                                       [?sculpture :artist-ids ?artist-id]
                                       [?sculpture :name ?sculpture-name]]
                        db)
                   (map first))))))))


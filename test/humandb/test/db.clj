(ns humandb.test.db
  (:require
    [clojure.test :refer :all]
    [humandb.db :as db]))

(deftest relationships->datascript-schema

  (testing "relationships->datascript-schema"
    (let [relationships [["episode" "episode-id" "level"]
                         ["level" "level-id" "word"]]
          schema (db/relationships->datascript-schema relationships)]

      (is (= schema {:episode-id {:db/cardinality :db.cardinality/many}
                     :level-id {:db/cardinality :db.cardinality/many}})))))

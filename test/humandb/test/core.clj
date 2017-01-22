(ns humandb.test.core
  (:require
    [clojure.test :refer :all]
    [me.raynes.fs :as fs]
    [humandb.core :as humandb]))

(deftest read-db
  (testing "creates target folder when folder does not exist"
    (let [path (str "/tmp/human-db-test" (gensym))
          db (humandb/read-db path)]
      (is (fs/exists? path))))

  (testing "creates schema file when no schema file"
    (let [path (str "/tmp/human-db-test" (gensym))
          db (humandb/read-db path)]
      (is (fs/exists? (str path "/schema.yaml")))))

  (testing "creates a data folder when no data folder"
    (let [path (str "/tmp/human-db-test" (gensym))
          db (humandb/read-db path)]
      (is (fs/exists? (str path "/data")))))

  (testing "creates a data file when no data files"
    (let [path (str "/tmp/human-db-test" (gensym))
          db (humandb/read-db path)]
      (is (fs/exists? (str path "/data/entity.yaml")))))

  (testing "does not create data file when existing data files"
    (let [path (str "/tmp/human-db-test" (gensym))
          _ (fs/mkdirs (str path "/data"))
          _ (fs/create (fs/file (str path "/data/foo.yaml")))
          db (humandb/read-db path)]
      (is (not (fs/exists? (str path "/data/entity.yaml")))))))

(deftest end-to-end-test
  (testing "end-to-end"
    (let [temp-db-path (str "/tmp/" (gensym "humandb_end_to_end_test"))]
      (fs/copy-dir "./resources/test_data" temp-db-path)
      (let [db (humandb/read-db temp-db-path)]
        (testing "query"
          (is (= (set ["Sculpture 1" "Sculpture 2"])
                 (set (->> (humandb/query '[:find ?sculpture-name
                                            :in $
                                            :where
                                            [?artist :name "Kosso Eloul"]
                                            [?artist :id ?artist-id]
                                            [?sculpture :artist-ids ?artist-id]
                                            [?sculpture :name ?sculpture-name]]
                             db)
                           (map first))))))

        (testing "update a doc"
          (let [artist-eid (first (humandb/query '[:find [?artist]
                                                   :where
                                                   [?artist :id "eloulk"]]
                                    db))
                path (humandb/query '[:find ?path
                                      :in $ ?eid
                                      :where
                                      [?eid :db/src ?path]]
                       db
                       artist-eid)]

            (humandb/transact! db [[:db/add artist-eid :name "John Smith"]])

            (testing "updated in memory"
              (is (= "John Smith"
                     (ffirst (humandb/query '[:find ?name
                                              :in $ ?artist-eid
                                              :where
                                              [?artist-eid :name ?name]]
                                            db
                                            artist-eid)))))

            (testing "updated on disk"
              (let [db2 (humandb/read-db temp-db-path)]
                (is (= "John Smith"
                       (ffirst (humandb/query '[:find ?name
                                                :in $ ?id
                                                :where
                                                [?artist-eid :id ?id]
                                                [?artist-eid :name ?name]]
                                 db2
                                 "eloulk"))))))))

        (testing "create a new doc"
          (humandb/transact! db
                             [{:db/id -1
                               :id "bobbersonb"
                               :type "artist"
                               :name "Bob Bobberson"}])

          (testing "added in memory"
            (is (= "Bob Bobberson"
                   (first (humandb/query '[:find [?name]
                                           :in $ ?id
                                           :where
                                           [?artist :id ?id]
                                           [?artist :name ?name]]
                                         db
                                         "bobbersonb")))))

          (testing "added to disk"
            (let [db2 (humandb/read-db temp-db-path)]
              (is (= "Bob Bobberson"
                     (first (humandb/query '[:find [?name]
                                             :in $ ?id
                                             :where
                                             [?artist :id ?id]
                                             [?artist :name ?name]]
                                           db2
                                           "bobbersonb")))))))



        (testing "delete a doc")))))

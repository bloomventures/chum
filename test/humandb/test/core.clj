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
      (let [db (humandb/read-db temp-db-path)
            artist-id "eloulk"]
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
          (let [artist-id "eloulk"
                artist-name "John Smith"
                artist-eid (first (humandb/query '[:find [?artist]
                                                   :in $ ?artist-id
                                                   :where
                                                   [?artist :id ?artist-id]]
                                    db
                                    artist-id))]

            (humandb/transact! db [[:db/add artist-eid :name artist-name]])

            (testing "updated in memory"
              (is (= artist-name
                     (ffirst (humandb/query '[:find ?name
                                              :in $ ?artist-id
                                              :where
                                              [?artist :name ?name]
                                              [?artist :id ?artist-id]]
                                            db
                                            artist-id)))))

            (testing "updated on disk"
              (let [db2 (humandb/read-db temp-db-path)]
                (is (= artist-name
                       (ffirst (humandb/query '[:find ?name
                                                :in $ ?id
                                                :where
                                                [?artist-eid :id ?id]
                                                [?artist-eid :name ?name]]
                                 db2
                                 artist-id))))))))

        (testing "create a new doc"
          (let [artist-id "bobbersonb"
                artist-name "Bob Bobberson"]
            (humandb/transact! db
                               [{:db/id -1
                                 :id artist-id
                                 :type "artist"
                                 :name "Bob Bobberson"}])

            (testing "added in memory"
              (is (= artist-name
                     (first (humandb/query '[:find [?name]
                                             :in $ ?id
                                             :where
                                             [?artist :id ?id]
                                             [?artist :name ?name]]
                                           db
                                           artist-id)))))

            (testing "added to disk"
              (let [db2 (humandb/read-db temp-db-path)]
                (is (= artist-name
                       (first (humandb/query '[:find [?name]
                                               :in $ ?id
                                               :where
                                               [?artist :id ?id]
                                               [?artist :name ?name]]
                                             db2
                                             artist-id))))))

            (testing "updating a newly created doc"
              (let [artist-name "Sir Bobberson the Updated"
                    artist-eid (first (humandb/query '[:find [?artist]
                                                       :in $ ?artist-id
                                                       :where
                                                       [?artist :id ?artist-id]]
                                                     db
                                                     artist-id))]

                (humandb/transact! db [[:db/add artist-eid :name artist-name]])

                (testing "updated in memory"
                  (is (= artist-name
                         (ffirst (humandb/query '[:find ?name
                                                  :in $ ?artist-id
                                                  :where
                                                  [?artist :name ?name]
                                                  [?artist :id ?artist-id]]
                                                db
                                                artist-id)))))

                (testing "updated on disk"
                  (let [db2 (humandb/read-db temp-db-path)
                        results (humandb/query '[:find ?name
                                                 :in $ ?id
                                                 :where
                                                 [?artist-eid :id ?id]
                                                 [?artist-eid :name ?name]]
                                               db2
                                               artist-id)]
                    (is (= 1 (count results)))
                    (is (= artist-name (ffirst results)))))))))




        (testing "delete a doc")))))

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


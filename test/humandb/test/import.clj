(ns humandb.test.import
  (:require
    [clojure.test :refer :all]
    [humandb.import :as db]
    [datascript.core :as d]))

(defn mock-generate-id [t]
  (binding [db/generate-id (let [id (atom 0)]
                             (fn []
                               (swap! id inc)
                               @id))]
    (t)))

(use-fixtures :each mock-generate-id)

(deftest relationships->datascript-schema

  (testing "relationships->datascript-schema"
    (let [relationships [["episode" "episode-id" "level"]
                         ["level" "level-id" "word"]]
          schema (db/relationships->datascript-schema relationships)]

      (is (= schema {:episode-id {:db/cardinality :db.cardinality/many}
                     :level-id {:db/cardinality :db.cardinality/many}})))))

(deftest import-docs
  (testing "import-docs"
    (testing "basic"
      (let [relationships []
            schema {}
            conn (db/init! schema)
            docs [{:name "Alice"
                   :id 1}
                  {:name "Bob"
                   :id 2}]]

        (db/import-docs conn relationships docs)

        (is (= "Bob" (first (d/q '[:find [?name]
                                   :where
                                   [?e :id 2]
                                   [?e :name ?name]]
                              @conn))))))

    (testing "string ids"
      (let [relationships [["user", "friend-id", "user"]]
            schema (db/relationships->datascript-schema relationships)
            conn (db/init! schema)
            docs [{:name "Alice"
                   :id "alice"
                   :type "user"
                   :friend-id "bob"}
                  {:name "Bob"
                   :id "bob"
                   :type "user"}]]

        (db/import-docs conn relationships docs)

        (is (= "Bob" (first (d/q '[:find [?name]
                                   :where
                                   [?a :name "Alice"]
                                   [?a :friend-id ?b-id]
                                   [?b :id ?b-id]
                                   [?b :name ?name]]
                              @conn))))))


    (testing "evil"
      (let [relationships [["episode", "levels", "level"]
                           ["level", "word-ids", "word"]
                           ["translation", "variation-id", "variation"]
                           ["translation", "word-id", "word"]]
            schema (db/relationships->datascript-schema relationships)
            conn (db/init! schema)
            docs [{:name "Artist"
                   :type "episode"
                   :id 1
                   :levels [{:id 2
                             :type "level"
                             :word-ids [3]}]}
                  {:id 4
                   :type "translation"
                   :variation-id 5
                   :word-id 3
                   :value "vert"}
                  {:id 5
                   :type "variation"
                   :name "fr-ca"}
                  {:id 3
                   :type "word"
                   :name "green"}]]

        (db/import-docs conn relationships docs)

        (is (= "Artist" (first (d/q '[:find [?name]
                                      :where
                                      [?variation :name "fr-ca"]
                                      [?variation :id ?variation-id]
                                      [?translation :variation-id ?variation-id]
                                      [?translation :word-id ?word-id]
                                      [?level :word-ids ?word-id]
                                      [?level :id ?level-id]
                                      [?episode :levels ?level-id]
                                      [?episode :name ?name]]
                                 @conn))))))))

(deftest docs->txs
  (testing "docs->txs"

    (testing "foreign key"
      (let [relationships [["level" "episode-id" "episode"]]
            docs [{:id 1
                   :type "level"
                   :episode-id 10}
                  {:id 2
                   :type "level"
                   :episode-id 20}
                  {:id 10
                   :type "episode"}
                  {:id 20
                   :type "episode"}]]

        (is (= [[:db/add 1 :id 1]
                [:db/add 1 :type "level"]
                [:db/add 1 :episode-id 10]
                [:db/add 2 :id 2]
                [:db/add 2 :type "level"]
                [:db/add 2 :episode-id 20]
                [:db/add 3 :id 10]
                [:db/add 3 :type "episode"]
                [:db/add 4 :id 20]
                [:db/add 4 :type "episode"]]
               (db/docs->txs relationships docs)))))))

(deftest doc->eav

  (testing "doc->eav"

    (testing "foreign key"
      (let [relationships [["level" "episode-id" "episode"]]
            doc {:id 100
                 :type "level"
                 :episode-id 300}]

        (is (= [[1 :id 100]
                [1 :type "level"]
                [1 :episode-id 300]]
              (db/doc->eav relationships doc)))))

    (testing "id array"
      (let [relationships [["episode" "level-ids" "level"]]
            doc {:id 1
                 :type "episode"
                 :level-ids [2 3]}]

        (is (= [[2 :id 1]
                [2 :type "episode"]
                [2 :level-ids 2]
                [2 :level-ids 3]]
               (db/doc->eav relationships doc)))))

    (testing "single embedded"
      (let [relationships [["episode" "level" "level"]]
            doc {:id 1
                 :type "episode"
                 :level {:id 2
                         :type "level"}}]

        (is (= [[3 :id 1]
                [3 :type "episode"]
                [3 :level 2]
                [4 :id 2]
                [4 :type "level"]]
               (db/doc->eav relationships doc)))))

    (testing "multiple embedded"
      (let [relationships [["episode" "levels" "level"]]
            doc {:id 1
                 :type "episode"
                 :levels [{:id 2
                           :type "level"}
                          {:id 3
                           :type "level"}]}]

        (is (= [[5 :id 1]
                [5 :type "episode"]
                [5 :levels 2]
                [6 :id 2]
                [6 :type "level"]
                [5 :levels 3]
                [7 :id 3]
                [7 :type "level"]]
               (db/doc->eav relationships doc)))))


    (testing "nested embedded"
      (let [relationships [["episode" "level" "level"]
                           ["level" "word" "word"]]
            doc {:id 1
                 :type "episode"
                 :level {:id 2
                         :type "level"
                         :word {:id 3
                                :type "word"}}}]

        (is (= [[8 :id 1]
                [8 :type "episode"]
                [8 :level 2]
                [9 :id 2]
                [9 :type "level"]
                [9 :word 3]
                [10 :id 3]
                [10 :type "word"]]
               (db/doc->eav relationships doc)))))))

(deftest doc->raw-eav
  (testing "doc->raw-eav"
    (let [doc  {:id 2
                :name "Colors 1"
                :type "level"
                :episode-id 1}]
      (is (= [[1 :id 2]
              [1 :name "Colors 1"]
              [1 :type "level"]
              [1 :episode-id 1]]
             (db/doc->raw-eav doc))))))

(deftest eavs->txs
  (testing "eavs->txs"
    (let [eavs [[2 :name "Colors 1"]
                [2 :type "level"]
                [2 :episode-id 1]]]
      (is (= [[:db/add 2 :name "Colors 1"]
              [:db/add 2 :type "level"]
              [:db/add 2 :episode-id 1]]
             (db/eavs->txs eavs))))))


(ns humandb.test.core
  (:require [clojure.test :refer :all]
            [humandb.core :as db]))

(defn tee [x]
  (println x)
  x)

(def db-config
  {:path "./resources/penyo-data"})

(deftest core-tests

  ; deprecated
  (testing "relationships->datascript-schema"
    (let [relationships [["episode" "..." "level"]
                         ["level" "..." "word"]]
          schema (db/relationships->datascript-schema relationships)]

      (is (= schema {:rel/episode-level {:db/cardinality :db.cardinality/many}
                     :rel/level-word {:db/cardinality :db.cardinality/many}})))

    (testing "relationship keys are in alpha order"
      (let [relationships [["zzz" "..." "aaa"]]
            schema (db/relationships->datascript-schema relationships)]

        (is (= schema {:rel/aaa-zzz {:db/cardinality :db.cardinality/many}})))))

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

        (is (= "Bob" (first (db/q '[:find [?name]
                                    :where
                                    [2 :name ?name]]
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

        (is (= "Artist" (first (db/q '[:find [?name]
                                       :where
                                       [?variation-id :name "fr-ca"]
                                       [?translation-id :rel/translation-variation ?variation-id]
                                       [?translation-id :rel/translation-word ?word-id]
                                       [?level-id :rel/level-word ?word-id]
                                       [?episode-id :rel/episode-level ?level-id]
                                       [?episode-id :name ?name]]
                                  @conn)))))))

  (testing "docs->txs"
    (testing "foreign key"
      (let [relationships [["level" "episode-id" "episode"]]
            schema (db/relationships->datascript-schema relationships)
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

        (is (= (db/docs->txs relationships docs)
               [[:db/add 1 :type "level"]
                [:db/add 1 :rel/episode-level 10]
                [:db/add 2 :type "level"]
                [:db/add 2 :rel/episode-level 20]
                [:db/add 10 :type "episode"]
                [:db/add 20 :type "episode"]])))))

  (testing "doc->eav"

    (testing "foreign key"
      (let [relationships [["level" "episode-id" "episode"]]
            schema (db/relationships->datascript-schema relationships)
            doc {:id 1
                 :type "level"
                 :episode-id 3}]

         (is (= (db/doc->eav relationships doc)
                [[1 :type "level"]
                 [1 :rel/episode-level 3]]))))

    (testing "id array"
      (let [relationships [["episode" "level-ids" "level"]]
            doc {:id 1
                 :type "episode"
                 :level-ids [2 3]}]

        (is (= (db/doc->eav relationships doc)
               [[1 :type "episode"]
                [1 :rel/episode-level 2]
                [1 :rel/episode-level 3]]))))

    (testing "single embedded"
      (let [relationships [["episode" "level" "level"]]
            doc {:id 1
                 :type "episode"
                 :level {:id 2
                         :type "level"}}]

        (is (= (db/doc->eav relationships doc)
               [[1 :type "episode"]
                [1 :rel/episode-level 2]
                [2 :type "level"]]))))


    (testing "multiple embedded"
      (let [relationships [["episode" "levels" "level"]]
            doc {:id 1
                 :type "episode"
                 :levels [{:id 2
                           :type "level"}
                          {:id 3
                           :type "level"}]}]

        (is (= (db/doc->eav relationships doc)
               [[1 :type "episode"]
                [1 :rel/episode-level 2]
                [2 :type "level"]
                [1 :rel/episode-level 3]
                [3 :type "level"]]))))


    (testing "nested embedded"
      (let [relationships [["episode" "level" "level"]
                           ["level" "word" "word"]]
            doc {:id 1
                 :type "episode"
                 :level {:id 2
                         :type "level"
                         :word {:id 3
                                :type "word"}}}]

        (is (= (db/doc->eav relationships doc)
               [[1 :type "episode"]
                [1 :rel/episode-level 2]
                [2 :type "level"]
                [2 :rel/level-word 3]
                [3 :type "word"]])))))

  (testing "doc->raw-eav"
    (let [doc  {:id 2
                :name "Colors 1"
                :type "level"
                :episode-id 1}]
      (is (= (db/doc->raw-eav doc)
             [[2 :name "Colors 1"]
              [2 :type "level"]
              [2 :episode-id 1]]))))

  (testing "eavs->txs"
    (let [eavs [[2 :name "Colors 1"]
                [2 :type "level"]
                [2 :episode-id 1]]]
      (is (= (db/eavs->txs eavs)
             [[:db/add 2 :name "Colors 1"]
              [:db/add 2 :type "level"]
              [:db/add 2 :episode-id 1]])))))



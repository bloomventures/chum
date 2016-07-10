(ns humandb.test.core
  (:require [clojure.test :refer :all]
            [humandb.core :as db]))

(defn tee [x]
  (println x)
  x)

(def db-config
  {:path "./resources/penyo-data"})

(deftest core-tests

  (testing "relationships->schema"
    (let [relationships [["episode" "..." "level"]
                         ["level" "..." "word"]]
          schema (db/relationships->schema relationships)]

      (is (= schema {:rel/episode-level {:db/cardinality :db.cardinality/many}
                     :rel/level-word {:db/cardinality :db.cardinality/many}})))

    (testing "relationship keys are in alpha order"
      (let [relationships [["zzz" "..." "aaa"]]
            schema (db/relationships->schema relationships)]

        (is (= schema {:rel/aaa-zzz {:db/cardinality :db.cardinality/many}})))))

  (testing "schema->lookup"
    (let [schema {:rel/episode-level {:db/cardinality :db.cardinality/many}
                  :rel/level-word {:db/cardinality :db.cardinality/many}}
          result  {:episode #{:level}
                   :level #{:word :episode}
                   :word #{:level}}]
      (is (= result (db/schema->lookup schema)))))

  #_(testing "import-docs"
    (testing "basic"
      (let [schema {}
            conn (db/init! schema)
            docs [{:name "Alice"
                   :id 1}
                  {:name "Bob"
                   :id 2}]]

        (db/import-docs conn docs)

        (is (= "Bob" (first (db/q '[:find [?name]
                                    :where
                                    [?e :name ?name]
                                    [?e :id 2]]
                                  @conn))))))

    (testing "one-to-many"
      (testing "foreign key"
        (let [raw-schema [["level" "episode-id" "episode"]]
              schema {:rel/episode-level {:db/cardinality :db.cardinality/many}}
              docs [{:id 1
                     :name "Artist"
                     :type "episode"}
                    {:id 2
                     :name "Colors 1"
                     :type "level"
                     :episode-id 1}]
              conn (db/init! schema)]

          (db/import-docs conn docs)

          ; TODO: need to have import-doc add the :rel/episode-level based on lookup table

          (is (= "Artist" (first (db/q '[:find [?name]
                                         :where
                                         [?lvl :id 2]
                                         [?e :rel/episode-level ?lvl]
                                         [?e :name ?name]]
                                       @conn))))))

      (testing "id array"
        (let [schema {}
              conn (db/init! schema)
              docs [{:id 1
                     :name "Artist"
                     :type "episode"
                     :level-ids [2]}
                    {:id 2
                     :name "Colors 1"
                     :type "level"}]]

          ; TODO
          ))


      (testing "embedded"
        (let [schema {}
              conn (db/init! schema)
              docs [{:id 1
                     :name "Artist"
                     :type "episode"
                     :levels [{:id 2
                               :name "Colors 1"
                               :type "level"}]}]]


          ; TODO
          )))


    )


  (testing "update-rel-keys"

    (testing "foreign key"
      (let [relationships [["level" "episode-id" "episode"]]
            schema (db/relationships->schema relationships)
            doc {:id 1
                 :type "level"
                 :episode-id 3}]

         (is (= (db/update-rel-keys relationships doc)
                [[1 :type "level"]
                 [1 :rel/episode-level 3]]))))

    (testing "id array"
      (let [relationships [["episode" "level-ids" "level"]]
            doc {:id 1
                 :type "episode"
                 :level-ids [2 3]}]

        (is (= (db/update-rel-keys relationships doc)
               [[1 :type "episode"]
                [1 :rel/episode-level 2]
                [1 :rel/episode-level 3]]))))

    (testing "single embedded"
      (let [relationships [["episode" "level" "level"]]
            doc {:id 1
                 :type "episode"
                 :level {:id 2
                         :type "level"}}]

        (is (= (db/update-rel-keys relationships doc)
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

        (is (= (db/update-rel-keys relationships doc)
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

        (is (= (db/update-rel-keys relationships doc)
               [[1 :type "episode"]
                [1 :rel/episode-level 2]
                [2 :type "level"]
                [2 :rel/level-word 3]
                [3 :type "word"]])))))

  (testing "doc->eav"
    (let [doc  {:id 2
                :name "Colors 1"
                :type "level"
                :episode-id 1}]
      (is (= (db/doc->eav doc)
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



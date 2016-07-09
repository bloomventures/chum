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
    (let [relationships [["episode" "-*" "level"]
                         ["level" "-*" "word"]]
          schema (db/relationships->schema relationships)]

      (is (= schema {:rel/episode-level {:db/cardinality :db.cardinality/many}
                     :rel/level-word {:db/cardinality :db.cardinality/many}})))

    (testing "relationship keys are in alpha order"
      (let [relationships [["zzz" "-*" "aaa"]]
            schema (db/relationships->schema relationships)]

        (is (= schema {:rel/aaa-zzz {:db/cardinality :db.cardinality/many}})))))

  (testing "schema->lookup"
    (let [schema {:rel/episode-level {:db/cardinality :db.cardinality/many}
                  :rel/level-word {:db/cardinality :db.cardinality/many}}
          result  {:episode #{:level}
                   :level #{:word :episode}
                   :word #{:level}}]
      (is (= result (db/schema->lookup schema)))))

  (testing "import-docs"
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
        (let [raw-schema [["episode" "-*" "level"]]
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
  )

(ns humandb.test.transact
  (:require
    [clojure.test :refer :all]
    [humandb.transact :as tx]
    [humandb.import :as db]
    [datascript.core :as d]))

(deftest affected-docs
  (testing "single"
    (let [txs [[:db/add 1234 :foo "bar"]]]
      (is (= #{1234}
             (tx/affected-docs txs)))))

  (testing "multiple"
    (let [txs [[:db/add 1234 :foo "bar"]
               [:db/add 5678 :foo "bar"]]]
      (is (= #{1234 5678}
             (tx/affected-docs txs)))))

  (testing "removes dupes"
    (let [txs [[:db/add 1234 :foo "bar"]
               [:db/add 1234 :bar "baz"]]]
      (is (= #{1234}
             (tx/affected-docs txs))))))

(deftest get-doc
  (testing "returns doc attributes"
    (let [conn (d/create-conn {})]
      (d/transact conn [{:db/id 1234
                         :foo "bar"}])
      (is (= {:db/id 1234
              :foo "bar"}
             (tx/get-doc (d/db conn) 1234))))))

(deftest remove-metadata
  (testing "removes db/* attributes"
    (testing "direct attributes"
      (let [raw-attrs {:normal-attr "keep this"
                       :db/anything "remove this"}]
        (is (= {:normal-attr "keep this"}
               (tx/remove-metadata raw-attrs)))))

    (testing "embedded doc"
      (let [raw-attrs {:attr "keep"
                       :db/attr "remove"
                       :embed {:attr "keep"
                               :db/attr "remove"}}]
        (is (= {:attr "keep"
                :embed {:attr "keep"}}
               (tx/remove-metadata raw-attrs)))))

    (testing "list of embedded docs"
      (let [raw-attrs {:attr "keep"
                       :db/attr "remove"
                       :embed [{:attr "keep"
                                :db/attr "remove"}
                               {:attr "keep"
                                :db/attr "remove"}]}]
        (is (= {:attr "keep"
                :embed [{:attr "keep"}
                        {:attr "keep"}]}
               (tx/remove-metadata raw-attrs)))))))

(deftest get-parent
  (testing "returns parent"
    (testing "when nested"
      (let [relationships [["post", "comments", "comment"]]
            schema (db/relationships->datascript-schema relationships)
            conn (db/init! schema)
            docs [{:type "post"
                   :content "abcde"
                   :id 1000
                   :comments {:id 2000
                               :content "foobar"
                               :type "comment"}}
                  {:type "post"
                   :id 4000
                   :content "zzzzz"}]]
        (db/import-docs conn relationships docs)
        (let [pid (first (d/q '[:find [?eid]
                                :in $ ?id
                                :where
                                [?eid :id ?id]]
                           @conn
                           1000))
              eid (first (d/q '[:find [?eid]
                                :in $ ?id
                                :where
                                [?eid :id ?id]]
                           @conn
                           2000))]
          (is (= pid
                 (tx/get-parent conn eid))))))

    (testing "when nested in array"
      (let [relationships [["post", "comments", "comment"]]
            schema (db/relationships->datascript-schema relationships)
            conn (db/init! schema)
            docs [{:type "post"
                   :content "abcde"
                   :id 1000
                   :comments [{:id 2000
                               :content "foobar"
                               :type "comment"}
                              {:id 3000
                               :content "foobar"
                               :type "comment"}]}
                  {:type "post"
                   :id 4000
                   :content "zzzzz"}]]
        (db/import-docs conn relationships docs)
        (let [pid (first (d/q '[:find [?eid]
                                :in $ ?id
                                :where
                                [?eid :id ?id]]
                           @conn
                           1000))
              eid (first (d/q '[:find [?eid]
                                :in $ ?id
                                :where
                                [?eid :id ?id]]
                           @conn
                           2000))]
          (is (= pid
                 (tx/get-parent conn eid))))))

    (testing "when nested multiple levels deep"
      (let [relationships [["post", "comments", "comment"]
                           ["comment", "author", "author"]]
            schema (db/relationships->datascript-schema relationships)
            conn (db/init! schema)
            docs [{:type "post"
                   :content "abcde"
                   :id 1000
                   :comments [{:id 2000
                               :content "foobar"
                               :type "comment"
                               :author {:id 4000
                                        :type "author"
                                        :email "foo@bar.com"}}]}]]
        (db/import-docs conn relationships docs)
        (let [pid (first (d/q '[:find [?eid]
                                :in $ ?id
                                :where
                                [?eid :id ?id]]
                           @conn
                           2000))
              eid (first (d/q '[:find [?eid]
                                :in $ ?id
                                :where
                                [?eid :id ?id]]
                           @conn
                           4000))]
          (is (= pid
                 (tx/get-parent conn eid)))))))

  (testing "does not return parent for docs that aren't embedded"
    (let [relationships [["post", "comments", "comment"]]
          schema (db/relationships->datascript-schema relationships)
          conn (db/init! schema)
          docs [{:type "post"
                 :content "abcde"
                 :id 1000
                 :comments [2000 3000]}
                {:id 2000
                 :content "foobar"
                 :type "comment"}
                {:id 3000
                 :content "foobar"
                 :type "comment"}
                {:type "post"
                 :id 4000
                 :content "zzzzz"}]]
      (db/import-docs conn relationships docs)
      (let [eid (first (d/q '[:find [?eid]
                              :in $ ?id
                              :where
                              [?eid :id ?id]]
                         @conn
                         2000))]
        (is (= nil
               (tx/get-parent conn eid)))))))

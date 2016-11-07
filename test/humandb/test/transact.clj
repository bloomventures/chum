(ns humandb.test.transact
  (:require
    [clojure.test :refer :all]
    [humandb.transact :as tx]
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

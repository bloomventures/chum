(ns humandb.test.out
  (:require
    [clojure.test :refer :all]
    [clojure.string :as string]
    [humandb.out :as out]))

(deftest replace
  (testing "first"
    (let [before (string/join
                   "\n"
                   ["---"
                    "id: 1"
                    "---"
                    "id: 2"
                    "---"
                    ""])
          after (string/join
                  "\n"
                  ["---"
                   "foo: bar"
                   "---"
                   "id: 2"
                   "---"
                   ""])]
      (is (= after
             (out/replace before [0] {:foo "bar"})))))

  (testing "second"
    (let [before (string/join
                   "\n"
                   ["---"
                    "id: 1"
                    "---"
                    "id: 2"
                    "---"
                    ""])
          after (string/join
                  "\n"
                  ["---"
                   "id: 1"
                   "---"
                   "foo: bar"
                   "---"
                   ""])]
      (is (= after
             (out/replace before [1] {:foo "bar"}))))))

(ns humandb.test.out
  (:require
    [clojure.test :refer :all]
    [clojure.string :as string]
    [humandb.out :as out]))

(deftest replace-doc-in-stream
  (testing "first"
    (let [before (string/join "\n"
                   ["---"
                    "id: 1"
                    "---"
                    "id: 2"
                    ""])
          after (string/join "\n"
                  ["---"
                   "foo: bar"
                   "---"
                   "id: 2"
                   ""])]
      (is (= after
             (out/replace-doc-in-stream before 0 {:foo "bar"})))))

  (testing "second"
    (let [before (string/join "\n"
                   ["---"
                    "id: 1"
                    "---"
                    "id: 2"
                    ""])
          after (string/join "\n"
                  ["---"
                   "id: 1"
                   "---"
                   "foo: bar"
                   ""])]
      (is (= after
             (out/replace-doc-in-stream before 1 {:foo "bar"}))))))

(deftest remove-doc-from-stream
  (testing "can remove doc from stream"
    (testing "start"
      (let [before (string/join "\n"
                                ["---"
                                 "id: 1"
                                 "---"
                                 "id: 2"
                                 ""])
            after (string/join "\n"
                               ["---"
                                "id: 2"
                                ""])]
        (is (= after
               (out/remove-doc-from-stream before 0))))

      (testing "middle"
        (let [before (string/join "\n"
                                  ["---"
                                   "id: 1"
                                   "---"
                                   "id: 2"
                                   "---"
                                   "id: 3"
                                   ""])
              after (string/join "\n"
                                 ["---"
                                  "id: 1"
                                  "---"
                                  "id: 3"
                                  ""])]
          (is (= after
                 (out/remove-doc-from-stream before 1)))))

      (testing "end"
        (let [before (string/join "\n"
                                  ["---"
                                   "id: 1"
                                   "---"
                                   "id: 2"
                                   "---"
                                   "id: 3"
                                   ""])
              after (string/join "\n"
                                 ["---"
                                  "id: 1"
                                  "---"
                                  "id: 2"
                                  ""])]
          (is (= after
                 (out/remove-doc-from-stream before 2))))))))

(deftest append-doc-to-stream
  (testing "can append doc to yaml stream"
    (let [doc {:id 3}
          before (string/join "\n"
                   ["---"
                    "id: 1"
                    "---"
                    "id: 2"
                    ""])
          after (string/join "\n"
                  ["---"
                   "id: 1"
                   "---"
                   "id: 2"
                   "---"
                   "id: 3"
                   ""])]
      (is (= after (out/append-doc-to-stream before doc))))))

(deftest create-stream-with-doc
  (testing "can create stream of yaml docs"
    (let [doc {:id 3}
          after (string/join "\n"
                  ["---"
                   "id: 3"
                   ""])]
      (is (= after (out/create-stream-with-doc doc))))))

(deftest replace!
  (testing "..."
    (let [before (string/join "\n"
                   ["---"
                    "id: 1"
                    "---"
                    "id: 2"
                    ""])
          after (string/join "\n"
                  ["---"
                   "id: 1"
                   "---"
                   "foo: bar"
                   ""])
          root-path "/tmp/"
          file-path "humandb_out_replace_test.yaml"
          path (str root-path file-path)]
      (spit path before)
      (out/replace! root-path [file-path 1] {:foo "bar"})
      (is (= after (slurp path))))))

(deftest insert!
  (testing "insert when no file exists, creates file"
    (let [doc {:id 3}
          after (string/join "\n"
                  ["---"
                   "id: 3"
                   ""])
          root-path "/tmp/"
          file-path (str (gensym "humandb_out_insert!_test") ".yaml")]
      (out/insert! root-path [file-path] doc)
      (is (= after (slurp (str root-path file-path))))))

  (testing "insert when file exists, appends to file"
    (let [doc {:id 3}
          before (string/join "\n"
                     ["---"
                      "id: 1"
                      "---"
                      "id: 2"
                      ""])
          after (string/join "\n"
                    ["---"
                     "id: 1"
                     "---"
                     "id: 2"
                     "---"
                     "id: 3"
                     ""])
          root-path "/tmp/"
          file-path (str (gensym "humandb_out_insert!_test") ".yaml")]
      (spit (str root-path file-path) before)
      (out/insert! root-path [file-path] doc)
      (is (= after (slurp (str root-path file-path)))))))

(ns humandb.test.io
  (:require
    [clojure.test :refer :all]
    [humandb.io :as io]))

(deftest parse-data-file
  (testing "parsing yaml file"
    (is (= [{:id "eloulk" :name "Kosso Eloul" :type "artist" :db/src ["artists.yaml" 0]}
            {:id "etrogs" :name "Sorel Etrog" :type "artist" :db/src ["artists.yaml" 1]}]
           (io/parse-data-file
             "./resources/test_data/data/artists.yaml"
             "./resources/test_data/data")))))

(deftest read-data
  (testing "reading all data from a directory"
    (is (= [{:id "eloulk" :name "Kosso Eloul" :type "artist" :db/src ["artists.yaml" 0]}
            {:id "etrogs" :name "Sorel Etrog" :type "artist" :db/src ["artists.yaml" 1]}
            {:name "Sculpture 1" :artist-ids ["eloulk"] :type "sculpture" :db/src ["sculptures.yaml" 0]}
            {:name "Sculpture 2" :artist-ids ["eloulk" "etrogs"] :type "sculpture" :db/src ["sculptures.yaml" 1]}]
           (io/read-data "./resources/test_data")))))

(deftest read-schema
  (testing "reading schema from file"
    (is (= {:relationships [["sculpture", "artist-ids", "artist"]]}
           (io/read-schema "./resources/test_data")))))

(deftest annonate-with-src
  (testing "annotates simple case"
    (is (= {:db/src [:a :b :c] :id "foo"}
           (io/annotate-with-src {:id "foo"} [:a :b :c]))))

  (testing "annotates embedded"
    (is (= {:db/src [:a]
            :id "foo"
            :embed {:db/src [:a :embed]
                    :id "bar"}}
           (io/annotate-with-src {:id "foo"
                                  :embed {:id "bar"}}
             [:a]))))

  (testing "annonates array embedded"
    (is (= {:db/src [:a]
            :id "foo"
            :embeds [{:db/src [:a :embeds 0]
                      :id "bar"}
                     {:db/src [:a :embeds 1]
                      :id "baz"}]}
           (io/annotate-with-src {:id "foo"
                                  :embeds [{:id "bar"}
                                           {:id "baz"}]}
             [:a])))))

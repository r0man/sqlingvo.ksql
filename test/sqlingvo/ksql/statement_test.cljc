(ns sqlingvo.ksql.statement-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [deftest are is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [sqlingvo.ksql.compiler :as compiler]
            [sqlingvo.ksql.core :as sql]
            [sqlingvo.ksql.db :as db]))

(def db
  (sql/db))

(deftest test-eval
  (is (= ["TERMINATE Q1"]
         (db/-eval db (sql/terminate db :q1) {})))
  (is (= ["TERMINATE Q1"]
         @(sql/terminate db :q1))))

(deftest test-to-string
  (are [statement expected] (= expected (str statement))

    (sql/terminate db :q1)
    "TERMINATE Q1"

    (sql/select db [1 2 3])
    "SELECT 1, 2, 3"))

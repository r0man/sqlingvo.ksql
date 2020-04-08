(ns sqlingvo.ksql.test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [deftest is]]
            [clojure.test.check]
            [clojure.test.check.clojure-test]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties]
            [sqlingvo.ksql.compiler :as compiler]
            [sqlingvo.ksql.core :as sql]))

(stest/instrument)

(def gen-table
  (gen/elements [:table-1 :table_2]))

(def gen-select-items
  (gen/vector (gen/elements [:* :column-1 :column_2]) 1 10))

(ns sqlingvo.ksql.core-test
  (:require [clojure.test :refer [deftest is]]
            [sqlingvo.ksql.core :as sql]))

(def db (sql/db))

(deftest test-auto-offset-rest
  (let [db (sql/auto-offset-rest db :earliest)]
    (is (= {"sqlingvo.ksql.streams.auto.offset.reset" "earliest"} (:streams-properties db)))))

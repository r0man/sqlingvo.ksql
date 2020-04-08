(ns sqlingvo.ksql.db-test
  (:require [clojure.test :refer [deftest is]]
            [sqlingvo.ksql.db :as db]))

(deftest test-db
  (is (map? (db/db))))

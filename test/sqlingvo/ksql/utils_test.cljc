(ns sqlingvo.ksql.utils-test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [deftest is]]
            [sqlingvo.ksql.utils :as utils]))

(stest/instrument)

(def schema-str
  "`ROWTIME` BIGINT, `ROWKEY` STRING, `PROFILE_ID` STRING, `LATITUDE` DOUBLE, `LONGITUDE` DOUBLE")

(deftest test-parse-schema
  (is (= {:column-by-id
          {:rowtime {:name :rowtime, :type :bigint},
           :rowkey {:name :rowkey, :type :string},
           :profile-id {:name :profile-id, :type :string},
           :latitude {:name :latitude, :type :double},
           :longitude {:name :longitude, :type :double}},
          :column-names [:rowtime :rowkey :profile-id :latitude :longitude]}
         (utils/parse-schema schema-str))))

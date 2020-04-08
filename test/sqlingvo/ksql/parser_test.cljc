(ns sqlingvo.ksql.parser-test
  (:require [clojure.pprint :refer [pprint]]
            [clojure.spec.test.alpha :as stest :include-macros true]
            [clojure.test :refer [deftest is]]
            [sqlingvo.ksql.parser :as parser]
            [sqlingvo.ksql.test]))

(deftest test-check
  (let [results (stest/check
                 `[parser/aliased-relation
                   parser/array-type
                   parser/base-type
                   parser/create-sink-connector
                   parser/create-source-connector
                   parser/create-type
                   parser/drop-connector
                   parser/drop-stream
                   parser/drop-table
                   parser/emit
                   parser/expression
                   parser/function-call
                   parser/group-by
                   parser/having
                   parser/identifier
                   parser/if-exists
                   parser/insert
                   parser/insert-columns
                   parser/join
                   parser/join-type
                   parser/limit
                   parser/list
                   parser/literal
                   parser/map-type
                   parser/order-by
                   parser/partition-by
                   parser/query
                   parser/relation-primary
                   parser/result-materialization
                   parser/select-item
                   parser/select-items
                   parser/source-name
                   parser/struct-field
                   parser/struct-type
                   parser/table-element
                   parser/terminate
                   parser/value
                   parser/values
                   parser/where
                   parser/window
                   parser/window-expression
                   parser/window-type
                   parser/with]
                 {:clojure.spec.test.check/opts {:num-tests 20}})
        {:keys [check-failed check-threw] :as summary} (stest/summarize-results results)]
    (is (not check-failed))
    (is (not check-threw))
    (when (or check-failed check-threw)
      (pprint summary))))

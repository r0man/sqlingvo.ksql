(ns sqlingvo.ksql.test.runner
  (:require [clojure.spec.test.alpha :as stest]
            [doo.runner :refer-macros [doo-tests]]
            sqlingvo.ksql.compiler-test
            sqlingvo.ksql.core-test
            sqlingvo.ksql.parser-test
            sqlingvo.ksql.statement-test
            sqlingvo.ksql.utils-test))

(stest/instrument)

(doo-tests
 'sqlingvo.ksql.compiler-test
 'sqlingvo.ksql.core-test
 'sqlingvo.ksql.parser-test
 'sqlingvo.ksql.statement-test
 'sqlingvo.ksql.utils-test)

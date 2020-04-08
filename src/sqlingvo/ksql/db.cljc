(ns sqlingvo.ksql.db
  (:require [sqlingvo.ksql.compiler :as compiler]))

(defprotocol IEval
  (-eval [evaluator statement opts]
    "Evaluate the `statement` via `evaluator` using `opts`."))

(defrecord DB []
  IEval
  (-eval [evaluator statement opts]
    (compiler/sql statement)))

(defn db
  "Return a new KSQL db."
  [& [opts]]
  (map->DB opts))

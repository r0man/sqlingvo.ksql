(ns sqlingvo.ksql.antlr
  (:require [clj-antlr.core :as antlr]
            [clojure.java.io :as io]))

(def parse
  (antlr/parser (slurp (io/resource "ksql.g4"))))

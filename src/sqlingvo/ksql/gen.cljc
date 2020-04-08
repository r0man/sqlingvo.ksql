(ns sqlingvo.ksql.gen
  (:require [clojure.string :as str]
            [clojure.test.check.generators :as gen]))

(def char-identifier
  "Generates identifier characters."
  (gen/fmap char (gen/one-of [(gen/choose 65 90)
                              (gen/choose 97 122)
                              (gen/elements [\- \_])])))

(def string-identifier
  "Generates identifier strings."
  (gen/fmap #(str (first %) (str/join (second %)))
            (gen/tuple gen/char-alpha (gen/vector char-identifier))))

(def keyword-identifier
  "Generates identifier keyword."
  (gen/fmap keyword string-identifier))

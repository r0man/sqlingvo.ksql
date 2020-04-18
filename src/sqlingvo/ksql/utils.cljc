(ns sqlingvo.ksql.utils
  (:require [cats.context :as ctx]
            [clojure.spec.alpha :as s]
            [cats.monad.state :as state]
            [clojure.string :as str]))

(defn run-statement
  "Run `statement` in the context of the state monad."
  [statement]
  (seq (state/run (ctx/with-context state/context statement) {})))

(defn ast
  "Return the AST of the `statement`."
  [statement]
  (last (run-statement statement)))

(defn- parse-identifier
  "Parse the identifier from the KSQL header `s`."
  [s]
  (-> (str/lower-case s)
      (str/replace "_" "-")
      (str/replace "`" "")
      (keyword)))

(s/fdef parse-identifier
  :args (s/cat :s string?)
  :ret keyword?)

(defn- parse-attribute
  "Parse the attribute from the KSQL header `s`."
  [s]
  (let [[column-name column-type] (map parse-identifier (str/split s #"\s+"))]
    {:name column-name
     :type column-type}))

(s/fdef parse-attribute
  :args (s/cat :s string?)
  :ret (s/map-of keyword? keyword?))

(defn parse-schema
  "Parse the schema from the KSQL header `s`."
  [s]
  (let [columns (map parse-attribute (str/split s #"\s*,\s*"))
        column-names (mapv :name columns)]
    {:column-by-id (zipmap column-names columns)
     :column-names column-names}))

(s/fdef parse-schema
  :args (s/cat :s string?)
  :ret (s/map-of keyword? keyword?))

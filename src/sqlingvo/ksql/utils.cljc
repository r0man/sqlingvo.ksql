(ns sqlingvo.ksql.utils
  (:require #?(:clj [cheshire.core :as json])
            [cats.context :as ctx]
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
  "Parse the identifier from the SQLINGVO.KSQL header `s`."
  [s]
  (-> (str/lower-case s)
      (str/replace "_" "-")
      (str/replace "`" "")
      (keyword)))

(s/fdef parse-identifier
  :args (s/cat :s string?)
  :ret keyword?)

(defn- parse-attribute
  "Parse the attribute from the SQLINGVO.KSQL header `s`."
  [s]
  (let [[column-name column-type] (map parse-identifier (str/split s #"\s+"))]
    {:name column-name
     :type column-type}))

(s/fdef parse-attribute
  :args (s/cat :s string?)
  :ret (s/map-of keyword? keyword?))

(defn parse-schema
  "Parse the schema from the SQLINGVO.KSQL header `s`."
  [s]
  (let [columns (map parse-attribute (str/split s #"\s*,\s*"))
        column-names (mapv :name columns)]
    {:column-by-id (zipmap column-names columns)
     :column-names column-names}))

(s/fdef parse-schema
  :args (s/cat :s string?)
  :ret (s/map-of keyword? keyword?))

(defn parse-json [s]
  (try #?(:clj (json/parse-string s keyword)
          :cljs (js->clj (js/JSON.parse s) :keywordize-keys true))
       (catch #?(:clj Exception :cljs js/Error) e
         (throw (ex-info (str "Can't parse JSON: " s) {:s s} e)))))

(s/fdef parse-json
  :args (s/cat :s string?))

(defn json-str [x]
  (try #?(:clj (json/generate-string x)
          :cljs (js/JSON.stringify (clj->js x)))))

(s/fdef json-str
  :args (s/cat :x any?)
  :ret string?)

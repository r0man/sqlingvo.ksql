(ns sqlingvo.ksql.antlr-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer :all]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [sqlingvo.ksql.antlr :refer [parse]]
            [sqlingvo.ksql.compiler :as compiler]
            [sqlingvo.ksql.core :as sql]
            [sqlingvo.ksql.test :refer :all]))

(def db
  (sql/db {:type :sqlingvo.ksql.evaluator/sql}))

(deftest test-select-literal-number
  (is (= '(:statements
           (:singleStatement
            (:statement
             (:query
              "SELECT"
              (:selectItem
               (:expression
                (:booleanExpression
                 (:predicated
                  (:valueExpression
                   (:primaryExpression (:literal (:number "1"))))))))
              "FROM"
              (:relation
               (:aliasedRelation
                (:relationPrimary (:sourceName (:identifier "S1")))))))
            ";")
           "<EOF>")
         (parse "SELECT 1 FROM S1;"))))

(deftest test-select-expression-plus
  (is (= '(:statements
           (:singleStatement
            (:statement
             (:query
              "SELECT"
              (:selectItem
               (:expression
                (:booleanExpression
                 (:predicated
                  (:valueExpression
                   (:valueExpression (:primaryExpression (:identifier "A")))
                   "+"
                   (:valueExpression
                    (:primaryExpression (:literal (:number "2")))))))))
              "FROM"
              (:relation
               (:aliasedRelation
                (:relationPrimary (:sourceName (:identifier "S1")))))))
            ";")
           "<EOF>")
         (parse "SELECT A + 2 FROM S1;"))))

(deftest test-select-columns
  (is (= '(:statements
           (:singleStatement
            (:statement
             (:query
              "SELECT"
              (:selectItem
               (:expression
                (:booleanExpression
                 (:predicated
                  (:valueExpression
                   (:primaryExpression (:identifier "FIRST")))))))
              ","
              (:selectItem
               (:expression
                (:booleanExpression
                 (:predicated
                  (:valueExpression
                   (:primaryExpression (:identifier "LAST")))))))
              "FROM"
              (:relation
               (:aliasedRelation
                (:relationPrimary (:sourceName (:identifier "USERS")))))))
            ";")
           "<EOF>")
         (parse "SELECT FIRST, LAST FROM USERS;"))))

(deftest test-select-all
  (is (= '(:statements
           (:singleStatement
            (:statement
             (:query
              "SELECT"
              (:selectItem "*")
              "FROM"
              (:relation
               (:aliasedRelation
                (:relationPrimary
                 (:sourceName (:identifier "PAGEVIEWS_ORIGINAL")))))
              "WHERE"
              (:booleanExpression
               (:predicated
                (:valueExpression (:primaryExpression (:identifier "PAGEID")))
                (:predicate
                 (:comparisonOperator "=")
                 (:valueExpression (:primaryExpression (:literal "'home'"))))))))
            ";")
           "<EOF>")
         (parse "SELECT * FROM PAGEVIEWS_ORIGINAL WHERE PAGEID = 'home';"))))

(deftest test-create-stream
  (is (= '(:statements
           (:singleStatement
            (:statement
             "CREATE"
             "STREAM"
             (:sourceName (:identifier "PAGEVIEWS_HOME"))
             "AS"
             (:query
              "SELECT"
              (:selectItem "*")
              "FROM"
              (:relation
               (:aliasedRelation
                (:relationPrimary
                 (:sourceName (:identifier "PAGEVIEWS_ORIGINAL")))))
              "WHERE"
              (:booleanExpression
               (:predicated
                (:valueExpression (:primaryExpression (:identifier "PAGEID")))
                (:predicate
                 (:comparisonOperator "=")
                 (:valueExpression (:primaryExpression (:literal "'home'"))))))))
            ";")
           "<EOF>")
         (parse "CREATE STREAM PAGEVIEWS_HOME AS SELECT * FROM PAGEVIEWS_ORIGINAL WHERE PAGEID = 'home';"))))

(deftest test-create-stream-from-topic
  (is (= '(:statements
           (:singleStatement
            (:statement
             "CREATE"
             "STREAM"
             (:sourceName (:identifier "PAGEVIEWS_ORIGINAL"))
             (:tableElements
              "("
              (:tableElement
               (:identifier "VIEWTIME")
               (:type (:baseType (:identifier "BIGINT"))))
              ","
              (:tableElement
               (:identifier "USERID")
               (:type (:baseType (:identifier "VARCHAR"))))
              ","
              (:tableElement
               (:identifier "PAGEID")
               (:type (:baseType (:identifier "VARCHAR"))))
              ")")
             "WITH"
             (:tableProperties
              "("
              (:tableProperty
               (:identifier "KAFKA_TOPIC")
               "="
               (:literal "'pageviews'"))
              ","
              (:tableProperty
               (:identifier "VALUE_FORMAT")
               "="
               (:literal "'DELIMITED'"))
              ")"))
            ";")
           "<EOF>")
         (parse "CREATE STREAM PAGEVIEWS_ORIGINAL (VIEWTIME BIGINT, USERID VARCHAR, PAGEID VARCHAR) WITH (KAFKA_TOPIC = 'pageviews', VALUE_FORMAT = 'DELIMITED');"))))

(deftest test-show-topics
  (is (= '(:statements
           (:singleStatement (:statement "SHOW" "TOPICS") ";")
           "<EOF>")
         (parse "SHOW TOPICS;"))))

(deftest test-print-users
  (is (= '(:statements
           (:singleStatement (:statement "PRINT" "'users'" (:printClause)) ";")
           "<EOF>")
         (parse "PRINT 'users';"))))

(defspec check-parse
  (prop/for-all [table gen-table
                 select-items gen-select-items
                 limit gen/nat]
    (parse (str (first @(sql/select db select-items
                          (sql/from table)
                          (sql/limit limit)))
                ";"))))

;; (defspec check-insert 1
;;   (prop/for-all [ast (s/gen (s/keys :req-un [:sqlingvo.ksql.ast.insert/op
;;                                              :sqlingvo.ksql.ast.insert/children
;;                                              :sqlingvo.ksql.ast.insert/table
;;                                              :sqlingvo.ksql.ast.insert/columns
;;                                              :sqlingvo.ksql.ast/values]))]
;;     (parse (str (compiler/compile-sql db ast) ";"))))

(defspec check-terminate 5
  (prop/for-all [ast (s/gen :sqlingvo.ksql.ast/terminate)]
    (parse (str (compiler/compile-sql db ast) ";"))))

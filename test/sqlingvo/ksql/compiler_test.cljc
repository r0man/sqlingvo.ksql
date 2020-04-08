(ns sqlingvo.ksql.compiler-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [sqlingvo.ksql.compiler :as compiler]
            [sqlingvo.ksql.core :as sql]))

(def db
  (sql/db))

(stest/instrument)

(deftest test-ast-meta
  (let [result (sql/sql (sql/show db :tables))
        ast (:sqlingvo.ksql/ast (meta result))]
    (is (= result (compiler/sql ast)))))

;; Create sink connector

(deftest test-create-sink-connector
  (is (= ["CREATE SINK CONNECTOR JDBC_SINK WITH ('connector.class' = 'io.confluent.connect.jdbc.JdbcSinkConnector')"]
         (sql/sql (sql/create-sink-connector db :jdbc-sink
                    (sql/with {"connector.class" "io.confluent.connect.jdbc.JdbcSinkConnector"}))))))

;; Create source connector

(deftest test-create-source-connector
  (is (= ["CREATE SOURCE CONNECTOR JDBC_SOURCE WITH ('connector.class' = 'io.confluent.connect.jdbc.JdbcSourceConnector')"]
         (sql/sql (sql/create-source-connector db :jdbc-source
                    (sql/with {"connector.class" "io.confluent.connect.jdbc.JdbcSourceConnector"}))))))

;; Create stream

(deftest test-create-stream
  (is (= [(str "CREATE STREAM PAGEVIEWS_HOME AS"
               " SELECT * FROM PAGEVIEWS_ORIGINAL WHERE PAGEID = 'home'")]
         (sql/sql (sql/create-stream db :pageviews-home
                    (sql/as (sql/select db [:*]
                              (sql/from :pageviews-original)
                              (sql/where '(= :pageid "home")))))))))

(deftest test-create-stream-from-topic
  (is (= [(str "CREATE STREAM PAGEVIEWS_ORIGINAL"
               " (VIEWTIME BIGINT, USERID VARCHAR, PAGEID VARCHAR)"
               " WITH (KAFKA_TOPIC = 'pageviews', VALUE_FORMAT = 'DELIMITED')")]
         (sql/sql (sql/create-stream db :pageviews-original
                    [[:viewtime :bigint]
                     [:userid :varchar]
                     [:pageid :varchar]]
                    (sql/with {:kafka-topic "pageviews"
                               :value-format "DELIMITED"}))))))

(deftest test-create-a-stream-over-an-existing-kafka-topic
  (is (= ["CREATE STREAM S1 (C1 VARCHAR, C2 INTEGER) WITH (KAFKA_TOPIC = 's1', VALUE_FORMAT = 'json')"]
         (sql/sql (sql/create-stream db :s1
                    [[:c1 :varchar]
                     [:c2 :integer]]
                    (sql/with {:kafka-topic "s1"
                               :value-format "json"}))))))

(deftest test-create-a-stream-or-table-with-a-specific-key
  (is (= ["CREATE STREAM KEYED (ID VARCHAR, C2 INTEGER) WITH (KAFKA_TOPIC = 'keyed', KEY = 'id', VALUE_FORMAT = 'json')"]
         (sql/sql (sql/create-stream db :keyed
                    [[:id :varchar]
                     [:c2 :integer]]
                    (sql/with {:kafka-topic "keyed"
                               :key "id"
                               :value-format "json"}))))))

(deftest test-create-a-derived-stream-from-another-stream
  (is (= ["CREATE STREAM DERIVED AS SELECT A + 1 AS X, B AS Y FROM S1 EMIT CHANGES"]
         (sql/sql (sql/create-stream db :derived
                    (sql/as (sql/select db [(sql/as '(+ :a 1) :x) (sql/as :b :y)]
                              (sql/from :s1)
                              (sql/emit :changes))))))))

(deftest test-create-stream-struct
  (is (= [(str "CREATE STREAM T (TYPE VARCHAR, DATA STRUCT<TIMESTAMP VARCHAR, \"field-a\" INT, "
               "\"field-b\" VARCHAR, \"field-c\" INT, \"field-d\" VARCHAR>) "
               "WITH (KAFKA_TOPIC = 'raw-topic', VALUE_FORMAT = 'JSON')")]
         (sql/sql (sql/create-stream db :t
                    [[:type :varchar]
                     [:data [:struct {:timestamp :varchar
                                      "field-a" :int
                                      "field-b" :varchar
                                      "field-c" :int
                                      "field-d" :varchar}]]]
                    (sql/with {:kafka-topic "raw-topic"
                               :value-format "JSON"}))))))

;; Create table

(deftest test-create-a-table-over-an-existing-kafka-topic
  (is (= ["CREATE TABLE T1 (C1 VARCHAR, C2 INTEGER) WITH (KAFKA_TOPIC = 't1', VALUE_FORMAT = 'json')"]
         (sql/sql (sql/create-table db :t1
                    [[:c1 :varchar]
                     [:c2 :integer]]
                    (sql/with {:kafka-topic "t1"
                               :value-format "json"}))))))

(deftest test-create-table-array-map
  (is (= [(str "CREATE TABLE USERS (REGISTERTIME BIGINT, USERID VARCHAR, GENDER VARCHAR, "
               "REGIONID VARCHAR, INTERESTS ARRAY<STRING>, CONTACTINFO MAP<STRING, STRING>) "
               "WITH (KAFKA_TOPIC = 'users', KEY = 'userid', VALUE_FORMAT = 'JSON')")]
         (sql/sql (sql/create-table db :users
                    [[:registertime :bigint]
                     [:userid :varchar]
                     [:gender :varchar]
                     [:regionid :varchar]
                     [:interests [:string]]
                     [:contactinfo {:string :string}]]
                    (sql/with {:kafka-topic "users"
                               :key "userid"
                               :value-format "JSON"}))))))

;; Create Type

(deftest test-create-type-address
  (is (= ["CREATE TYPE ADDRESS AS STRUCT<NUMBER INTEGER, STREET VARCHAR, CITY VARCHAR>"]
         (sql/sql (sql/create-type db :address
                    [:struct {:number :integer
                              :street :varchar
                              :city :varchar}])))))

(deftest test-create-type-person
  (is (= ["CREATE TYPE PERSON AS STRUCT<NAME VARCHAR, ADDRESS ADDRESS>"]
         (sql/sql (sql/create-type db :person
                    [:struct {:name :varchar
                              :address :address}])))))

;; Drop connector

(deftest test-drop-connector
  (is (= ["DROP CONNECTOR C1"] (sql/sql (sql/drop-connector db :c1)))))

;; Drop stream

(deftest test-drop-stream
  (is (= ["DROP STREAM S1"] (sql/sql (sql/drop-stream db :s1)))))

(deftest test-drop-stream-if-exists
  (is (= ["DROP STREAM IF EXISTS S1"]
         (sql/sql (sql/drop-stream db :s1
                    (sql/if-exists)))))
  (is (= ["DROP STREAM IF EXISTS S1"]
         (sql/sql (sql/drop-stream db :s1
                    (sql/if-exists true)))))
  (is (= ["DROP STREAM S1"]
         (sql/sql (sql/drop-stream db :s1
                    (sql/if-exists false))))))

(deftest test-drop-stream-delete-topic
  (is (= ["DROP STREAM S1 DELETE TOPIC"]
         (sql/sql (sql/drop-stream db :s1
                    (sql/delete-topic)))))
  (is (= ["DROP STREAM S1 DELETE TOPIC"]
         (sql/sql (sql/drop-stream db :s1
                    (sql/delete-topic true)))))
  (is (= ["DROP STREAM S1"]
         (sql/sql (sql/drop-stream db :s1
                    (sql/delete-topic false))))))

;; Drop table

(deftest test-drop-table
  (is (= ["DROP TABLE T1"]
         (sql/sql (sql/drop-table db :t1)))))

(deftest test-drop-table-if-exists
  (is (= ["DROP TABLE IF EXISTS T1"]
         (sql/sql (sql/drop-table db :t1
                    (sql/if-exists)))))
  (is (= ["DROP TABLE IF EXISTS T1"]
         (sql/sql (sql/drop-table db :t1
                    (sql/if-exists true)))))
  (is (= ["DROP TABLE T1"]
         (sql/sql (sql/drop-table db :t1
                    (sql/if-exists false))))))

(deftest test-drop-table-delete-topic
  (is (= ["DROP TABLE T1 DELETE TOPIC"]
         (sql/sql (sql/drop-table db :t1
                    (sql/delete-topic)))))
  (is (= ["DROP TABLE T1 DELETE TOPIC"]
         (sql/sql (sql/drop-table db :t1
                    (sql/delete-topic true)))))
  (is (= ["DROP TABLE T1"]
         (sql/sql (sql/drop-table db :t1
                    (sql/delete-topic false))))))

;; Drop type

(deftest test-drop-type
  (is (= ["DROP TYPE ADDRESS"] (sql/sql (sql/drop-type db :address)))))

;;; Insert

(deftest test-insert-values-as-map
  (is (= ["INSERT INTO FOO (KEY_COL) VALUES ('key')"]
         (sql/sql (sql/insert db :foo [:key-col]
                    (sql/values [{:key-col "key"}]))))))

(deftest test-insert-values-as-seq
  (is (= ["INSERT INTO FOO (KEY_COL) VALUES ('key')"]
         (sql/sql (sql/insert db :foo [:key-col]
                    (sql/values [["key"]]))))))

(deftest test-insert-multiple-columns-1
  (is (= ["INSERT INTO FOO (KEY_COL, COL_A) VALUES ('key', 'A')"]
         (sql/sql (sql/insert db :foo [:key-col :col-a]
                    (sql/values [["key" "A"]]))))))

(deftest test-insert-multiple-columns-2
  (is (= ["INSERT INTO FOO (ROWTIME, ROWKEY, KEY_COL, COL_A) VALUES (1234, 'key', 'key', 'A')"]
         (sql/sql (sql/insert db :foo [:rowtime :rowkey :key-col :col-a]
                    (sql/values [[1234 "key" "key" "A"]]))))))

(deftest test-list-streams
  (is (= ["LIST STREAMS"]
         (sql/sql (sql/list db :streams)))))

;; Print

(deftest test-print-users
  (is (= ["PRINT 'users'"]
         (sql/sql (sql/print db :users)))))

;; Select

(deftest test-select
  (is (= ["SELECT * FROM PAGEVIEWS_ORIGINAL WHERE PAGEID = 'home'"]
         (sql/sql (sql/select db [:*]
                    (sql/from :pageviews-original)
                    (sql/where '(= :pageid "home")))))))

(deftest test-select-all-rows-and-columns
  (is (= ["SELECT * FROM S1 EMIT CHANGES"]
         (sql/sql (sql/select db [:*]
                    (sql/from :s1)
                    (sql/emit :changes))))))

(deftest test-select-a-subset-of-columns
  (is (= ["SELECT C1, C2, C3 FROM S1 EMIT CHANGES"]
         (sql/sql (sql/select db [:c1 :c2 :c3]
                    (sql/from :s1)
                    (sql/emit :changes))))))

(deftest test-select-filter-rows
  (is (= ["SELECT * FROM S1 WHERE C1 != 'foo' AND C2 = 42 EMIT CHANGES"]
         (sql/sql (sql/select db [:*]
                    (sql/from :s1)
                    (sql/where '(and (!= :c1 "foo") (= :c2 42)))
                    (sql/emit :changes))))))

(deftest test-select-apply-a-function-to-columns
  (is (= ["SELECT SUBSTRING(STR, 1, 10) FROM S1 EMIT CHANGES"]
         (sql/sql (sql/select db ['(substring :str 1 10)]
                    (sql/from :s1)
                    (sql/emit :changes))))))

(deftest test-select-null-or-non-null-columns
  (is (= ["SELECT * FROM S1 WHERE C1 IS NOT NULL OR C2 IS NULL EMIT CHANGES"]
         (sql/sql (sql/select db [:*]
                    (sql/from :s1)
                    (sql/where '(or (is-not-null :c1) (is-null :c2)))
                    (sql/emit :changes))))))

(deftest test-select-timestamp-comparison
  (is (= ["SELECT * FROM S1 WHERE ROWTIME >= '2019-11-20T00:00:00' EMIT CHANGES"]
         (sql/sql (sql/select db [:*]
                    (sql/from :s1)
                    (sql/where '(>= :rowtime "2019-11-20T00:00:00"))
                    (sql/emit :changes))))))

(deftest test-select-timestamp-comparison-epoch
  (is (= ["SELECT * FROM S1 WHERE ROWTIME >= 1574208000000 EMIT CHANGES"]
         (sql/sql (sql/select db [:*]
                    (sql/from :s1)
                    (sql/where '(>= :rowtime 1574208000000))
                    (sql/emit :changes))))))

(deftest test-select-format-a-epoch-milliseconds-timestamp-as-a-string
  (is (= ["SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss') FROM S1 EMIT CHANGES"]
         (sql/sql (sql/select db ['(timestamptostring :rowtime "yyyy-MM-dd HH:mm:ss")]
                    (sql/from :s1)
                    (sql/emit :changes))))))

(deftest test-select-parse-a-date-string-using-the-given-format
  (is (= ["SELECT STRINGTOTIMESTAMP(DATESTR, 'yyyy-MM-dd HH:mm:ss') FROM S1 EMIT CHANGES"]
         (sql/sql (sql/select db ['(stringtotimestamp :datestr "yyyy-MM-dd HH:mm:ss")]
                    (sql/from :s1)
                    (sql/emit :changes))))))

(deftest test-select-alias
  (is (= ["SELECT A + 1 AS X FROM S1"]
         (sql/sql (sql/select db [(sql/as '(+ :a 1) :x)]
                    (sql/from :s1))))))

(deftest test-select-arithmetic-binary
  (is (= ["SELECT A + 1 + 2 + 3, B - 4 - 5 - 6 FROM S1"]
         (sql/sql (sql/select db ['(+ :a 1 2 3) '(- :b 4 5 6)]
                    (sql/from :s1))))))

(deftest test-select-columns
  (is (= ["SELECT FIRST, LAST FROM USERS"]
         (sql/sql (sql/select db [:first :last]
                    (sql/from :users))))))

(deftest test-select-literal-number
  (is (= ["SELECT 1 FROM S1"]
         (sql/sql (sql/select db [1]
                    (sql/from :s1))))))

(deftest test-select-struct
  (is (= ["SELECT ADDRESS->CITY, ADDRESS->ZIP FROM ORDERS"]
         (sql/sql (sql/select db ['(-> :address :city)
                                  '(-> :address :zip)]
                    (sql/from :orders))))))

(deftest test-select-struct-string-fields
  (is (= [(str "SELECT DATA->\"field-a\", DATA->\"field-c\", DATA->\"field-d\" FROM T "
               "WHERE TYPE = 'key2' EMIT CHANGES LIMIT 2")]
         (sql/sql (sql/select db ['(-> :data "field-a")
                                  '(-> :data "field-c")
                                  '(-> :data "field-d")]
                    (sql/from :t)
                    (sql/where '(= :type "key2"))
                    (sql/limit 2)
                    (sql/emit :changes))))))

(deftest test-select-array-map-type
  (is (= [(str "SELECT INTERESTS[0] AS FIRST_INTEREST, CONTACTINFO['zipcode'] AS ZIPCODE, "
               "CONTACTINFO['city'] AS CITY, USERID, GENDER, REGIONID FROM USERS EMIT CHANGES")]
         (sql/sql (sql/select db [(sql/as '(get :interests 0) :first-interest)
                                  (sql/as '(get :contactinfo "zipcode") :zipcode)
                                  (sql/as '(get :contactinfo "city") :city)
                                  :userid :gender :regionid
                                  ]
                    (sql/from :users)
                    (sql/emit :changes))))))

(deftest test-select-join
  (is (= [(str "SELECT PV.VIEWTIME, PV.USERID AS USERID, PV.PAGEID, PV.TIMESTRING, U.GENDER, "
               "U.REGIONID, U.INTERESTS, U.CONTACTINFO FROM PAGEVIEWS_TRANSFORMED AS PV "
               "LEFT JOIN USERS_5PART AS U ON PV.USERID = U.USERID")]
         (sql/sql (sql/select db [:pv.viewtime
                                  (sql/as :pv.userid :userid)
                                  :pv.pageid
                                  :pv.timestring
                                  :u.gender
                                  :u.regionid
                                  :u.interests
                                  :u.contactinfo]
                    (sql/from (sql/as :pageviews-transformed :pv))
                    (sql/join (sql/as :users-5part :u) '(= :pv.userid :u.userid) :left))))))

(deftest test-select-having
  (is (= [(str "SELECT CARD_NUMBER, COUNT(*) "
               "FROM AUTHORIZATION_ATTEMPTS "
               "WINDOW TUMBLING (SIZE 5 SECONDS) "
               "GROUP BY CARD_NUMBER "
               "HAVING COUNT(*) > 3")]
         (sql/sql (sql/select db [:card-number '(count :*)]
                    (sql/from :authorization-attempts)
                    (sql/window :tumbling '(size 5 seconds))
                    (sql/group-by :card-number)
                    (sql/having '(> (count :*) 3)))))))

(deftest test-select-partition-by
  (is (= ["SELECT * FROM PRODUCTS PARTITION BY PRODUCT_ID"]
         (sql/sql (sql/select db [:*]
                    (sql/from :products)
                    (sql/partition-by :product-id))))))

(deftest test-select-window-session
  (is (= [(str "SELECT ITEM_ID, SUM(QUANTITY) FROM ORDERS WINDOW SESSION (20 SECONDS) GROUP BY ITEM_ID")]
         (sql/sql (sql/select db [:item-id '(sum :quantity)]
                    (sql/from :orders)
                    (sql/window :session '(20 seconds))
                    (sql/group-by :item-id))))))

(deftest test-select-window-tumbling
  (is (= [(str "SELECT PAGE_ID, CONCAT(CAST(COUNT(*) AS VARCHAR), '_HELLO') FROM PAGEVIEWS_ENRICHED "
               "WINDOW TUMBLING (SIZE 20 SECONDS) GROUP BY PAGE_ID")]
         (sql/sql (sql/select db [:page-id '(concat (cast (count :*) :varchar) "_HELLO")]
                    (sql/from :pageviews-enriched)
                    (sql/window :tumbling '(size 20 seconds))
                    (sql/group-by :page-id))))))

(deftest test-select-array-type
  (is (= ["SELECT ARRAY[A, B, 3] AS L FROM TEST"]
         (sql/sql (sql/select db [(sql/as '(array :a :b 3) :l)]
                    (sql/from :test))))))

(deftest test-select-map-type
  (is (= ["SELECT MAP(K1 := V1, K2 := 2 * V2) AS M FROM TEST"]
         (sql/sql (sql/select db [(sql/as '(map :k1 :v1 :k2 (* 2 :v2)) :m)]
                    (sql/from :test))))))

(deftest test-select-struct-type
  (is (= ["SELECT STRUCT(NAME := COL0, AGEINDOGYEARS := COL1 * 7) AS DOGS FROM ANIMALS"]
         (sql/sql (sql/select db [(sql/as '(struct :name :col0 :ageindogyears (* :col1 7)) :dogs)]
                    (sql/from :animals))))))

;; Show

(deftest test-show-streams
  (is (= ["SHOW STREAMS"]
         (sql/sql (sql/show db :streams)))))

(deftest test-show-tables
  (is (= ["SHOW TABLES"]
         (sql/sql (sql/show db :tables)))))

(deftest test-show-topics
  (is (= ["SHOW TOPICS"]
         (sql/sql (sql/show db :topics)))))

;; Terminate

(deftest test-terminate
  (is (= ["TERMINATE Q1"]
         (sql/sql (sql/terminate db :q1)))))

;; Check compilation

(defspec check-insert
  (prop/for-all [query (s/gen :sqlingvo.ksql.ast/insert)]
    (string? (compiler/compile-sql db query))))

(defspec check-terminate
  (prop/for-all [query (s/gen :sqlingvo.ksql.ast/terminate)]
    (string? (compiler/compile-sql db query))))

(defspec check-query
  (prop/for-all [query (s/gen :sqlingvo.ksql.ast/query)]
    (string? (compiler/compile-sql db query))))

(comment

  (gen/generate (s/gen :sqlingvo.ksql.ast/query))

  ;; TODO: Use vectors for alias

  (sql/select db [:pv.viewtime [:pv.userid :userid] :pv.pageid :pv.timestring
                  :u.gender :u.regionid :u.interests :u.contactinfo]
    (sql/from [:pageviews-transformed :pv])
    (sql/join [:users-5part :u] '(= :pv.userid :u.userid) :left))

  )

(ns sqlingvo.ksql.core
  (:refer-clojure :exclude [group-by list partition-by print])
  (:require [cats.context :as context]
            [cats.core :as m]
            [cats.monad.state :as state]
            [clojure.pprint :refer [pprint]]
            [clojure.spec.alpha :as s]
            [sqlingvo.ksql.compiler :as compiler]
            [sqlingvo.ksql.parser :as parser]
            [sqlingvo.ksql.statement :refer [map->Statement statement?]]
            [sqlingvo.ksql.db :as db]))

(s/def ::alias map?)
(s/def ::auto-offset-rest #{:earliest :latest :none})
(s/def ::body (s/* ::clause))
(s/def ::clause state/state?)
(s/def ::column ::parser/column)
(s/def ::columns ::parser/columns)
(s/def ::condition list?)
(s/def ::connector ::parser/identifier)
(s/def ::db ::parser/db)
(s/def ::expression ::parser/expression)
(s/def ::join-type #{:left :inner :right})
(s/def ::result-materialization #{:changes})
(s/def ::source (s/or :alias? ::alias :keyword ::parser/identifier))
(s/def ::statement statement?)
(s/def ::stream ::parser/identifier)
(s/def ::table ::parser/identifier)

(defn db
  "Returns a new db."
  [& [config]]
  (db/db config))

(defn- statement
  "Return a statement."
  [node & [body]]
  (map->Statement
   (m/mlet [_ (state/put node)
            _ (m/sequence (filter state/state? body))]
     (m/return node))))

(s/fdef statement
  :args (s/cat :node map? :body (s/* any?))
  :ret ::statement)

(defn- update-state [f & args]
  (m/mlet [state' (state/swap #(apply f % args))]
    (m/return state')))

(s/fdef update-state
  :args (s/cat :f ifn? :args (s/* any?))
  :ret state/state?)

(defn- assoc-node [node]
  (update-state assoc (get node :op) node))

(s/fdef assoc-node
  :args (s/cat :node map?)
  :ret state/state?)

(defn ast
  "Compile the `statement` to an AST."
  [statement]
  (last (seq (state/run (context/with-context state/context statement) {}))))

(s/fdef ast
  :args (s/cat :statement ::statement)
  :ret map?)

(defn auto-offset-rest [db value]
  (assoc-in db [:streams-properties "ksql.streams.auto.offset.reset"] (name value)))

(s/fdef auto-offset-rest
  :args (s/cat :db ::db :value ::auto-offset-rest)
  :ret ::db)

(defn pp
  "Compile the `statement` to an AST."
  [statement]
  (pprint (ast statement)))

(s/fdef pp
  :args (s/cat :statement ::statement))

(defn as
  ([alias]
   (assoc-node (parser/as (ast alias))))
  ([expr alias]
   (parser/as expr alias)))

(defn emit
  [result-materialization]
  (assoc-node (parser/emit result-materialization)))

(s/fdef emit
  :args (s/cat :result-materialization ::result-materialization)
  :ret ::clause)

(defn sql
  "Compile the `statement` to SQL string.."
  [statement]
  (compiler/sql statement))

(s/fdef sql
  :args (s/cat :statement ::statement)
  :ret string?)

(defn create-sink-connector
  {:style/indent 2}
  [db name & body]
  (statement (parser/create-sink-connector db name) body))

(s/fdef create-sink-connector
  :args (s/cat :db ::db :name ::connector :body ::body)
  :ret ::statement)

(defn create-source-connector
  {:style/indent 2}
  [db name & body]
  (statement (parser/create-source-connector db name) body))

(s/fdef create-source-connector
  :args (s/cat :db ::db :name ::connector :body ::body)
  :ret ::statement)

(defn create-stream
  {:style/indent 2}
  [db stream & body]
  (statement (parser/create-stream db stream body) body))

(s/fdef create-stream
  :args (s/alt :ary-2 (s/cat :db ::db :stream ::stream :body ::body)
               :ary-3 (s/cat :db ::db :stream ::stream :definition vector? :body ::body))
  :ret ::statement)

(defn create-table
  {:style/indent 2}
  [db stream & body]
  (statement (parser/create-table db stream body) body))

(s/fdef create-table
  :args (s/alt :ary-2 (s/cat :db ::db :table ::table :body ::body)
               :ary-3 (s/cat :db ::db :table ::table :definition vector? :body ::body))
  :ret ::statement)

(defn create-type
  {:style/indent 2}
  [db name type]
  (statement (parser/create-type db name type)))

(s/fdef create-type
  :args (s/cat :db ::db :name ::parser/identifier ::type ::parser/column-type)
  :ret ::statement)

(defn delete-topic
  ([]
   (delete-topic true))
  ([delete?]
   (assoc-node (parser/delete-topic delete?))))

(s/fdef delete-topic
  :args (s/alt :ary-0 (s/cat) :ary-1 (s/cat :delete? (s/nilable boolean?)))
  :ret ::clause)

(defn drop-connector
  {:style/indent 2}
  [db connector]
  (statement (parser/drop-connector db connector)))

(s/fdef drop-connector
  :args (s/cat :db ::db :name ::connector)
  :ret ::statement)

(defn drop-table
  {:style/indent 2}
  [db table & body]
  (statement (parser/drop-table db table) body))

(s/fdef drop-table
  :args (s/cat :db ::db :table ::table :body ::body)
  :ret ::statement)

(defn drop-type
  {:style/indent 2}
  [db name]
  (statement (parser/drop-type db name)))

(s/fdef drop-type
  :args (s/cat :db ::db :name ::parser/identifier)
  :ret ::statement)

(defn drop-stream
  {:style/indent 2}
  [db stream & body]
  (statement (parser/drop-stream db stream) body))

(s/fdef drop-stream
  :args (s/cat :db ::db :stream ::stream :body ::body)
  :ret ::statement)

(defn from [source]
  (assoc-node (parser/from source)))

(s/fdef from
  :args (s/cat :source ::source)
  :ret ::clause)

(defn group-by [& columns]
  (assoc-node (parser/group-by columns)))

(s/fdef group-by
  :args (s/cat :columns (s/* ::column))
  :ret ::clause)

(defn having [expr]
  (assoc-node (parser/having expr)))

(s/fdef having
  :args (s/cat :expr ::expression)
  :ret ::clause)

(defn join [source condition type]
  (assoc-node (parser/join source condition type)))

(s/fdef join
  :args (s/cat :source ::source :condition ::condition :type (s/nilable ::join-type))
  :ret ::clause)

(defn if-exists
  ([]
   (if-exists true))
  ([if-exists?]
   (assoc-node (parser/if-exists if-exists?))))

(s/fdef if-exists
  :args (s/alt :ary-0 (s/cat) :ary-1 (s/cat :if-exists? (s/nilable boolean?)))
  :ret ::clause)

(defn insert
  "Build a INSERT statement."
  {:style/indent 3}
  [db table columns & body]
  (statement (parser/insert db table columns) body))

(s/fdef insert
  :args (s/cat :db ::db :table ::table :columns ::columns :body ::body)
  :ret ::statement)

(defn order-by [& columns]
  (assoc-node (parser/order-by columns)))

(s/fdef order-by
  :args (s/cat :columns (s/* ::column))
  :ret ::clause)

(defn select
  {:style/indent 2}
  [db expr & body]
  (statement (parser/query db expr) body))

(s/fdef select
  :args (s/cat :db ::db :expr vector? :body ::body)
  :ret ::statement)

(defn show
  {:style/indent 2}
  [db type]
  (statement (parser/show db type)))

(s/fdef show
  :args (s/cat :db ::db :type simple-keyword?)
  :ret ::statement)

(defn limit [limit]
  (assoc-node (parser/limit limit)))

(s/fdef limit
  :args (s/cat :limit nat-int?)
  :ret ::clause)

(defn list
  {:style/indent 2}
  [db type]
  (statement (parser/list db type)))

(s/fdef list
  :args (s/cat :db ::db :type :sqlingvo.ksql.ast.list/type)
  :ret ::statement)

(defn partition-by
  [expr]
  (assoc-node (parser/partition-by expr)))

(s/fdef partition-by
  :args (s/cat :expr ::expression)
  :ret ::clause)

(defn print
  {:style/indent 2}
  [db clause]
  (statement (parser/print db clause)))

(s/fdef print
  :args (s/cat :db ::db :clause any?)
  :ret ::statement)

(defn terminate
  {:style/indent 2}
  [db query]
  (statement (parser/terminate db query)))

(s/fdef terminate
  :args (s/cat :db ::db :query simple-keyword?)
  :ret ::statement)

(defn values
  [values]
  (m/mlet [ast (state/get)
           :let [columns (map :identifier (-> ast :columns :elements))
                 node (parser/values columns values)]
           _ (state/swap #(assoc % :values node))]
    (m/return node)))

(defn where [expr]
  (assoc-node (parser/where expr)))

(s/fdef where
  :args (s/cat :expr ::expression)
  :ret ::clause)

(defn window [type & expressions]
  (assoc-node (parser/window type expressions)))

(s/fdef window
  :args (s/cat :type simple-keyword? :expressions (s/* ::parser/window-expression))
  :ret ::clause)

(defn with [opts]
  (assoc-node (parser/with opts)))

(s/fdef with
  :args (s/cat :opts map?)
  :ret ::clause)

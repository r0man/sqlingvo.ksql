(ns sqlingvo.ksql.parser
  (:refer-clojure :exclude [group-by list print partition-by])
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [sqlingvo.ksql.ast :as ast]
            [sqlingvo.ksql.compiler :as compiler]))

(declare expression)

(defn- array-type?
  "Returns true if `x` is an array type, otherwise false."
  [x]
  (and (vector? x) (= 1 (count x))))

(defn- compileable? [{:keys [ret]}]
  (let [result (compiler/compile-ast ret)]
    (and (vector? result)
         (string? (first result)))))

(defn- map-type?
  "Returns true if `x` is a map type, otherwise false."
  [type]
  (and (map? type) (= 1 (count type))))

(defn- struct-type?
  "Returns true if `x` is a struct type, otherwise false."
  [type]
  (and (vector? type) (= :struct (first type))))

(s/def ::array-type
  (s/coll-of simple-keyword? :count 1))

(s/def ::body (s/nilable sequential?))
(s/def ::boolean boolean?)
(s/def ::column simple-keyword?)
(s/def ::columns (s/coll-of ::column :min-count 1 :gen-max 3))
(s/def ::db map?)
(s/def ::identifier :sqlingvo.ksql.ast.identifier/identifier)
(s/def ::nil nil?)
(s/def ::number number?)
(s/def ::stream simple-keyword?)
(s/def ::string string?)
(s/def ::symbol symbol?)
(s/def ::table simple-keyword?)

(s/def ::literal
  (s/or :boolean ::boolean
        :nil ::nil
        :number ::number
        :string ::string))

(s/def ::expression
  (s/or :identifier ::identifier
        :literal ::literal
        :function-call ::function-call))

(s/def ::function-call
  (s/spec (s/cat :name symbol? :args (s/* ::expression))))

(s/def ::map-type :sqlingvo.ksql.ast.map-type/definition)

(s/def ::select-item
  (s/or :as ::ast/as :expression ::expression))

(s/def ::select-items
  (s/coll-of ::select-item :gen-max 3))

(s/def ::struct-field-name
  (s/or :keyword simple-keyword? :string string?))

(s/def ::struct-field-type simple-keyword?)

(s/def ::struct-field
  (s/tuple ::struct-field-name ::struct-field-type))

(s/def ::struct-fields
  (s/tuple #{:struct} (s/map-of ::struct-field-name ::struct-field-type)))

(s/def ::struct-type
  (s/spec ::struct-fields))

(s/def ::base-type simple-keyword?)

(s/def ::column-type
  (s/or :base-type ::base-type
        :array-type ::array-type
        :map-type ::map-type
        :struct-type ::struct-type))

(s/def ::table-element
  (s/tuple ::identifier ::column-type))

(s/def ::source
  (s/or :as ::ast/as :identifier ::identifier :query ::ast/query))

(s/def ::value :sqlingvo.ksql.ast.value/value)

(s/def ::values
  (s/coll-of ::value :gen-max 3 :min-count 1))

(s/def ::window-expression
  :sqlingvo.ksql.ast.window-expression/expression)

(s/def ::window-type
  :sqlingvo.ksql.ast.window-type/type)

(defn- node
  "Helper fn to create an AST node with :op and :children."
  [op & kws]
  (merge {:op op :children (ast/op->children op)} (apply hash-map kws)))

(s/fdef with-node
  :args (s/cat :op simple-keyword? :opts (s/map-of simple-keyword? any?))
  :ret map?)

(defn identifier
  "Returns the AST node for an identifier."
  [expr]
  (node :identifier :identifier expr))

(s/fdef identifier
  :args (s/cat :identifier ::identifier)
  :fn compileable?
  :ret ::ast/identifier)

(defn function-call
  "Returns the AST node for a function call."
  [expr]
  (node :function-call
        :name (first expr)
        :args (mapv expression (rest expr))))

(s/fdef function-call
  :args (s/cat :expr ::function-call)
  :ret ::ast/function-call)

(defn limit
  "Returns the AST node the LIMIT clause."
  [limit]
  (node :limit :limit limit))

(s/fdef limit
  :args (s/cat :limit :sqlingvo.ksql.ast.limit/limit)
  :ret ::ast/limit)

(defn literal
  "Returns the AST node a literal."
  [expr]
  (node :literal :literal expr))

(s/fdef literal
  :args (s/cat :literal :sqlingvo.ksql.ast.literal/literal)
  :ret ::ast/literal)

(defn list
  "Returns the AST node for a LIST statement."
  [db type]
  (node :list :db db :type type))

(s/fdef list
  :args (s/cat :db ::db :type :sqlingvo.ksql.ast.list/type)
  :fn compileable?
  :ret ::ast/list)

(defn expression
  "Returns the AST node for an expression."
  [expr]
  (node :expression
        :expression
        (cond
          (s/valid? ::identifier expr)
          (identifier expr)
          (s/valid? ::literal expr)
          (literal expr)
          (s/valid? ::function-call expr)
          (function-call expr))))

(s/fdef expression
  :args (s/cat :expr ::expression)
  :ret ::ast/expression)

(defn as
  "Returns the AST node for an AS clause."
  ([alias]
   (node :as :alias alias))
  ([expr alias]
   (node :as :expression (expression expr) :alias (identifier alias))))

(s/fdef as
  :args (s/alt :ary-1 (s/cat :alias ::ast/query)
               :ary-2 (s/cat :expr (s/or :expr ::expression :query ::ast/query) :alias ::identifier))
  :ret ::ast/as)

(defn result-materialization
  "Returns the AST node for the RESULT MATERIALIZATION clause."
  [result-materialization]
  (node :result-materialization :result-materialization result-materialization))

(s/fdef result-materialization
  :args (s/cat :result-materialization :sqlingvo.ksql.ast.result-materialization/result-materialization)
  :fn compileable?
  :ret ::ast/result-materialization)

(defn emit
  "Returns the AST node for the EMIT clause."
  [materialization]
  (node :emit :result-materialization (result-materialization materialization)))

(s/fdef emit
  :args (s/cat :materialization :sqlingvo.ksql.ast.result-materialization/result-materialization)
  :fn compileable?
  :ret ::ast/emit)

(defn delete-topic
  "Returns the AST node for the DELETE TOPIC clause."
  [delete-topic]
  (node :delete-topic :delete-topic (true? delete-topic)))

(s/fdef delete-topic
  :args (s/cat :delete-topic (s/nilable :sqlingvo.ksql.ast.delete-topic/delete-topic))
  :fn compileable?
  :ret ::ast/delete-topic)

(defn if-exists
  "Returns the AST node for an IF EXISTS clause."
  [if-exists]
  (node :if-exists :if-exists (true? if-exists)))

(s/fdef if-exists
  :args (s/cat :if-exists (s/nilable :sqlingvo.ksql.ast.if-exists/if-exists))
  :ret ::ast/if-exists)

(defn insert-columns
  "Returns the AST node for the insert columns."
  [columns]
  (node :insert-columns :elements (mapv identifier columns)))

(s/fdef insert-columns
  :args (s/cat :columns ::columns)
  :fn compileable?
  :ret ::ast/insert-columns)

(defn insert
  "Returns the AST node for an INSERT statement."
  [db table columns]
  (node :insert
        :db db
        :table (identifier table)
        :columns (insert-columns columns)))

(s/fdef insert
  :args (s/cat :db ::db :table ::table :columns ::columns)
  :fn compileable?
  :ret ::ast/insert)

(defn source-name
  "Returns the AST node for a source name."
  [source]
  (node :source-name
        :source (cond
                  (s/valid? ::identifier source)
                  (identifier source)
                  (map? source)
                  source)))

(s/fdef source-name
  :args (s/cat :source ::source)
  :ret ::ast/source-name)

(defn relation-primary
  "Returns the AST node for a primary relation."
  [relation-primary]
  (node :relation-primary :source-name (source-name relation-primary)))

(s/fdef relation-primary
  :args (s/cat :relation-primary ::source)
  :ret ::ast/relation-primary)

(defn aliased-relation
  "Returns the AST node for an aliased relation."
  [aliased-relation]
  (node :aliased-relation :relation-primary (relation-primary aliased-relation)))

(s/fdef aliased-relation
  :args (s/cat :aliased-relation ::source)
  :ret ::ast/aliased-relation)

(defn relation
  "Returns the AST node for a relation."
  [relation]
  (node :relation :aliased-relation (aliased-relation relation)))

(s/fdef relation
  :args (s/cat :relation ::source)
  :ret ::ast/relation)

(defn from
  "Returns the AST node for the FROM clause."
  [from]
  (node :from :relation (relation from)))

(s/fdef from
  :args (s/cat :from ::source)
  :ret ::ast/from)

(defn having
  "Returns the AST node for the HAVING clause."
  [expr]
  (node :having :expression (expression expr)))

(s/fdef having
  :args (s/cat :expr ::expression)
  :ret ::ast/having)

(defn where
  "Returns the AST node for the WHERE clause."
  [expr]
  (node :where :expression (expression expr)))

(s/fdef where
  :args (s/cat :expr ::expression)
  :ret ::ast/where)

(defn order-by
  "Returns the AST node for the ORDER BY clause."
  [columns]
  (node :order-by :columns columns))

(s/fdef order-by
  :args (s/cat :columns ::columns)
  :ret ::ast/order-by)

(defn select-item
  "Returns the AST node for a select item."
  [item]
  (node :select-item
        :expression
        (cond
          (and (map? item)
               (= :as (:op item)))
          item
          :else (expression item))))

(s/fdef select-item
  :args (s/cat :item ::select-item)
  :ret ::ast/select-item)

(defn select-items
  "Returns the AST node for select items."
  [items]
  (node :select-items :items (mapv select-item items)))

(s/fdef select-items
  :args (s/cat :items ::select-items)
  :ret ::ast/select-items)

(defn query
  "Returns the AST node for the SELECT statement."
  [db selection]
  (node :query :db db :select-items (select-items selection)))

(s/fdef query
  :args (s/cat :db ::db :selection ::select-items)
  :ret ::ast/query)

(defn show
  "Returns the AST node for the SHOW statement."
  [db type]
  (node :show :db db :type type))

(s/fdef show
  :args (s/cat :db ::db :type :sqlingvo.ksql.ast.show/type)
  :fn compileable?
  :ret ::ast/show)

(defn partition-by
  "Returns the AST node for the PARTITION BY clause."
  [expr]
  (node :partition-by :expression (expression expr)))

(s/fdef partition-by
  :args (s/cat :expr ::expression)
  :ret ::ast/partition-by)

(defn print
  "Returns the AST node for the PRINT statement."
  [db clause]
  (node :print :db db :clause clause))

(s/fdef print
  :args (s/cat :db ::db :clause :sqlingvo.ksql.ast.print/clause)
  :fn compileable?
  :ret ::ast/print)

(defn with
  "Returns the AST node for the WITH clause."
  [opts]
  (node :with :opts opts))

(s/fdef with
  :args (s/cat :opts :sqlingvo.ksql.ast.with/opts)
  :fn compileable?
  :ret ::ast/with)

(defn base-type
  "Returns the AST node for a base type."
  [base-type]
  (node :base-type :type (identifier base-type)))

(s/fdef base-type
  :args (s/cat :base-type ::identifier)
  :fn compileable?
  :ret ::ast/base-type)

(defn array-type
  "Returns the AST node for an array type."
  [[definition :as array-type]]
  (node :array-type :definition definition))

(s/fdef array-type
  :args (s/cat :array-type (s/coll-of :sqlingvo.ksql.ast.array-type/definition :count 1))
  :fn compileable?
  :ret ::ast/array-type)

(defn map-type
  "Returns the AST node for a map type."
  [definition]
  (node :map-type :definition definition))

(s/fdef map-type
  :args (s/cat :map-type :sqlingvo.ksql.ast.map-type/definition)
  :fn compileable?
  :ret ::ast/map-type)

(defn struct-field
  "Returns the AST node for a struct field."
  [[name type :as field]]
  (node :struct-field :name name :type type))

(s/fdef struct-field
  :args (s/cat :field ::struct-field)
  :fn compileable?
  :ret ::ast/struct-field)

(defn struct-type
  "Returns the AST node for a struct type."
  [[type fields :as struct-type]]
  (node :struct-type :fields (mapv struct-field fields)))

(s/fdef struct-type
  :args (s/cat :struct-type ::struct-type)
  :fn compileable?
  :ret ::ast/struct-type)

(defn column-type
  "Returns the AST node for a column type."
  [type]
  (cond
    (simple-keyword? type)
    (base-type type)
    (array-type? type)
    (array-type type)
    (map-type? type)
    (map-type type)
    (struct-type? type)
    (struct-type type)))

(s/fdef column-type
  :args (s/cat :type ::column-type)
  :ret ::ast/type)

(defn create-sink-connector
  "Returns the AST node for a CREATE SINK CONNECTOR statement."
  [db name]
  (node :create-sink-connector :db db :name (identifier name)))

(s/fdef create-sink-connector
  :args (s/cat :db ::db :name ::identifier)
  :fn compileable?
  :ret ::ast/create-sink-connector)

(defn create-source-connector
  "Returns the AST node for a CREATE SOURCE CONNECTOR statement."
  [db name]
  (node :create-source-connector :db db :name (identifier name)))

(s/fdef create-source-connector
  :args (s/cat :db ::db :name ::identifier)
  :fn compileable?
  :ret ::ast/create-source-connector)

(defn create-type
  "Returns the AST node for a CREATE TYPE statement."
  [db name type]
  (node :create-type
        :db db
        :name (identifier name)
        :type (column-type type)))

(s/fdef create-type
  :args (s/cat :db ::db :name ::identifier :type ::column-type)
  :ret ::ast/create-type)

(defn table-element
  "Returns the AST node for a table element."
  [[name type :as element]]
  (node :table-element
        :identifier (identifier name)
        :type (column-type type)))

(s/fdef table-element
  :args (s/cat :element ::table-element)
  :ret ::ast/table-element)

(defn table-elements
  "Returns the AST node for table elements."
  [elements]
  (node :table-elements :elements (map table-element elements)))

(s/fdef table-elements
  :args (s/cat :elements (s/coll-of ::table-element :gen-max 3))
  :ret ::ast/table-elements)

(defn create-stream
  "Returns the AST node for a CREATE STREAM statement."
  [db stream body]
  (cond-> (node :create-stream :db db :source-name (source-name stream))
    (sequential? (first body))
    (assoc :table-elements (table-elements (first body)))))

(s/fdef create-stream
  :args (s/cat :db ::db :stream ::stream :body ::body)
  :fn compileable?
  :ret ::ast/create-stream)

(defn create-table
  "Returns the AST node for a CREATE TABLE statement."
  [db table body]
  (cond-> (node :create-table :db db :source-name (source-name table))
    (sequential? (first body))
    (assoc :table-elements (table-elements (first body)))))

(s/fdef create-table
  :args (s/cat :db ::db :table ::table :body ::body)
  :fn compileable?
  :ret ::ast/create-table)

(defn drop-connector
  "Returns the AST node for a DROP CONNECTOR statement."
  [db connector]
  (node :drop-connector :db db :connector (identifier connector)))

(s/fdef drop-connector
  :args (s/cat :db ::db :connector ::identifier)
  :fn compileable?
  :ret ::ast/drop-connector)

(defn drop-table
  "Returns the AST node for a DROP TABLE statement."
  [db table]
  (node :drop-table :db db :table (identifier table)))

(s/fdef drop-table
  :args (s/cat :db ::db :table ::table)
  :fn compileable?
  :ret ::ast/drop-table)

(defn drop-stream
  "Returns the AST node for a DROP STREAM statement."
  [db stream]
  (node :drop-stream :db db :stream (identifier stream)))

(s/fdef drop-stream
  :args (s/cat :db ::db :stream ::stream)
  :fn compileable?
  :ret ::ast/drop-stream)

(defn drop-type
  "Returns the AST node for a DROP TYPE statement."
  [db name]
  (node :drop-type :db db :name (identifier name)))

(s/fdef drop-type
  :args (s/cat :db ::db :type ::identifier)
  :fn compileable?
  :ret ::ast/drop-type)

(defn group-by
  "Returns the AST node for a GROUP BY clause."
  [expressions]
  (node :group-by :expressions (mapv expression expressions)))

(s/fdef group-by
  :args (s/cat :columns ::columns)
  :ret ::ast/group-by)

(defn join-type
  "Returns the AST node for a join type."
  [type]
  (node :join-type :type type))

(s/fdef join-type
  :args (s/cat :type :sqlingvo.ksql.ast.join-type/type)
  :fn compileable?
  :ret ::ast/join-type)

(defn join
  "Returns the AST node for a JOIN clause."
  [table condition type]
  (node :join
        :table table
        :condition (expression condition)
        :type (join-type type)))

(s/fdef join
  :args (s/cat :table :sqlingvo.ksql.ast.join/table :condition ::expression :type :sqlingvo.ksql.ast.join-type/type)
  :ret ::ast/join)

(defn terminate
  "Returns the AST node for a TERMINATE statement."
  [db query]
  (node :terminate :db db :query (identifier query)))

(s/fdef terminate
  :args (s/cat :db ::db :query simple-keyword?)
  :fn compileable?
  :ret ::ast/terminate)

(defn value
  "Returns the AST node for a value."
  [columns value]
  (node :value :columns columns :value value))

(s/fdef value
  :args (s/cat :columns ::columns :value ::value)
  :fn compileable?
  :ret ::ast/value)

(defn values
  "Returns the AST node for a VALUES clause."
  [columns values]
  (node :values
        :columns columns
        :values (mapv #(value columns %) values)))

(s/fdef values
  :args (s/cat :columns ::columns :values ::values)
  :fn compileable?
  :ret ::ast/values)

(defn window-expression
  "Returns the AST node for a window expression."
  [expression]
  (node :window-expression :expression expression))

(s/fdef window-expression
  :args (s/cat :expression ::window-expression)
  :ret ::ast/window-expression)

(defn window-type
  "Returns the AST node for a window type."
  [type]
  (node :window-type :type type))

(s/fdef window-type
  :args (s/cat :type ::window-type)
  :fn compileable?
  :ret ::ast/window-type)

(defn window
  "Returns the AST node for a WINDOW clause."
  [type expressions]
  (node :window
        :type (window-type type)
        :expressions (mapv window-expression expressions)))

(s/fdef window
  :args (s/cat :type ::window-type :expressions (s/coll-of ::window-expression :gen-max 3))
  :fn compileable?
  :ret ::ast/window)

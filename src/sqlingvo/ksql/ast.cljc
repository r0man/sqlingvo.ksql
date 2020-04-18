(ns sqlingvo.ksql.ast
  (:require [clojure.spec.alpha :as s]
            [sqlingvo.ksql.gen :as gens]))

(def op->children
  "Returns the children of an op as a vector keywords, ordered by the
  position they appear in a statement."
  {:aliased-relation [:relation-primary]
   :array-type [:definition]
   :as [:expression :alias]
   :base-type [:type]
   :create-sink-connector [:name :with]
   :create-source-connector [:name :with]
   :create-stream [:source-name :as :table-elements :with]
   :create-table [:source-name :as :table-elements :with]
   :create-type [:name :type]
   :delete-topic [:delete-topic]
   :drop-connector [:connector]
   :drop-stream [:if-exists :stream :delete-topic]
   :drop-table [:if-exists :table :delete-topic]
   :drop-type [:name]
   :emit [:result-materialization]
   :expression [:expression]
   :from [:relation]
   :function-call [:name :args]
   :group-by [:expressions]
   :having [:expression]
   :identifier [:identifier]
   :if-exists [:if-exists]
   :insert [:table :columns :values]
   :insert-columns [:elements]
   :join [:type :table :condition]
   :join-type [:type]
   :limit [:limit]
   :list [:type]
   :literal [:literal]
   :map-type [:definition]
   :order-by [:columns]
   :partition-by [:expression]
   :print [:clause]
   :query [:select-items :from :where :join :window :group-by :partition-by :having :emit :limit]
   :relation [:aliased-relation]
   :relation-primary [:source-name]
   :result-materialization [:type]
   :select-item [:expression]
   :select-items [:items]
   :show [:type]
   :source-name [:identifier]
   :struct-field [:name :type]
   :struct-type [:fields]
   :table-element [:identifier :type]
   :table-elements [:elements]
   :terminate [:query]
   :value [:value]
   :values [:columns :values]
   :where [:expression]
   :window [:type :expressions]
   :window-expression [:expression]
   :window-type [:type]
   :with [:opts]})

(s/def ::op
  (set (keys op->children)))

(defn- op
  "Returns the spec for the `op`."
  [op]
  #{op})

(s/fdef op
  :args (s/cat :op ::op)
  :ret (s/coll-of ::op :kind set? :count 1))

(defn- children
  "Returns the spec for the children of `op`."
  [op]
  #{(or (op->children op) (throw (ex-info (str "Invalid op:" op) {:op op})))})

(s/fdef children
  :args (s/cat :op ::op)
  :ret (s/coll-of (s/coll-of simple-keyword? :kind vector?) :kind set? :count 1))

;;; Identifier

(s/def :sqlingvo.ksql.ast.identifier/op
  (op :identifier))

(s/def :sqlingvo.ksql.ast.identifier/children
  (children :identifier))

(s/def :sqlingvo.ksql.ast.identifier/identifier
  (s/with-gen simple-keyword? (constantly gens/keyword-identifier)))

(s/def :sqlingvo.ksql.ast/identifier
  (s/keys :req-un [:sqlingvo.ksql.ast.identifier/op
                   :sqlingvo.ksql.ast.identifier/children
                   :sqlingvo.ksql.ast.identifier/identifier]))

;;; Limit

(s/def :sqlingvo.ksql.ast.limit/op
  (op :limit))

(s/def :sqlingvo.ksql.ast.limit/children
  (children :limit))

(s/def :sqlingvo.ksql.ast.limit/limit
  nat-int?)

(s/def :sqlingvo.ksql.ast/limit
  (s/keys :req-un [:sqlingvo.ksql.ast.limit/op
                   :sqlingvo.ksql.ast.limit/children
                   :sqlingvo.ksql.ast.limit/limit]))

;;; Literal

(s/def :sqlingvo.ksql.ast.literal/op
  (op :literal))

(s/def :sqlingvo.ksql.ast.literal/children
  (children :literal))

(s/def :sqlingvo.ksql.ast.literal/literal
  (s/or :boolean boolean?
        :nil nil?
        :number number?
        :string string?))

(s/def :sqlingvo.ksql.ast/literal
  (s/keys :req-un [:sqlingvo.ksql.ast.literal/op
                   :sqlingvo.ksql.ast.literal/children
                   :sqlingvo.ksql.ast.literal/literal]))

;;; List

(s/def :sqlingvo.ksql.ast.list/op
  (op :list))

(s/def :sqlingvo.ksql.ast.list/children
  (children :list))

(s/def :sqlingvo.ksql.ast.list/type
  #{:functions :properties :streams :tables :topics :types})

(s/def :sqlingvo.ksql.ast/list
  (s/keys :req-un [:sqlingvo.ksql.ast.list/op
                   :sqlingvo.ksql.ast.list/children
                   :sqlingvo.ksql.ast.list/type]))

;;; Expression

(s/def :sqlingvo.ksql.ast.expression/op
  (op :expression))

(s/def :sqlingvo.ksql.ast.expression/children
  (children :expression))

(s/def :sqlingvo.ksql.ast.expression/expression
  (s/or :identifier ::identifier
        :literal ::literal
        :function-call ::function-call))

(s/def :sqlingvo.ksql.ast/expression
  (s/keys :req-un [:sqlingvo.ksql.ast.expression/op
                   :sqlingvo.ksql.ast.expression/children
                   :sqlingvo.ksql.ast.expression/expression]))

;;; Function Call

(s/def :sqlingvo.ksql.ast.function-call/op
  (op :function-call))

(s/def :sqlingvo.ksql.ast.function-call/children
  (children :function-call))

(s/def :sqlingvo.ksql.ast.function-call/name symbol?)

(s/def :sqlingvo.ksql.ast.function-call/args
  (s/coll-of ::expression :gen-max 3))

(s/def :sqlingvo.ksql.ast/function-call
  (s/keys :req-un [:sqlingvo.ksql.ast.function-call/op
                   :sqlingvo.ksql.ast.function-call/children
                   :sqlingvo.ksql.ast.function-call/name
                   :sqlingvo.ksql.ast.function-call/args]))

;; Having

(s/def :sqlingvo.ksql.ast.having/op
  (op :having))

(s/def :sqlingvo.ksql.ast.having/children
  (children :having))

(s/def :sqlingvo.ksql.ast.having/expression ::expression)

(s/def :sqlingvo.ksql.ast/having
  (s/keys :req-un [:sqlingvo.ksql.ast.having/op
                   :sqlingvo.ksql.ast.having/children
                   :sqlingvo.ksql.ast.having/expression]))

;;; Result Materialization

(s/def :sqlingvo.ksql.ast.result-materialization/op
  (op :result-materialization))

(s/def :sqlingvo.ksql.ast.result-materialization/children
  (children :result-materialization))

(s/def :sqlingvo.ksql.ast.result-materialization/type
  #{:changes})

(s/def :sqlingvo.ksql.ast/result-materialization
  (s/keys :req-un [:sqlingvo.ksql.ast.result-materialization/op
                   :sqlingvo.ksql.ast.result-materialization/children
                   :sqlingvo.ksql.ast.result-materialization/type]))

;;; Emit

(s/def :sqlingvo.ksql.ast.emit/op
  (op :emit))

(s/def :sqlingvo.ksql.ast.emit/children
  (children :emit))

(s/def :sqlingvo.ksql.ast.emit/result-materialization
  :sqlingvo.ksql.ast/result-materialization)

(s/def :sqlingvo.ksql.ast/emit
  (s/keys :req-un [:sqlingvo.ksql.ast.emit/op
                   :sqlingvo.ksql.ast.emit/children
                   :sqlingvo.ksql.ast.emit/result-materialization]))

;;; Delete Topic

(s/def :sqlingvo.ksql.ast.delete-topic/op
  (op :delete-topic))

(s/def :sqlingvo.ksql.ast.delete-topic/children
  (children :delete-topic))

(s/def :sqlingvo.ksql.ast.delete-topic/delete? boolean?)

(s/def :sqlingvo.ksql.ast/delete-topic
  (s/keys :req-un [:sqlingvo.ksql.ast.delete-topic/op
                   :sqlingvo.ksql.ast.delete-topic/children
                   :sqlingvo.ksql.ast.delete-topic/delete?]))

;;; If Exists

(s/def :sqlingvo.ksql.ast.if-exists/op
  (op :if-exists))

(s/def :sqlingvo.ksql.ast.if-exists/children
  (children :if-exists))

(s/def :sqlingvo.ksql.ast.if-exists/if-exists boolean?)

(s/def :sqlingvo.ksql.ast/if-exists
  (s/keys :req-un [:sqlingvo.ksql.ast.if-exists/op
                   :sqlingvo.ksql.ast.if-exists/children
                   :sqlingvo.ksql.ast.if-exists/if-exists]))

;;; Insert Columns

(s/def :sqlingvo.ksql.ast.insert-columns/op
  (op :insert-columns))

(s/def :sqlingvo.ksql.ast.insert-columns/children
  (children :insert-columns))

(s/def :sqlingvo.ksql.ast.insert-columns/elements
  (s/coll-of ::identifier :gen-max 3))

(s/def :sqlingvo.ksql.ast/insert-columns
  (s/keys :req-un [:sqlingvo.ksql.ast.insert-columns/op
                   :sqlingvo.ksql.ast.insert-columns/children
                   :sqlingvo.ksql.ast.insert-columns/elements]))

;;; Insert

(s/def :sqlingvo.ksql.ast.insert/op
  (op :insert))

(s/def :sqlingvo.ksql.ast.insert/children
  (children :insert))

(s/def :sqlingvo.ksql.ast.insert/table
  ::identifier)

(s/def :sqlingvo.ksql.ast.insert/columns
  :sqlingvo.ksql.ast/insert-columns)

(s/def :sqlingvo.ksql.ast/insert
  (s/keys :req-un [:sqlingvo.ksql.ast.insert/op
                   :sqlingvo.ksql.ast.insert/children
                   :sqlingvo.ksql.ast.insert/table
                   :sqlingvo.ksql.ast.insert/columns]))

;;; Source Name

(s/def :sqlingvo.ksql.ast.source-name/op
  (op :source-name))

(s/def :sqlingvo.ksql.ast.source-name/children
  (children :source-name))

(s/def :sqlingvo.ksql.ast.source-name/source
  (s/or :alias map? ;; TODO: Spec alias
        :identifier ::identifier))

(s/def :sqlingvo.ksql.ast/source-name
  (s/keys :req-un [:sqlingvo.ksql.ast.source-name/op
                   :sqlingvo.ksql.ast.source-name/children
                   :sqlingvo.ksql.ast.source-name/source]))

;;; Relation Primary

(s/def :sqlingvo.ksql.ast.relation-primary/op
  (op :relation-primary))

(s/def :sqlingvo.ksql.ast.relation-primary/children
  (children :relation-primary))

(s/def :sqlingvo.ksql.ast/relation-primary
  (s/keys :req-un [:sqlingvo.ksql.ast.relation-primary/op
                   :sqlingvo.ksql.ast.relation-primary/children]))

;;; Aliased Relation

(s/def :sqlingvo.ksql.ast.aliased-relation/op
  (op :aliased-relation))

(s/def :sqlingvo.ksql.ast.aliased-relation/children
  (children :aliased-relation))

(s/def :sqlingvo.ksql.ast.aliased-relation/relation-primary
  ::relation-primary)

(s/def :sqlingvo.ksql.ast/aliased-relation
  (s/keys :req-un [:sqlingvo.ksql.ast.aliased-relation/op
                   :sqlingvo.ksql.ast.aliased-relation/children
                   :sqlingvo.ksql.ast.aliased-relation/relation-primary]))

;;; Relation

(s/def :sqlingvo.ksql.ast.relation/op
  (op :relation))

(s/def :sqlingvo.ksql.ast.relation/children
  (children :relation))

(s/def :sqlingvo.ksql.ast.relation/aliased-relation
  ::aliased-relation)

(s/def :sqlingvo.ksql.ast/relation
  (s/keys :req-un [:sqlingvo.ksql.ast.relation/op
                   :sqlingvo.ksql.ast.relation/children
                   :sqlingvo.ksql.ast.relation/aliased-relation]))

;;; From

(s/def :sqlingvo.ksql.ast.from/op
  (op :from))

(s/def :sqlingvo.ksql.ast.from/children
  (children :from))

(s/def :sqlingvo.ksql.ast.from/relation ::relation)

(s/def :sqlingvo.ksql.ast/from
  (s/keys :req-un [:sqlingvo.ksql.ast.from/op
                   :sqlingvo.ksql.ast.from/children
                   :sqlingvo.ksql.ast.from/relation]))

;;; Where

(s/def :sqlingvo.ksql.ast.where/op
  (op :where))

(s/def :sqlingvo.ksql.ast.where/children
  (children :where))

(s/def :sqlingvo.ksql.ast.where/expression
  ::expression)

(s/def :sqlingvo.ksql.ast/where
  (s/keys :req-un [:sqlingvo.ksql.ast.where/op
                   :sqlingvo.ksql.ast.where/children
                   :sqlingvo.ksql.ast.where/expression]))

;;; Order By

(s/def :sqlingvo.ksql.ast.order-by/op
  (op :order-by))

(s/def :sqlingvo.ksql.ast.order-by/children
  (children :order-by))

(s/def :sqlingvo.ksql.ast.order-by/columns
  (s/coll-of simple-keyword? :gen-max 3))

(s/def :sqlingvo.ksql.ast/order-by
  (s/keys :req-un [:sqlingvo.ksql.ast.order-by/op
                   :sqlingvo.ksql.ast.order-by/children
                   :sqlingvo.ksql.ast.order-by/columns]))

;;; Select Item

(s/def :sqlingvo.ksql.ast.select-item/op
  (op :select-item))

(s/def :sqlingvo.ksql.ast.select-item/children
  (children :select-item))

(s/def :sqlingvo.ksql.ast.select-item/expression
  (s/or :as ::as :expresion ::expression))

(s/def :sqlingvo.ksql.ast/select-item
  (s/keys :req-un [:sqlingvo.ksql.ast.select-item/op
                   :sqlingvo.ksql.ast.select-item/children
                   :sqlingvo.ksql.ast.select-item/expression]))

;;; Select Items

(s/def :sqlingvo.ksql.ast.select-items/op
  (op :select-items))

(s/def :sqlingvo.ksql.ast.select-items/children
  (children :select-items))

(s/def :sqlingvo.ksql.ast.select-items/items
  (s/coll-of ::select-item :gen-max 3))

(s/def :sqlingvo.ksql.ast/select-items
  (s/keys :req-un [:sqlingvo.ksql.ast.select-items/op
                   :sqlingvo.ksql.ast.select-items/children
                   :sqlingvo.ksql.ast.select-items/items]))

;;; query

(s/def :sqlingvo.ksql.ast.query/op
  (op :query))

(s/def :sqlingvo.ksql.ast.query/children
  (children :query))

(s/def :sqlingvo.ksql.ast.query/select-items
  ::select-items)

(s/def ::query
  (s/keys :req-un [:sqlingvo.ksql.ast.query/op
                   :sqlingvo.ksql.ast.query/children
                   :sqlingvo.ksql.ast.query/select-items]))

;;; Show

(s/def :sqlingvo.ksql.ast.show/op
  (op :show))

(s/def :sqlingvo.ksql.ast.show/children
  (children :show))

(s/def :sqlingvo.ksql.ast.show/type
  #{:functions :properties :streams :tables :topics :types})

(s/def :sqlingvo.ksql.ast/show
  (s/keys :req-un [:sqlingvo.ksql.ast.show/op
                   :sqlingvo.ksql.ast.show/children
                   :sqlingvo.ksql.ast.show/type]))

;;; Partition By

(s/def :sqlingvo.ksql.ast.partition-by/op
  (op :partition-by))

(s/def :sqlingvo.ksql.ast.partition-by/children
  (children :partition-by))

(s/def :sqlingvo.ksql.ast.partition-by/expression
  ::expression)

(s/def :sqlingvo.ksql.ast/partition-by
  (s/keys :req-un [:sqlingvo.ksql.ast.partition-by/op
                   :sqlingvo.ksql.ast.partition-by/children
                   :sqlingvo.ksql.ast.partition-by/expression]))

;;; Print

(s/def :sqlingvo.ksql.ast.print/op
  (op :print))

(s/def :sqlingvo.ksql.ast.print/children
  (children :print))

(s/def :sqlingvo.ksql.ast.print/clause simple-keyword?)

(s/def :sqlingvo.ksql.ast/print
  (s/keys :req-un [:sqlingvo.ksql.ast.print/op
                   :sqlingvo.ksql.ast.print/children
                   :sqlingvo.ksql.ast.print/clause]))

;;; With

(s/def :sqlingvo.ksql.ast.with/op
  (op :with))

(s/def :sqlingvo.ksql.ast.with/children
  (children :with))

(s/def :sqlingvo.ksql.ast.with/opts map?)

(s/def :sqlingvo.ksql.ast/with
  (s/keys :req-un [:sqlingvo.ksql.ast.with/op
                   :sqlingvo.ksql.ast.with/children
                   :sqlingvo.ksql.ast.with/opts]))

;;; Base Type

(s/def :sqlingvo.ksql.ast.base-type/op
  (op :base-type))

(s/def :sqlingvo.ksql.ast.base-type/children
  (children :base-type))

(s/def :sqlingvo.ksql.ast.base-type/type ::identifier)

(s/def :sqlingvo.ksql.ast/base-type
  (s/keys :req-un [:sqlingvo.ksql.ast.base-type/op
                   :sqlingvo.ksql.ast.base-type/children
                   :sqlingvo.ksql.ast.base-type/type]))

;;; Array Type

(s/def :sqlingvo.ksql.ast.array-type/op
  (op :array-type))

(s/def :sqlingvo.ksql.ast.array-type/children
  (children :array-type))

(s/def :sqlingvo.ksql.ast.array-type/definition simple-keyword?)

(s/def :sqlingvo.ksql.ast/array-type
  (s/keys :req-un [:sqlingvo.ksql.ast.array-type/op
                   :sqlingvo.ksql.ast.array-type/children
                   :sqlingvo.ksql.ast.array-type/definition]))

;;; Map Type

(s/def :sqlingvo.ksql.ast.map-type/op
  (op :map-type))

(s/def :sqlingvo.ksql.ast.map-type/children
  (children :map-type))

(s/def :sqlingvo.ksql.ast.map-type/definition
  (s/map-of simple-keyword? simple-keyword? :count 1))

(s/def :sqlingvo.ksql.ast/map-type
  (s/keys :req-un [:sqlingvo.ksql.ast.map-type/op
                   :sqlingvo.ksql.ast.map-type/children
                   :sqlingvo.ksql.ast.map-type/definition]))

;;; Struct Field

(s/def :sqlingvo.ksql.ast.struct-field/op
  (op :struct-field))

(s/def :sqlingvo.ksql.ast.struct-field/children
  (children :struct-field))

(s/def :sqlingvo.ksql.ast.struct-field/name
  (s/or :keyword simple-keyword? :string string?))

(s/def :sqlingvo.ksql.ast.struct-field/type
  simple-keyword?)

(s/def :sqlingvo.ksql.ast/struct-field
  (s/keys :req-un [:sqlingvo.ksql.ast.struct-field/op
                   :sqlingvo.ksql.ast.struct-field/children
                   :sqlingvo.ksql.ast.struct-field/name
                   :sqlingvo.ksql.ast.struct-field/type]))

;;; Struct Type

(s/def :sqlingvo.ksql.ast.struct-type/op
  (op :struct-type))

(s/def :sqlingvo.ksql.ast.struct-type/children
  (children :struct-type))

(s/def :sqlingvo.ksql.ast.struct-type/fields
  (s/coll-of ::struct-field :gen-max 3))

(s/def :sqlingvo.ksql.ast/struct-type
  (s/keys :req-un [:sqlingvo.ksql.ast.struct-type/op
                   :sqlingvo.ksql.ast.struct-type/children
                   :sqlingvo.ksql.ast.struct-type/fields]))

;;; Create Sink Connector

(s/def :sqlingvo.ksql.ast.create-sink-connector/op
  (op :create-sink-connector))

(s/def :sqlingvo.ksql.ast.create-sink-connector/children
  (children :create-sink-connector))

(s/def :sqlingvo.ksql.ast.create-sink-connector/name ::identifier)

(s/def :sqlingvo.ksql.ast/create-sink-connector
  (s/keys :req-un [:sqlingvo.ksql.ast.create-sink-connector/op
                   :sqlingvo.ksql.ast.create-sink-connector/children
                   :sqlingvo.ksql.ast.create-sink-connector/name]))

;;; Create Source Connector

(s/def :sqlingvo.ksql.ast.create-source-connector/op
  (op :create-source-connector))

(s/def :sqlingvo.ksql.ast.create-source-connector/children
  (children :create-source-connector))

(s/def :sqlingvo.ksql.ast.create-source-connector/name ::identifier)

(s/def :sqlingvo.ksql.ast/create-source-connector
  (s/keys :req-un [:sqlingvo.ksql.ast.create-source-connector/op
                   :sqlingvo.ksql.ast.create-source-connector/children
                   :sqlingvo.ksql.ast.create-source-connector/name]))


(s/def :sqlingvo.ksql.ast/type
  (s/or :base-type ::base-type
        :array-type ::array-type
        :map-type ::map-type
        :struct-type ::struct-type))

;;; Table Element

(s/def :sqlingvo.ksql.ast.table-element/op
  (op :table-element))

(s/def :sqlingvo.ksql.ast.table-element/children
  (children :table-element))

(s/def :sqlingvo.ksql.ast.table-element/identifier
  ::identifier)

(s/def :sqlingvo.ksql.ast.table-element/type
  ::type)

(s/def :sqlingvo.ksql.ast/table-element
  (s/keys :req-un [:sqlingvo.ksql.ast.table-element/op
                   :sqlingvo.ksql.ast.table-element/children
                   :sqlingvo.ksql.ast.table-element/identifier
                   :sqlingvo.ksql.ast.table-element/type]))

;;; Table Elements

(s/def :sqlingvo.ksql.ast.table-elements/op
  (op :table-elements))

(s/def :sqlingvo.ksql.ast.table-elements/children
  (children :table-elements))

(s/def :sqlingvo.ksql.ast.table-elements/elements
  (s/coll-of ::table-element :gen-max 3))

(s/def :sqlingvo.ksql.ast/table-elements
  (s/keys :req-un [:sqlingvo.ksql.ast.table-elements/op
                   :sqlingvo.ksql.ast.table-elements/children
                   :sqlingvo.ksql.ast.table-elements/elements]))

;;; Create Stream

(s/def :sqlingvo.ksql.ast.create-stream/op
  (op :create-stream))

(s/def :sqlingvo.ksql.ast.create-stream/children
  (children :create-stream))

(s/def :sqlingvo.ksql.ast.create-stream/source-name
  ::source-name)

(s/def :sqlingvo.ksql.ast.create-stream/table-elements
  ::table-elements)

(s/def :sqlingvo.ksql.ast/create-stream
  (s/keys :req-un [:sqlingvo.ksql.ast.create-stream/op
                   :sqlingvo.ksql.ast.create-stream/children
                   :sqlingvo.ksql.ast.create-stream/source-name]
          :opt-un [:sqlingvo.ksql.ast.create-stream/table-elements]))

;;; Create Table

(s/def :sqlingvo.ksql.ast.create-table/op
  (op :create-table))

(s/def :sqlingvo.ksql.ast.create-table/children
  (children :create-table))

(s/def :sqlingvo.ksql.ast.create-table/source-name
  ::source-name)

(s/def :sqlingvo.ksql.ast.create-table/table-elements
  ::table-elements)

(s/def :sqlingvo.ksql.ast/create-table
  (s/keys :req-un [:sqlingvo.ksql.ast.create-table/op
                   :sqlingvo.ksql.ast.create-table/children
                   :sqlingvo.ksql.ast.create-table/source-name]
          :opt-un [:sqlingvo.ksql.ast.create-table/table-elements]))

;; Create Type

(s/def :sqlingvo.ksql.ast.create-type/op (op :create-type))
(s/def :sqlingvo.ksql.ast.create-type/children (children :create-type))
(s/def :sqlingvo.ksql.ast.create-type/name ::identifier)
(s/def :sqlingvo.ksql.ast.create-type/type ::type)

(s/def :sqlingvo.ksql.ast/create-type
  (s/keys :req-un [:sqlingvo.ksql.ast.create-type/op
                   :sqlingvo.ksql.ast.create-type/children
                   :sqlingvo.ksql.ast.create-type/name
                   :sqlingvo.ksql.ast.create-type/type]))

;;; Drop Connector

(s/def :sqlingvo.ksql.ast.drop-connector/op
  (op :drop-connector))

(s/def :sqlingvo.ksql.ast.drop-connector/children
  (children :drop-connector))

(s/def :sqlingvo.ksql.ast.drop-connector/connector ::identifier)

(s/def :sqlingvo.ksql.ast/drop-connector
  (s/keys :req-un [:sqlingvo.ksql.ast.drop-connector/op
                   :sqlingvo.ksql.ast.drop-connector/children
                   :sqlingvo.ksql.ast.drop-connector/connector]))

;;; Drop Table

(s/def :sqlingvo.ksql.ast.drop-table/op
  (op :drop-table))

(s/def :sqlingvo.ksql.ast.drop-table/children
  (children :drop-table))

(s/def :sqlingvo.ksql.ast.drop-table/table ::identifier)

(s/def :sqlingvo.ksql.ast/drop-table
  (s/keys :req-un [:sqlingvo.ksql.ast.drop-table/op
                   :sqlingvo.ksql.ast.drop-table/children
                   :sqlingvo.ksql.ast.drop-table/table]))

;;; Drop Stream

(s/def :sqlingvo.ksql.ast.drop-stream/op
  (op :drop-stream))

(s/def :sqlingvo.ksql.ast.drop-stream/children
  (children :drop-stream))

(s/def :sqlingvo.ksql.ast.drop-stream/stream ::identifier)

(s/def :sqlingvo.ksql.ast/drop-stream
  (s/keys :req-un [:sqlingvo.ksql.ast.drop-stream/op
                   :sqlingvo.ksql.ast.drop-stream/children
                   :sqlingvo.ksql.ast.drop-stream/stream]))

;;; Drop Type

(s/def :sqlingvo.ksql.ast.drop-type/op (op :drop-type))
(s/def :sqlingvo.ksql.ast.drop-type/children (children :drop-type))
(s/def :sqlingvo.ksql.ast.drop-type/name ::identifier)

(s/def :sqlingvo.ksql.ast/drop-type
  (s/keys :req-un [:sqlingvo.ksql.ast.drop-type/op
                   :sqlingvo.ksql.ast.drop-type/children
                   :sqlingvo.ksql.ast.drop-type/name]))

;;; Group By

(s/def :sqlingvo.ksql.ast.group-by/op
  (op :group-by))

(s/def :sqlingvo.ksql.ast.group-by/children
  (children :group-by))

(s/def :sqlingvo.ksql.ast.group-by/expressions
  (s/coll-of ::expression :gen-max 3))

(s/def :sqlingvo.ksql.ast/group-by
  (s/keys :req-un [:sqlingvo.ksql.ast.group-by/op
                   :sqlingvo.ksql.ast.group-by/children
                   :sqlingvo.ksql.ast.group-by/expressions]))

;;; Join Type

(s/def :sqlingvo.ksql.ast.join-type/op
  (op :join-type))

(s/def :sqlingvo.ksql.ast.join-type/children
  (children :join-type))

(s/def :sqlingvo.ksql.ast.join-type/type
  #{:left :inner :right})

(s/def :sqlingvo.ksql.ast/join-type
  (s/keys :req-un [:sqlingvo.ksql.ast.join-type/op
                   :sqlingvo.ksql.ast.join-type/children
                   :sqlingvo.ksql.ast.join-type/type]))

;;; Join

(s/def :sqlingvo.ksql.ast.join/op
  (op :join))

(s/def :sqlingvo.ksql.ast.join/children
  (children :join))

(s/def :sqlingvo.ksql.ast.join/table (s/or :as ::as :keyword simple-keyword?))
(s/def :sqlingvo.ksql.ast.join/condition ::expression)
(s/def :sqlingvo.ksql.ast.join/type ::join-type)

(s/def :sqlingvo.ksql.ast/join
  (s/keys :req-un [:sqlingvo.ksql.ast.join/op
                   :sqlingvo.ksql.ast.join/children
                   :sqlingvo.ksql.ast.join/table
                   :sqlingvo.ksql.ast.join/condition
                   :sqlingvo.ksql.ast.join/type]))


;;; Terminate

(s/def :sqlingvo.ksql.ast.terminate/op
  (op :terminate))

(s/def :sqlingvo.ksql.ast.terminate/children
  (children :terminate))

(s/def :sqlingvo.ksql.ast.terminate/query ::identifier)

(s/def :sqlingvo.ksql.ast/terminate
  (s/keys :req-un [:sqlingvo.ksql.ast.terminate/op
                   :sqlingvo.ksql.ast.terminate/children
                   :sqlingvo.ksql.ast.terminate/query]))
;;; Value

(s/def :sqlingvo.ksql.ast.value/op
  (op :value))

(s/def :sqlingvo.ksql.ast.value/children
  (children :value))

(s/def :sqlingvo.ksql.ast.value/columns
  (s/coll-of simple-keyword? :gen-max 3))

(s/def :sqlingvo.ksql.ast.value/value
  (s/or :seq (s/coll-of :sqlingvo.ksql.ast.literal/literal :gen-max 3)
        :map (s/map-of simple-keyword? :sqlingvo.ksql.ast.literal/literal :gen-max 3)))

(s/def :sqlingvo.ksql.ast/value
  (s/keys :req-un [:sqlingvo.ksql.ast.value/op
                   :sqlingvo.ksql.ast.value/children
                   :sqlingvo.ksql.ast.value/columns
                   :sqlingvo.ksql.ast.value/value]))

;;; Values

(s/def :sqlingvo.ksql.ast.values/op
  (op :values))

(s/def :sqlingvo.ksql.ast.values/children
  (children :values))

(s/def :sqlingvo.ksql.ast.values/columns
  :sqlingvo.ksql.ast.value/columns)

(s/def :sqlingvo.ksql.ast.values/values
  (s/coll-of ::value :gen-max 3))

(s/def :sqlingvo.ksql.ast/values
  (s/keys :req-un [:sqlingvo.ksql.ast.values/op
                   :sqlingvo.ksql.ast.values/children
                   :sqlingvo.ksql.ast.values/columns
                   :sqlingvo.ksql.ast.values/values]))

;;; Window Expression

(s/def :sqlingvo.ksql.ast.window-expression/op
  (op :window-expression))

(s/def :sqlingvo.ksql.ast.window-expression/children
  (children :window-expression))

(s/def :sqlingvo.ksql.ast.window-expression/expression
  (s/coll-of (s/or :number number? :symbol symbol?) :gen-max 3))

(s/def :sqlingvo.ksql.ast/window-expression
  (s/keys :req-un [:sqlingvo.ksql.ast.window-expression/op
                   :sqlingvo.ksql.ast.window-expression/children
                   :sqlingvo.ksql.ast.window-expression/expression]))

;;; Window Type

(s/def :sqlingvo.ksql.ast.window-type/op
  (op :window-type))

(s/def :sqlingvo.ksql.ast.window-type/children
  (children :window-type))

(s/def :sqlingvo.ksql.ast.window-type/type
  #{:hopping :session :tumbling})

(s/def :sqlingvo.ksql.ast/window-type
  (s/keys :req-un [:sqlingvo.ksql.ast.window-type/op
                   :sqlingvo.ksql.ast.window-type/children
                   :sqlingvo.ksql.ast.window-type/type]))

;;; Window

(s/def :sqlingvo.ksql.ast.window/op
  (op :window))

(s/def :sqlingvo.ksql.ast.window/children
  (children :window))

(s/def :sqlingvo.ksql.ast.window/type
  ::window-type)

(s/def :sqlingvo.ksql.ast.window/expressions
  (s/coll-of ::window-expression :gen-max 3))

(s/def :sqlingvo.ksql.ast/window
  (s/keys :req-un [:sqlingvo.ksql.ast.window/op
                   :sqlingvo.ksql.ast.window/children
                   :sqlingvo.ksql.ast.window/type
                   :sqlingvo.ksql.ast.window/expressions]))

;;; As

(s/def :sqlingvo.ksql.ast.as/op
  (op :as))

(s/def :sqlingvo.ksql.ast.as/children
  (children :as))

(s/def :sqlingvo.ksql.ast.as/alias
  ::identifier)

(s/def :sqlingvo.ksql.ast.as/expression
  (s/or :identifier ::identifier
        :expression ::expression
        :query ::query))

(s/def :sqlingvo.ksql.ast/as
  (s/keys :req-un [:sqlingvo.ksql.ast.as/op
                   :sqlingvo.ksql.ast.as/children
                   :sqlingvo.ksql.ast.as/alias]
          :opt-un [:sqlingvo.ksql.ast.as/expression]))

(ns sqlingvo.ksql.compiler
  (:require [clojure.string :as str]
            [sqlingvo.ksql.utils :as util]))

(defmulti compile-sql
  "Compile the `ast` into SQL."
  (fn [db ast] (:op ast)))

(defmulti compile-fn
  "Compile a function call into SQL."
  (fn [db ast] (keyword (:name ast))))

(defmulti compile-object
  "Compile `value` into SQL."
  (fn [value] (type value)))

(defn- separated [db separator xs]
  ;; TODO: Simplify by not returning spaces in the beginning.
  (->> (map #(compile-sql db %) xs)
       (remove nil?)
       (map str/trim)
       (str/join separator)))

(defn- comma-separated [db xs]
  (separated db ", " xs))

(defn- space-separated [db xs]
  (separated db " " xs))

(defn- sql-name [x]
  (cond
    (keyword? x)
    (-> (name x)
        (str/upper-case)
        (str/replace #"-" "_"))
    (string? x)
    (str "\"" x "\"")))

(defn- compile-children
  [db {:keys [children] :as ast}]
  (->> (map ast children)
       (remove nil?)
       (space-separated db)))

(defn- compile-arithmetic-binary
  [db {:keys [name args]}]
  (separated db (str/upper-case (str \space name \space)) args))

(defmethod compile-fn :* [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :/ [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :+ [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :- [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :->
  [db {:keys [name args]}]
  (->> (for [{:keys [expression]} args]
         (case (:op expression)
           :identifier
           (compile-sql db expression)
           :literal
           (str "\"" (:literal expression) "\"")))
       (str/join "->")))

(defmethod compile-fn := [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :!= [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :> [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :>= [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :< [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :<= [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :and [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :array [db {:keys [name args]}]
  (str "ARRAY[" (comma-separated db args) "]"))

(defn- compile-associative-args [db args]
  (str/join ", " (map #(separated db " := " %) (partition-all 2 args))))

(defmethod compile-fn :map [db {:keys [name args]}]
  (str "MAP(" (compile-associative-args db args) ")"))

(defmethod compile-fn :struct [db {:keys [name args]}]
  (str "STRUCT(" (compile-associative-args db args) ")"))

(defmethod compile-fn :cast
  [db {:keys [args]}]
  (let [[expr alias] args]
    (str "CAST(" (compile-sql db expr)
         " AS " (compile-sql db alias) ")")))

(defmethod compile-fn :get [db {:keys [args]}]
  (let [[field index] args]
    (str (compile-sql db field) "[" (compile-sql db index) "]")))

(defmethod compile-fn :or [db ast]
  (compile-arithmetic-binary db ast))

(defmethod compile-fn :is-null [db {:keys [args]}]
  (str (compile-sql db (first args)) " IS NULL"))

(defmethod compile-fn :is-not-null [db {:keys [args]}]
  (str (compile-sql db (first args)) " IS NOT NULL"))

(defmethod compile-fn :default [db {:keys [name args]}]
  (str (str/upper-case (str name)) "("
       (comma-separated db args) ")"))

(defmethod compile-object
  #?(:clj clojure.lang.Keyword :cljs cljs.core.Keyword) [value]
  (sql-name value))

;; TODO: Howto dispatch on string type in ClojureScript?
;; (defmethod compile-object
;;   #?(:clj String :cljs Object) [value]
;;   (str "'" value "'"))

(defmethod compile-object :default [value]
  (cond
    ;; TODO: Howto dispatch on string type in ClojureScript?
    (string? value)
    (str "'" value "'")
    :else (str value)))

(defmethod compile-sql :aliased-relation [db {:keys [relation-primary]}]
  (compile-sql db relation-primary))

(defmethod compile-sql :array-type [db {:keys [definition]}]
  (str "ARRAY<" (sql-name definition) ">"))

(defmethod compile-sql :as [db {:keys [alias expression]}]
  (str (some->> expression (compile-sql db))
       " AS " (compile-sql db alias)))

(defmethod compile-sql :base-type [db {:keys [type]}]
  (compile-sql db type))

(defmethod compile-sql :create-sink-connector [db ast]
  (str "CREATE SINK CONNECTOR " (compile-children db ast)))

(defmethod compile-sql :create-source-connector [db ast]
  (str "CREATE SOURCE CONNECTOR " (compile-children db ast)))

(defmethod compile-sql :create-stream [db ast]
  (str "CREATE STREAM " (compile-children db ast)))

(defmethod compile-sql :create-table [db ast]
  (str "CREATE TABLE " (compile-children db ast)))

(defmethod compile-sql :create-type [db {:keys [name type]}]
  (str "CREATE TYPE " (compile-sql db name) " AS " (compile-sql db type)))

(defmethod compile-sql :struct-field [db {:keys [name type]}]
  (str (sql-name name) " " (sql-name type)))

(defmethod compile-sql :struct-type [db {:keys [fields]}]
  (str "STRUCT<" (comma-separated db fields) ">"))

(defmethod compile-sql :delete-topic [db {:keys [delete-topic]}]
  (when delete-topic (str " DELETE TOPIC")))

(defmethod compile-sql :drop-connector [db {:keys [connector]}]
  (str "DROP CONNECTOR " (compile-sql db connector)))

(defmethod compile-sql :drop-stream [db ast]
  (str "DROP STREAM " (compile-children db ast)))

(defmethod compile-sql :drop-table [db ast]
  (str "DROP TABLE " (compile-children db ast)))

(defmethod compile-sql :drop-type [db ast]
  (str "DROP TYPE " (compile-children db ast)))

(defmethod compile-sql :group-by [db {:keys [expressions]}]
  (str " GROUP BY " (comma-separated db expressions)))

(defmethod compile-sql :result-materialization [db {:keys [result-materialization]}]
  (str/upper-case (name result-materialization)))

(defmethod compile-sql :emit [db ast]
  (str " EMIT " (compile-children db ast)))

(defmethod compile-sql :expression [db {:keys [expression]}]
  (compile-sql db expression))

(defmethod compile-sql :from [db ast]
  (str "FROM " (compile-children db ast)))

(defmethod compile-sql :function-call [db ast]
  (compile-fn db ast))

(defmethod compile-sql :having [db {:keys [expression]}]
  (str "HAVING " (compile-sql db expression)))

(defmethod compile-sql :identifier [db {:keys [identifier]}]
  (cond
    (keyword? identifier)
    (sql-name identifier)
    :else (compile-sql db identifier)))

(defmethod compile-sql :if-exists [db {:keys [if-exists]}]
  (when if-exists (str " IF EXISTS")))

(defmethod compile-sql :insert [db ast]
  (str "INSERT INTO " (compile-children db ast)))

(defmethod compile-sql :insert-columns [db {:keys [elements]}]
  (str " (" (comma-separated db elements) ") "))

(defmethod compile-sql :join [db {:keys [table condition type]}]
  (str (some->> type (compile-sql db))
       " JOIN " (compile-sql db table)
       " ON " (compile-sql db condition)))

(defmethod compile-sql :join-type [db {:keys [type]}]
  (str " " (str/upper-case (name type))))

(defmethod compile-sql :limit [db {:keys [limit]}]
  (str " LIMIT " limit))

(defmethod compile-sql :literal [db {:keys [literal]}]
  (cond
    (nil? literal)
    "NULL"
    (boolean? literal)
    (str literal)
    (number? literal)
    (str literal)
    (string? literal)
    (str "'" literal "'")))

(defmethod compile-sql :list [db {:keys [type]}]
  (str "LIST " (sql-name type) ""))

(defmethod compile-sql :map-type [db {:keys [definition]}]
  (let [[k v] (first definition)]
    (str "MAP<" (sql-name k) ", " (sql-name v) ">")))

(defmethod compile-sql :select-item [db {:keys [expression]}]
  (compile-sql db expression))

(defmethod compile-sql :select-items [db {:keys [items]}]
  (comma-separated db items))

(defmethod compile-sql :show [db {:keys [type]}]
  (str "SHOW " (sql-name type)))

(defmethod compile-sql :table-element [db {:keys [identifier type]}]
  (str (compile-sql db identifier) " " (compile-sql db type)))

(defmethod compile-sql :query [db ast]
  (str "SELECT " (compile-children db ast)))

(defmethod compile-sql :partition-by [db {:keys [expression]}]
  (str "PARTITION BY " (compile-sql db expression)))

(defmethod compile-sql :print [db {:keys [clause]}]
  (str "PRINT '" (name clause) "'"))

(defmethod compile-sql :relation [db {:keys [aliased-relation]}]
  (compile-sql db aliased-relation))

(defmethod compile-sql :relation-primary [db {:keys [source-name]}]
  (compile-sql db source-name))

(defmethod compile-sql :source-name [db {:keys [source]}]
  (compile-sql db source))

(defmethod compile-sql :table-elements [db {:keys [elements]}]
  (str " (" (comma-separated db elements) ")"))

(defmethod compile-sql :terminate [db {:keys [query]}]
  (str "TERMINATE " (compile-sql db query)))

(defmethod compile-sql :value-expression [db {:keys [primary-expression]}]
  (compile-sql db primary-expression))

(defn- compile-map-value [columns value]
  (str/join ", " (map compile-object (map value columns))))

(defn- compile-seq-value [value]
  (str/join ", " (map compile-object value)))

(defmethod compile-sql :value [db {:keys [columns value]}]
  (str "(" (cond
             (sequential? value)
             (compile-seq-value value)
             (map? value)
             (compile-map-value columns value))
       ")"))

(defmethod compile-sql :values [db {:keys [values]}]
  (str "VALUES " (comma-separated db values)))

(defmethod compile-sql :where [db {:keys [expression]}]
  (str " WHERE " (compile-sql db expression)))

(defmethod compile-sql :window [db {:keys [expressions type]}]
  (str " WINDOW " (compile-sql db type) " " (space-separated db expressions)))

(defmethod compile-sql :window-expression [db {:keys [expression]}]
  (str "(" (str/join " " (map (comp str/upper-case str) expression)) ")"))

(defmethod compile-sql :window-type [db {:keys [type]}]
  (str/upper-case (name type)))

(defmethod compile-sql :with [db {:keys [opts]}]
  (str " WITH ("
       (->> (for [[k v] opts]
              (str (compile-object k) " = '" v "'"))
            (str/join ", ")) ")"))

(defn ast
  "Returns the AST for ``statement``."
  [statement]
  (util/ast statement))

(defn compile-ast
  "Compile the `ast` to SQL string."
  [ast]
  (with-meta [(compile-sql (:db ast) ast)]
    {:sqlingvo.ksql/ast ast}))

(defn- compile-statement
  "Compile the `statement` to SQL string."
  [statement]
  (compile-ast (ast statement)))

(defn sql
  "Compile the `statement` to SQL string."
  [statement]
  (cond
    (and (map? statement) (:op statement))
    (compile-ast statement)
    :else
    (compile-statement statement)))

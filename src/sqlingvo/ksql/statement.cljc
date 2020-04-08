(ns sqlingvo.ksql.statement
  (:require [cats.protocols :as p]
            [clojure.pprint :refer [simple-dispatch]]
            [sqlingvo.ksql.compiler :as compiler]
            [sqlingvo.ksql.db :as db]
            [sqlingvo.ksql.utils :as util]))

(defn- evaluator
  [statement]
  (-> statement util/ast :db :evaluator))

(defn- eval-statement
  [statement]
  (let [{:keys [db] :as ast} (util/ast statement)]
    (db/-eval db ast nil)))

(defrecord Statement [mfn state-context]
  p/Contextual
  (-get-context [_] state-context)

  p/Extract
  (-extract [_] mfn)

  #?(:clj clojure.lang.IDeref :cljs cljs.core/IDeref)
  (#?(:clj deref :cljs -deref)
    [statement]
    (eval-statement statement))

  Object
  (toString [statement]
    (first (compiler/sql statement))))

(defn- printable-object [statement]
  (try (compiler/sql statement)
       (catch #?(:clj Exception :cljs js/Error) e
         "<INVALID SQL>")))

(defn statement?
  "Returns true if `x` is a Statement, otherwise false."
  [x]
  (instance? Statement x))

#?(:clj (defmethod print-method Statement
          [statement writer]
          (print-method (printable-object statement) writer)))

#?(:cljs
   (extend-protocol IPrintWithWriter
     sqlingvo.ksql.statement.Statement
     (-pr-writer [statement writer opts]
       (-pr-writer (printable-object statement) writer opts))))

;; Override deref for pretty printing :/ ???

(defmethod simple-dispatch Statement [statement]
  (pr (printable-object statement)))

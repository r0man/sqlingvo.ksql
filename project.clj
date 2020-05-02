(defproject sqlingvo.ksql "0.1.2-SNAPSHOT"
  :description "A Clojure DSL to build SQL statements for KSQL DB."
  :url "https://github.com/r0man/sqlingvo.ksql"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :deploy-repositories [["releases" :clojars]]
  :aliases {"ci" ["do"
                  ["difftest"]
                  ["doo" "node" "node" "once"]
                  ["doo" "phantom" "none" "once"]
                  ["doo" "phantom" "advanced" "once"]
                  ["lint"]]
            "lint" ["do"  ["eastwood"]]}
  :dependencies [[funcool/cats "2.3.6"]
                 [org.clojure/clojure "1.10.1"]]
  :plugins [[jonase/eastwood "0.3.11"]
            [lein-cljsbuild "1.1.8"]
            [lein-difftest "2.0.0"]
            [lein-doo "0.1.11"]]
  :profiles {:dev {:dependencies [[org.clojure/clojurescript "1.10.597"]
                                  [org.clojure/core.rrb-vector "0.1.1"]
                                  [org.clojure/test.check "1.0.0"]]}
             :provided {:dependencies [[clj-antlr "0.2.5"]]}
             :repl {:source-paths ["dev"]}
             :test {:resource-paths ["test-resources"]}}
  :cljsbuild
  {:builds
   [{:id "none"
     :compiler
     {:main sqlingvo.ksql.test.runner
      :optimizations :none
      :output-dir "target/none"
      :output-to "target/none.js"
      :parallel-build true
      :pretty-print true
      :verbose false}
     :source-paths ["src" "test"]}
    {:id "node"
     :compiler
     {:main sqlingvo.ksql.test.runner
      :optimizations :none
      :output-dir "target/node"
      :output-to "target/node.js"
      :parallel-build true
      :pretty-print true
      :target :nodejs
      :verbose false}
     :source-paths ["src" "test"]}
    {:id "advanced"
     :compiler
     {:main sqlingvo.ksql.test.runner
      :optimizations :advanced
      :output-dir "target/advanced"
      :output-to "target/advanced.js"
      :parallel-build true
      :pretty-print true
      :verbose false}
     :source-paths ["src" "test"]}]})

(defproject clj-zoo-watcher "1.0.10"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.netflix.curator/curator-recipes "1.1.13"]
                 [zookeeper-clj "0.9.2"]]
  :plugins [[lein-swank "1.4.4"]]
  :repl-init clj-zoo-watcher.core
  ; :aot :all
  :warn-on-reflection true
  :jar-exclusions [#"project.clj"]
  :omit-source true)

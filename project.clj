(defproject clj-zoo-watcher "1.0.10"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [zookeeper-clj "0.9.2"]]
  :repl-init clj-zoo-watcher.core
  :aot [clj-zoo-watcher.core clj-zoo-watcher.multi]
  :warn-on-reflection true
  :jar-exclusions [#"project.clj"]
  :omit-source true)

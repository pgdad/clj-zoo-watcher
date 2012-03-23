(defproject clj-zoo-watcher "1.0.2"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [clj-tree-zipper "1.0.0"]
                 [zookeeper-clj "0.9.2"]]
  :repl-init clj-zoo-watcher.core
  :aot [clj-zoo-watcher.core])

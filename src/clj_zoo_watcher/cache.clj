(ns clj-zoo-watcher.cache
  (:import (com.netflix.curator.framework.recipes.cache PathChildrenCache
                                                        PathChildrenCacheListener
                                                        PathChildrenCacheEvent))
  (:require [zookeeper :as zk]
            [clj-zoo-watcher.core :as core]))

(defn- listener
  [added removed updated reset]
  (proxy [com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener] []
    (childEvent [fWork event]
      (println (str "PATH CHILD EVENT: " event " TYPE: " (.getType event)))
      (try (case (str (.getType event))
             "CHILD_ADDED" (added event)
             "CHILD_REMOVED" (removed event)
             "CHILD_UPDATED" (updated event)
             "RESET" (reset)
             :default (println (str "HIT DEAFULT IN CASE: " event)))
           
           (catch Exception e (println (str "CATCH in LISTENER: " e)))))))

(defn cache
  "added, removed, updated, reset are functions (maybe nil) called for events"
  [fWork path added removed updated reset]
  (let [c (PathChildrenCache. fWork path true)
        lc (.getListenable c)]
    (.addListener lc (listener added removed updated reset))
    (.start c)
    c))

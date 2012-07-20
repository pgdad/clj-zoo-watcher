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
      (try (case (str (.getType event))
             "CHILD_ADDED" (added event)
             "CHILD_REMOVED" (removed event)
             "CHILD_UPDATED" (updated event)
             "RESET" (reset)
             :default (println (str "HIT DEAFULT IN CASE: " event)))
           
           (catch Exception e (do (println (str "CATCH in LISTENER: " e " EVENT: " event)) (.printStackTrace e)))))))

(defn cache
  "added, removed, updated, reset are functions (maybe nil) called for events"
  [fWork path added removed updated reset]
  (let [c (PathChildrenCache. fWork path true)
        lc (.getListenable c)]
    (.addListener lc (listener added removed updated reset))
    (try (.start c) (catch Exception e (println (str "START C EX: " e))))
    c))

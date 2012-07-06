(ns clj-zoo-watcher.mapper
  (:require [clj-zoo-watcher.cache :as ca]))

(defn- listener
  [added removed updated reset]
  (proxy [com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener] []
    (childEvent [fWork event]
      (println (str "PATH CHILD EVENT: " event))
      ())))

(defn- init-mapper-cache
  []
  (ref {:bending-reset false
        :m {}}))

(defn- reset-cb
  [m-ref]
  (dosync
   (alter m-ref update-in [:bending-reset] #(identity true))))

(defn- close-cache
  [caches-ref path]
  (.close (@caches-ref path))
  (dosync
   (alter caches-ref dissoc path)))

(defn- rm-cb
  [caches-ref m-ref event]
  (let [event-data (.getData event)
        path (.getPath event-data)]
    (close-cache caches-ref path)
    (dosync
     (if (:bending-reset @m-ref)
       (alter m-ref (init-mapper-cache))
       (alter m-ref update-in [:m] dissoc path)))))

(declare add-cb)

(defn- update-cb
  [caches-ref m-ref data-cb event]
  (add-cb nil caches-ref m-ref data-cb event))

(declare mapper-cache)

(defn- add-cb
  [fWork caches-ref m-ref data-cb event]
  (println (str "IN ADD CB: " event))
  (let [event-data (.getData event)
        path (.getPath event-data)
        data (.getData event-data)
        stat (.getStat event-data)
        fixed-data (if (and data data-cb)
                     (data-cb data)
                     data)]
    (println "AFTER LOOKING AT DATA IN ADD: " fixed-data)
    (dosync
     (when (:bending-reset @m-ref)
       (alter m-ref (init-mapper-cache)))
     (alter m-ref update-in [:m path] #(identity %2) {:data fixed-data :stat stat}))
    (when fWork
      (let [sub-cache (mapper-cache fWork caches-ref m-ref data-cb path)]
        (dosync
         (println (str "ADDING SUB CACHE: " path))
         (alter caches-ref assoc-in [path] sub-cache))))))

(defn mapper-cache
  "make a cache that updates a referenced map as the cache changes"
  ([fWork m-ref data-cb path]
     (let [caches-ref (ref {})]
       (mapper-cache fWork caches-ref m-ref data-cb path)
       caches-ref))
  ([fWork caches-ref m-ref data-cb path]
     (let [ c (ca/cache fWork path
                        (partial add-cb fWork caches-ref m-ref data-cb)
                        (partial rm-cb caches-ref m-ref)
                        (partial update-cb caches-ref m-ref data-cb)
                        (partial reset-cb m-ref))]
       (dosync
        (alter caches-ref assoc-in [path] c))
       c)))

(defn close
  [m-cache]
  (doseq [[path _] @m-cache]
    (close-cache m-cache path)))
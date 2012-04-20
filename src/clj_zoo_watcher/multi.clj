(ns clj-zoo-watcher.multi
  (:require [zookeeper :as zk]
            [clj-zoo-watcher.core :as core]))

(defn- watch-for-children
  [connection m-ref node event]
  (if event
    (println (str "WATCH FOR CHILDREN: " event))
    )
  (zk/children connection node :watcher (partial watch-for-children connection m-ref node)))

(defn- exists-watcher
  [connection m-ref node event]
  (if event
    (println (str "WATCHING MULTI NODE PARENT: " node " :: EVENT :: " event)))
  (println (str "EXISTS WATCHER: " @m-ref "::" node))
  (zk/exists connection node :watcher (partial exists-watcher connection m-ref node)))

(defn- start-watching
  [connection node m-ref]
  (let [exists (exists-watcher connection m-ref node nil)]
    (if exists
      (let [m @m-ref
            children (zk/children connection node)]
        (doseq [child children]
          (dosync
           (alter (:kids-ref m)
                  assoc child (core/watcher connection
                                            (str node "/" child)
                                            (:conn-watcher m)
                                            (:dir-created m)
                                            (:dir-deleted m)
                                            (:file-created m)
                                            (:file-deleted m)
                                            (:file-data-changed m)
                                            (:data-ref m)))))))
  (zk/children connection node :watcher (partial watch-for-children connection m-ref node))))

(defn child-watchers
  [connection
   ;; root is a node
   root
   ;; kids-ref is a ref of a map, keyed by child node name, value is watcher
   kids-ref
   ;; rest of the args are using in the core/watcher
   connwatcher dir-created dir-deleted file-created file-deleted file-data-changed data-ref]
  (let [m-ref (ref {:kids-ref kids-ref
                    :connection connection
                    :conn-watcher connwatcher
                    :root-node root
                    :dir-created dir-created
                    :dir-deleted dir-deleted
                    :file-created file-created
                    :file-deleted file-deleted
                    :file-data-changed file-data-changed
                    :data-ref data-ref})]
    (start-watching connection root m-ref)
    m-ref)
  )

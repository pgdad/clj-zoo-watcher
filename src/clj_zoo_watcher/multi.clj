(ns clj-zoo-watcher.multi
  (:require [zookeeper :as zk]
            [clj-zoo-watcher.core :as core]))

(defn- add-child-to-m-ref
  [m-ref connection node child]
  (dosync
   (alter (:kids-ref @m-ref)
          assoc child (core/watcher connection
                                    (str node "/" child)
                                    (partial (:conn-watcher @m-ref) child)
                                    (partial (:dir-created @m-ref) child)
                                    (partial (:dir-deleted @m-ref) child)
                                    (partial (:file-created @m-ref) child)
                                    (partial (:file-deleted @m-ref) child)
                                    (partial (:file-data-changed @m-ref) child)
                                    (:data-ref @m-ref)))))

(defn- watch-for-children
  "value of prior-children is used to determine if the set of children has changed"
  [connection m-ref node prior-children event]
  (let [children (zk/children connection node)]
    (if (= (:event-type event) :NodeChildrenChanged)
      (let [prev-kids (set prior-children)
            curr-kids (set children)
            new-kids (clojure.set/difference curr-kids prev-kids)
            removed-kids (clojure.set/difference prev-kids curr-kids)]
        ;; add new kids into the mix
        (doseq [child new-kids]
          (add-child-to-m-ref m-ref connection node child))
        ;; remove removed kids from the mix
        (doseq [child removed-kids]
          (dosync
           (alter (:kids-ref @m-ref)
                  dissoc child)))))
    (zk/children connection node
                 :watcher (partial watch-for-children connection m-ref node children))))

(defn- exists-watcher
  [connection m-ref node event]
  (if event
    (println (str "WATCHING MULTI NODE PARENT: " node " :: EVENT :: " event)))
  (zk/exists connection node :watcher (partial exists-watcher connection m-ref node)))

(defn- start-watching
  [connection node m-ref]
  (let [exists (exists-watcher connection m-ref node nil)
        children (if exists (zk/children connection node) '())]
    (if exists
      (doseq [child children]
        (add-child-to-m-ref m-ref connection node child)))
    (zk/children connection node
                 :watcher (partial watch-for-children connection m-ref node children))))

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

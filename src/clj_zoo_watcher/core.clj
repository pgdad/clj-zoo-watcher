(ns clj-zoo-watcher.core
  (:require [zookeeper :as zk]
            [clojure.set]
            [clojure.zip :as z]))

(def ^:dynamic *keepers* "localhost")

(declare con-watcher)

(declare start-watching-root)

(defn watcher
  "client is a zookeeper client connection
root is a zookeeper node path
dir-created is a function that is called when a zookeeper 'directory' node is created
  It is a function of the form (fn [data-ref dir-node] ....) where dir-node is the zookeper path.
  'directory' node is understood to be a persistent zookeeper node
dir-deleted is a function that is called when a zookeeper 'directory' node is deleted
  It is a fuction of the form (fn [data-ref dir-node] ....)
file-created is a function that is called when a zookeeper 'file' node is created
  It is a function of the form (fn [data-ref file-node] ....) where file-node is a the zookeeper path.
  'file' node is understood to be a ephemeral zookeeper node
file-deleted is a function that is called when a zookeeper 'file' node is deleted
  It is a function of the form (fn [data-ref file-node] ...)
file-data-changed is a function that is called when a zookeeper 'file' node data is changed.
  It is a function of the form (fn [data-ref file-node data] ....) where data is the new data content of the node.
  'data' is a java binary array
"
  [client root connwatcher dir-created dir-deleted file-created file-deleted file-data-changed data-ref]
  (let [w {:client client
           :connwatcher connwatcher
           :root root
           :watched-dir-nodes-ref (ref #{})
           :watched-file-nodes-ref (ref #{})
           :dir-created dir-created
           :dir-deleted dir-deleted
           :file-created file-created
           :file-deleted file-deleted
           :file-data-changed file-data-changed
           :data-ref data-ref}
        w-ref (ref w)]
    (zk/register-watcher client (partial con-watcher @w-ref))
    (start-watching-root w-ref root)
    w-ref))

(defn- wclient
  [w]
  (:client @w))

(defn- wroot
  [w]
  (:root @w))

(defn- call-dir-created
  [w dir]
  (let [f (:dir-created @w)]
    (f (:data-ref @w) dir)))

(defn- call-dir-deleted
  [w dir]
  (let [f (:dir-deleted @w)]
    (f (:data-ref @w) dir)))

(defn- call-file-created
  [w file]
  (let [f (:file-created @w)]
    (f (:data-ref @w) file)))

(defn- call-file-deleted
  [w file]
  (let [f (:file-deleted @w)]
    (f (:data-ref @w) file)))

(defn- call-file-data-changed
  [w file data]
  (let [f (:file-data-changed @w)]
    (f (:data-ref @w) file data)))

(defn- persistent?
  [client node]
  (let [data (zk/data client node)]
    (= 0 (:ephemeralOwner (:stat data)))))

(defn- con-watcher
  [watcher event]
  (println (str "CONN WATCHER: " event))
  (let [cwatcher (:connwatcher watcher)]
    (cwatcher event))
  (zk/register-watcher (:client watcher) (partial con-watcher watcher)))

(defn- added-children
  "return entries that exist in node-children but not in z-children"
  [new-children old-children]
  (clojure.set/difference new-children old-children))

(defn- removed-children
  "return return entries that exist in z-children but not in node-children"
  [new-children old-children]
  (clojure.set/difference old-children new-children))

(defn- file-data-watcher
  [watcher-ref event]
  (let [path (:path event)
        event-type (:event-type event)
        keeper-state (:keeper-state event)]
    (if (and (= event-type :None) (= keeper-state :SyncConnected))
      nil
      (if-not (= event-type :NodeDeleted)
        (try
          (let [data (:data (zk/data (wclient watcher-ref) path
                                     :watcher (partial file-data-watcher
                                                       watcher-ref)))]
            (call-file-data-changed watcher-ref path data))
          (catch Exception ex
            (do
              (println (str "DATA WATCHER EXCEPTION: " ex))
              (. ex printStackTrace)
              (println "==================")
              (println (str "EVENT: " event)))))
        ))
    ))

(defn- add-kid
  [watcher-ref directory node]
  (dosync
   (let [watcher @watcher-ref
         watched-dir-nodes-ref (:watched-dir-nodes-ref watcher)
         watched-file-nodes-ref (:watched-file-nodes-ref watcher)]
     
     (if directory
       (alter watched-dir-nodes-ref conj node)
       (alter watched-file-nodes-ref conj node))
     
     (if-not directory
       (let [data (:data (zk/data (wclient watcher-ref) node
                                  :watcher (partial file-data-watcher watcher-ref)))])))))

(defn- remove-kid
  [watcher-ref node-path]
  (dosync
   (let [watcher @watcher-ref
         watched-dir-nodes-ref (:watched-dir-nodes-ref watcher)
         watched-file-nodes-ref (:watched-file-nodes-ref watcher)]
     
     (if (contains? @watched-dir-nodes-ref node-path)
       (do
         (alter watched-dir-nodes-ref disj node-path)
         (call-dir-deleted watcher-ref node-path))
       (do
         (alter watched-file-nodes-ref disj node-path)
         (call-file-deleted watcher-ref node-path))))))

(declare start-watching-children)

(defn- child-watcher
  [watcher-ref node event]
  (let [client (wclient watcher-ref)
        children (zk/children (wclient watcher-ref) node :watcher (partial child-watcher watcher-ref node))]
    (case (:event-type event)
      :NodeChildrenChanged
      (do
        (let [root (wroot watcher-ref)
              old-kids (clojure.set/union
                        @(:watched-dir-nodes-ref @watcher-ref)
                        @(:watched-file-nodes-ref @watcher-ref))
              
              n-kids (set children)
              new-kids (set (map #(str node "/" %) children))
              additions (added-children new-kids old-kids)
              removals (removed-children new-kids old-kids)]
          (doseq [kid removals]
            (try
              (remove-kid watcher-ref (str node "/" kid))
              (catch Exception ex
                (do
                  (println (str "EXCEPTION IN NODE CHANGED: " ex))
                  (.printStackTrace ex)))))
          (doseq [kid additions]
            (let [kid-node (str node "/" kid)
                  directory (persistent? client kid-node)]
              (add-kid watcher-ref directory kid-node)
              (if directory
                (call-dir-created watcher-ref kid-node)
                (call-file-created watcher-ref kid-node))
              (start-watching-children watcher-ref kid-node)))))
      :NodeDeleted
      (remove-kid watcher-ref (:path event))

      (println (str "CHILD WATCHER UNCAUGHT EVENT: " event)))))

(defn- start-watching-children
  [watcher-ref node]
  (let [client (wclient watcher-ref)
        children (zk/children client node
                              :watcher (partial child-watcher watcher-ref node))]
    (doseq [child children]
      (let [child-node (str node "/" child)
            root (wroot watcher-ref)
            watched-dir-nodes-ref (:watched-dir-nodes-ref @watcher-ref)
            watched-file-nodes-ref (:watched-file-nodes-ref @watcher-ref)]
        ;; if we not already watching this node
        (if-not (or (contains? @watched-dir-nodes-ref child-node)
                    (contains? @watched-file-nodes-ref child-node))
          (dosync
           (let [watcher @watcher-ref
                 directory (persistent? (wclient watcher-ref) child-node)]
             (add-kid watcher-ref directory child-node)
             (if directory
               (call-dir-created watcher-ref child-node)
               (call-file-created watcher-ref child-node))
             (start-watching-children watcher-ref child-node))))))))

(defn- start-watching-root
  [watcher-ref node & event]
  (let [exists (zk/exists (wclient watcher-ref) node
                          :watcher (partial start-watching-root
                                            watcher-ref node))]
    (if (not (= 0  (count event)))
      (case (:event-type (first event))
        (println (str "START WATCHING ROOT UNCAUGHT EVENT: " (first event)))))
    (if exists
      (do (start-watching-children watcher-ref node)))))

(defn- test-connwatcher
  [event]
  (println (str "TEST CONNWATCHER: " event)))

(defn cdo0
  []
  (let [client (zk/connect *keepers*)
        node "/testnode"
        w (watcher client "/testnode"
                   test-connwatcher
                   (fn [dir-node] (println (str "DIRECTORY CREATED CALLBACK: " dir-node)))
                   (fn [dir-node] (println (str "DIRECTORY DELETED CALLBACK: " dir-node)))
                   (fn [file-node] (println (str "NODE CREATED CALLBACK: " file-node)))
                   (fn [file-node] (println (str "NODE DELETED CALLBACK: " file-node)))
                   (fn [file-node data]
                     (println (str "NODE DATA CALLBACK: "
                                   file-node " DATA: " (String. data "UTF-8")))))]
    w))

(defn cdo
  [watcher-ref]
  (zk/create (wclient watcher-ref) "/testnode/Z1" :persistent? true)
  (zk/create (wclient watcher-ref) "/testnode/Z2" :persistent? true)
  (zk/create (wclient watcher-ref) "/testnode/Z1/d1" :persistent? true)
  (zk/create (wclient watcher-ref) "/testnode/Z2/d2" :persistent? true)
  (zk/create (wclient watcher-ref) "/testnode/Z1/d1/file1.txt" :persistent? false)
  (zk/create (wclient watcher-ref) "/testnode/Z2/d2/file2.txt" :persistent? false)
  watcher-ref)


(defn cdo2
  [watcher-ref]
  (zk/delete-all (wclient watcher-ref) "/testnode/Z2")
  (zk/delete-all (wclient watcher-ref) "/testnode/Z1"))

(defn data-example
  [watcher-ref]
  (let [version (:version (zk/exists (wclient watcher-ref) "/testnode/Z2/d2/file2.txt"))]
    (zk/set-data (wclient watcher-ref) "/testnode/Z2/d2/file2.txt"
                 (.getBytes "hello world2" "UTF-8") version)))

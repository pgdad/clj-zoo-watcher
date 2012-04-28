(ns clj-zoo-watcher.core
  (:require [zookeeper :as zk]
            [clj-tree-zipper.core :as tz]
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
           :zipper (tz/tree-zip (clj_tree_zipper.core.Directory. "/" nil))
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

(defn- wzipper
  [w]
  (:zipper @w))

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

(defn- node-zipper-path
  "given root node and zk node, return 'zipper-field path'"
  [root node]
  (if (= root node)
    '("/")
    (let [remove-root (clojure.string/replace-first node (str root "/") "")
          zipper-path (cons "/" (seq (clojure.string/split remove-root
                                                           (re-pattern "/"))))]
      zipper-path)))

(defn- added-children
  "return entries that exist in node-children but not in z-children"
  [node-children z-children]
  (clojure.set/difference node-children z-children))

(defn- removed-children
  "return return entries that exist in z-children but not in node-children"
  [node-children z-children]
  (clojure.set/difference z-children node-children))

(defn- z-named-children
  [zipper zipper-path]
  (let [z-node (tz/find-path zipper zipper-path)
        z-child-nodes (z/children z-node)]
    (map (fn [child] (:name child)) z-child-nodes)))

(defn- zipper-element
  [name directory]
  (if directory
    (clj_tree_zipper.core.Directory. name nil)
    (clj_tree_zipper.core.File. name)))

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
        (if-not (:path event)
          (println (str "FILE DATA WATCHER PATHLESS EVENT: " event))
          (println (str "FILE DATA WATCHER UNHANDLED EVENT: " event)))
        ))
    ))

(defn- add-z-kid
  [watcher-ref parent-z-path name directory node]
  (dosync
   (let [watcher (ensure watcher-ref)
         zipper (wzipper watcher-ref)
         z-element (zipper-element name directory)
         z-kids (set (z-named-children zipper parent-z-path))
         ;; only create a new tree if the element does not exist already
         contains (contains? z-kids name)
         new-tree (if-not contains
                    (tz/add-at-path zipper parent-z-path z-element)
                    nil) ]
     (if new-tree
       (do
         (alter watcher-ref (fn [& args] (assoc watcher :zipper new-tree)))
         (if-not directory
           (let [data (:data (zk/data (wclient watcher-ref) node
                                      :watcher (partial file-data-watcher watcher-ref)))])))))))

(defn- remove-z-kid
  [watcher-ref parent-z-path name node-path]
  (dosync
   (try
     (let [watcher (ensure watcher-ref)
           zipper (wzipper watcher-ref)
           zipper-path (flatten (list parent-z-path name))
           ])
     (catch Exception ex (println (str "EXECTION IN BRANCH:" ex))))
   (let [watcher (ensure watcher-ref)
         zipper (wzipper watcher-ref)
         zipper-path (flatten (list parent-z-path name))
         branch? (z/branch? (tz/find-path zipper zipper-path))
         new-tree (tz/remove-path zipper zipper-path)]
     (if new-tree
       (do
         (alter watcher-ref (fn [& args] (assoc watcher :zipper new-tree)))
         (if branch?
           (call-dir-deleted watcher-ref node-path)
           (call-file-deleted watcher-ref node-path)))))))

(declare start-watching-children)

(defn- child-watcher
  [watcher-ref node event]
  (let [children (zk/children (wclient watcher-ref) node :watcher (partial child-watcher watcher-ref node))]
    (case (:event-type event)
      :NodeChildrenChanged
      (do
        (let [root (wroot watcher-ref)
              client (wclient watcher-ref)
              zipper-path (node-zipper-path root node)
              z-kids (set (z-named-children (wzipper watcher-ref) zipper-path))
              n-kids (set children)
              additions (added-children n-kids z-kids)
              removals (removed-children n-kids z-kids)]
          (doseq [kid removals]
            (try
              (remove-z-kid watcher-ref zipper-path
                            kid  (str node "/" kid))
              (catch Exception ex (println (str "EXCEPTION IN NODE CHANGED: " ex)))))
          (doseq [kid (added-children n-kids z-kids)]
            (let [kid-node (str node "/" kid)
                  directory (persistent? client kid-node)]
              (add-z-kid watcher-ref zipper-path kid directory kid-node)
              (if directory
                (call-dir-created watcher-ref kid-node)
                (call-file-created watcher-ref kid-node))
              (start-watching-children watcher-ref kid-node)))))
      :NodeDeleted
      (do
        (let [zipper-path (node-zipper-path (wroot watcher-ref) (:path event))
              parent-z-path (drop-last zipper-path)
              kid (last zipper-path)]
          (remove-z-kid watcher-ref parent-z-path kid node)))

      (println (str "CHILD WATCHER UNCAUGHT EVENT: " event)))))

(defn- zipper-path-exists
  [watcher-ref path]
  (tz/find-path (wzipper watcher-ref) path))

(defn- start-watching-children
  [watcher-ref node]
  (let [client (wclient watcher-ref)
        children (zk/children client node
                              :watcher (partial child-watcher watcher-ref node))]
    (doseq [child children]
      (let [child-node (str node "/" child)
            root (wroot watcher-ref)
            z-path (node-zipper-path root child-node)]
        (if-not (zipper-path-exists watcher-ref z-path)
          (dosync
           (let [watcher (ensure watcher-ref)
                 zipper (wzipper watcher-ref)
                 directory (persistent? (wclient watcher-ref) child-node)]
             (add-z-kid watcher-ref (drop-last z-path)
                        child directory child-node)
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

(defn print-tree
  [original]
  (loop [loc original]
    (if-not (z/end? loc)
      (recur (z/next (do
                       (println (reverse (tz/path-up loc)))
                       loc))))))


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
  (dosync   (let [watcher (ensure watcher-ref)] (print-tree (wzipper watcher-ref))))
  watcher-ref)


(defn cdo2
  [watcher-ref]
  (zk/delete-all (wclient watcher-ref) "/testnode/Z2")
  (zk/delete-all (wclient watcher-ref) "/testnode/Z1")
  (dosync   (let [watcher (ensure watcher-ref)] (print-tree (wzipper watcher-ref)))))

(defn data-example
  [watcher-ref]
  (let [version (:version (zk/exists (wclient watcher-ref) "/testnode/Z2/d2/file2.txt"))]
    (zk/set-data (wclient watcher-ref) "/testnode/Z2/d2/file2.txt"
                 (.getBytes "hello world2" "UTF-8") version)))

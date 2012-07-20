(ns clj-zoo-watcher.watchedMapper
  (:require [clj-zoo-watcher.mapper :as m]))

(defn watched-cache
  "make a mapper-cache and watch it for changes"
  [fWork m-ref data-cb path k f]
  (let [watched-cache (m/mapper-cache fWork m-ref data-cb path)]
    (add-watch watched-cache f)
    watched-cache))

(defn close
  [watched-cache]
  (m/close (:mapper @watched-cache)))
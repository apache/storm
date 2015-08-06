(ns backtype.storm.cgroup.core.net-cls-cgroup
  (:use [com.jd.bdp.constant])
  (:require [backtype.storm.util :as util]))

(defn- get-dir [dir file-dir]
  (util/normalize-path (str dir file-dir)))

(defn get-tasks [dir]
  (if-let [string-tasks (util/read-file (get-dir dir TASKS))]
    (into #{} (map #(util/parse-int %) string-tasks))))

(defn add-task [dir task]
  (util/append-line (get-dir dir TASKS) (str task)))

(defn- to-hex [num]
  (let [hex (str num)
        l (.length hex)
        hex (if (> l 4) (.substring hex (- l 4 1) l) hex)
        sb (StringBuilder.)]
    (loop [len l]
      (when (< len 4)
        (.append sb \0)
        (recur (inc len))))
    (.append sb hex)
    (.toString sb)))

(defn set-class-id [dir major minor]
  (util/append-line (get-dir dir NET-CLS-CLASSID) (apply str (map to-hex [major minor]))))

(defn get-class-id [dir]
  (let [output (first (util/read-file (get-dir dir NET-CLS-CLASSID)))
        l (.length output)]
    {:major (util/parse-int (.substring output 0 (- l 4))) :minor (util/parse-int (.substring (- l 4)))}))
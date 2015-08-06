(ns backtype.storm.cgroup.core.cpu-cgroup
  (:use [backtype.storm.cgroup.constant])
  (:require [backtype.storm.util :as util]))

(defn- get-dir [dir file-dir]
  (util/normalize-path (str dir file-dir)))

(defn get-tasks [dir]
  (if-let [string-tasks (util/read-file (get-dir dir TASKS))]
    (into #{} (map #(util/parse-int %) string-tasks))))

(defn add-task [dir task]
  (util/append-line (get-dir dir TASKS) (str task)))

(defn set-cpu-shares [dir weight]
  (util/append-line (get-dir dir CPU-SHARES) (str weight)))

(defn get-cpu-shares [dir]
  (util/parse-int (first (util/read-file (get-dir dir CPU-SHARES)))))

(defn set-cpu-rt-runtime-us [dir us]
  (util/append-line (get-dir dir CPU-RT-RUNTIME-US) (str us)))

(defn get-cpu-rt-runtime-us [dir]
  (Long/parseLong (first (util/read-file (get-dir dir CPU-RT-RUNTIME-US)))))

(defn set-cpu-rt-period-us [dir us]
  (util/append-line (get-dir dir CPU-RT-PERIOD-US) (str us)))

(defn get-cpu-rt-period-us [dir]
  (Long/parseLong (first (util/read-file (get-dir dir CPU-RT-PERIOD-US)))))

(defn set-cpu-cfs-period-us [dir us]
  (util/append-line (get-dir dir CPU-CFS-PERIOD-US) (str us)))

(defn get-cpu-cfs-period-us [dir]
  (Long/parseLong (first (util/read-file (get-dir dir CPU-CFS-PERIOD-US)))))

(defn set-cpu-cfs-quota-us [dir us]
  (util/append-line (get-dir CPU-CFS-QUOTA-US) (str us)))

(defn get-cpu-cfs-quota-us [dir]
  (Long/parseLong (first (util/read-file (get-dir dir CPU-CFS-QUOTA-US)))))

(defn get-cpu-stat [dir]
  (let [lines (util/read-file (get-dir dir CPU-STAT))
        lines (map #(util/parse-int (nth (.split % " ") 1)) lines)]
    {:nr-periods (nth lines 0)
     :nr-throttled (nth lines 1)
     :throttled-time (nth lines 2)}))
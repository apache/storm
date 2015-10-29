(ns backtype.storm.cgroup.core.memory-cgroup
  (:use [backtype.storm.cgroup.constant])
  (:require [backtype.storm.util :as util]))

(defn- get-dir [dir file-dir]
  (util/normalize-path (str dir file-dir)))

(defn get-tasks [dir]
  (if-let [string-tasks (util/read-file (get-dir dir TASKS))]
    (into #{} (map #(util/parse-int %) string-tasks))))

(defn get-stat [dir]
  (let [line (first (util/read-file (get-dir dir MEMORY-STAT)))
        splits (map #(Long/parseLong %) (.split line "\n"))]
    {:cache-size (nth splits 0) :rss-size (nth splits 1) :mapped-file-size (nth splits 2) :pgpgin-num (nth splits 3) :pgpgout-num (nth splits 4) :swap-size (nth splits 5) :inactive-anon-size (nth splits 6) :active-anon-size (nth splits 7) :inactive-file-size (nth splits 8) :active-file-size (nth splits 9) :unevictable-size (nth splits 10) :hierarchical-memory-limit-size (nth splits 11) :hierarchical-memsw-limit-size (nth splits 12) :total-cache-size (nth splits 13) :total-rss-size (nth splits 14) :total-mapped-file-size (nth splits 15) :total-pgpgin-num (nth splits 16) :total-pgpgout-num (nth splits 17) :total-swap-size (nth splits 18) :total-inactive-anon-size (nth splits 19) :total-active-anon-size (nth splits 20) :total-inactive-file-size (nth splits 21) :total-active-file-size (nth splits 22) :total-unevictable-size (nth splits 23) :total-hierarchical-memory-limit-size (nth splits 24) :total-hierarchical-memsw-limit-size (nth splits 25)}))

(defn get-physical-usage [dir]
  (Long/parseLong (first (util/read-file (get-dir dir MEMORY-USAGE-IN-BYTES)))))

(defn get-with-swap-usage [dir]
  (Long/parseLong (first (util/read-file (get-dir dir MEMORY-MEMSW-USAGE-IN-BYTES)))))

(defn get-max-physical-usage [dir]
  (Long/parseLong (first (util/read-file (get-dir dir MEMORY-MAX-USAGE-IN-BYTES)))))

(defn get-max-with-swap-usage [dir]
  (Long/parseLong (first (util/read-file (get-dir dir MEMORY-MEMSW-USAGE-IN-BYTES)))))

(defn set-physical-usage-limit [dir value]
  (util/append-line (get-dir dir MEMORY-LIMIT-IN-BYTES) (str value)))

(defn get-physical-usage-limit [dir]
  (Long/parseLong (first (util/read-file (get-dir dir MEMORY-LIMIT-IN-BYTES)))))

(defn set-with-swap-usage-limit [dir value]
  (util/append-line (get-dir dir MEMORY-MEMSW-LIMIT-IN-BYTES) (str value)))

(defn get-with-swap-usage-limit [dir]
  (Long/parseLong (first (util/read-file (get-dir dir MEMORY-MEMSW-LIMIT-IN-BYTES)))))

(defn get-physical-fail-count [dir]
  (util/parse-int (first (util/read-file (get-dir dir MEMORY-FAILCNT)))))

(defn get-with-swap-fail-count [dir]
  (util/parse-int (first (util/read-file (get-dir dir MEMORY-MEMSW-FAILCNT)))))

(defn clear-force-empty [dir]
  (util/append-line (get-dir dir MEMORY-FORCE-EMPTY) "0"))

(defn set-swappiness [dir value]
  (util/append-line (get-dir dir MEMORY-SWAPPINESS) (str value)))

(defn get-swappiness [dir]
  (util/parse-int (first (util/read-file (get-dir dir MEMORY-SWAPPINESS)))))

(defn set-use-hierarchy [dir flag]
  (util/append-line (get-dir dir MEMORY-USE-HIERARCHY) (if flag "1" "0")))

(defn is-use-hierarchy? [dir]
  (> (util/parse-int (first (util/read-file (get-dir dir MEMORY-USE-HIERARCHY)))) 0))

(defn set-oom-control [dir flag]
  (util/append-line (get-dir dir MEMORY-OOM-CONTROL) (if flag "1" "0")))

(defn is-oom-control? [dir]
  (> (util/parse-int (nth (.split (first (.split (first (util/read-file (get-dir dir MEMORY-OOM-CONTROL))) "\n")) "[\\s]") 1)) 0))

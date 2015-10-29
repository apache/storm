(ns backtype.storm.cgroup.cgroup-manager
  (:import [java.io File BufferedReader FileReader])
  (:use [backtype.storm log]
        [backtype.storm.cgroup constant])
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

(defn set-physical-usage-limit [dir value]
  (util/append-line (get-dir dir MEMORY-LIMIT-IN-BYTES) (str value)))

(defn set-with-swap-usage-limit [dir value]
  (util/append-line (get-dir dir MEMORY-MEMSW-LIMIT-IN-BYTES) (str value)))

(defn set-oom-control [dir flag]
  (util/append-line (get-dir dir MEMORY-OOM-CONTROL) (if flag "0" "1")))

(defn analyse-subsystem [subsystems]
  (loop [systems #{}
         rest-systems (.split subsystems ",")]
    (if-let [system (keyword (first rest-systems))]
      (recur (if (contains? SUBSYSTEM-TYPE system)
               (conj systems system)
               systems)
        (rest rest-systems))
      systems)))

(defn get-hierarchies []
  (with-open [^FileReader file (FileReader. MOUNT-STATUS-FILE)
              ^BufferedReader br (BufferedReader. file)]
    (loop [hierarchies {}
           line (.readLine br)]
      (if line
        (let [fields (.split line " ")]
          (if (= (get fields 2) "cgroup")
            (let [name (get fields 0)
                  type (get fields 3)
                  dir (get fields 1)
                  hierarchy {:name name :subsystems (analyse-subsystem type) :dir dir :parent nil :is-root? true}]
              (recur (conj hierarchies {type hierarchy}) (.readLine br)))
            (recur hierarchies (.readLine br))))
        (vals hierarchies)))))

(defn busy [subsystem]
  (loop [_hierarchy (get-hierarchies)]
    (if-let [hierarchy (first _hierarchy)]
      (if (contains? (:subsystems hierarchy) subsystem)
        hierarchy
        (recur (rest _hierarchy))))))

(defn check-cgroup
  "return true means cgroup service is ok, else means that cgroup hasn't been install or hasn't been started."
  [conf]
  (log-message "checking cgroup mode")
  (if (true? (conf CGROUP-ENABLE))
    (if-not (.exists (File. CGROUP-STATUS-FILE))
      (log-error "Cgroup error, please check /proc/cgroups, maybe hasn't been started.")
      (if-let [root-dir (conf CGROUP-ROOT-DIR)]
        (let [_files (map #(File. (util/normalize-path (str % File/separator root-dir))) [CPU-HIERARCHY-DIR MEMORY-HIERARCHY-DIR])]
          (loop [files _files]
            (if-let [file (first files)]
              (if-not (.exists file)
                (log-error (.getAbsolutePath file) " is not existing.")
                (recur (rest files)))
              true)))))))

(defn mounted [hierarchy]
  (let [dir (:dir hierarchy)
        name (:name hierarchy)]
    (if (util/exists-dir? dir)
      (loop [a_hierarchy (get-hierarchies)]
        (if-let [_hierarchy (first a_hierarchy)]
          (if (and (= dir (:dir _hierarchy)) (= name (:name _hierarchy)))
            _hierarchy
            (recur (rest a_hierarchy))))
        ))))

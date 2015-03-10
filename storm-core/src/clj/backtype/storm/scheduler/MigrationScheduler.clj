(ns backtype.storm.scheduler.MigrationScheduler
  (:use [backtype.storm util config log])
  (:require [backtype.storm.scheduler.DefaultScheduler :as DefaultScheduler])
  (:require [backtype.storm [zookeeper :as zk] [cluster :as cluster]])
  (:require [clojure.set :as set])
  (:import [backtype.storm.utils Utils])
  (:import [java.util HashSet Set List LinkedList ArrayList Map HashMap])
  (:import [backtype.storm.scheduler IScheduler Topologies
            Cluster TopologyDetails WorkerSlot SchedulerAssignment
            EvenScheduler ExecutorDetails])
  (:gen-class
    :init init
    :constructors {[] []}
    :state state
    :implements [backtype.storm.scheduler.IScheduler]))

(defn -init []
  [[] (atom {})])

(defn -prepare [this conf]
  (let [zk (zk/mk-client conf
                         (conf STORM-ZOOKEEPER-SERVERS)
                         (conf STORM-ZOOKEEPER-PORT)
                         :root (conf STORM-ZOOKEEPER-ROOT))]
    (swap! (.state this) into {:conf conf :zk zk})))

(defn get-zk-assignment [cluster topology-id zk]
  (let [assignment-data (zk/get-data zk (cluster/assignment-path topology-id) false)]
    (if assignment-data (Utils/deserialize assignment-data))))

(defn get-alive-assigned-node+port->executors [existing-assignment]
  (let [executor->node+port (:executor->node+port existing-assignment)
        alive-assigned (map-val shuffle (reverse-map executor->node+port))]
    alive-assigned))

(defn sort-slots [all-slots]
  (let [split-up (sort-by count > (vals (group-by first all-slots)))]
    (apply interleave-all split-up)))

(defn reverse-slot->executors [slot->executors]
  (->> slot->executors
       (map (fn [[k v]] (map #(vector % k) v)))
       (apply concat)
       (into {})))

;; TODO: executors may change, slots may die
(defn schedule-scale-up [^TopologyDetails topology ^Cluster cluster alive-assigned diff]
  (let [all-executors (->> topology
                           .getExecutors
                           (map #(vector (.getStartTask %) (.getEndTask %)))
                           set)
        available-slots (->> (.getAvailableSlots cluster)
                             (map #(vector (.getNodeId %) (.getPort %)))
                             set)
        alive-assigned (filter-key available-slots alive-assigned)
        new-slots (->> available-slots
                       (#(set/difference % (set (keys alive-assigned))))
                       sort-slots
                       (take diff))
        _ (log-message "available-slots " (vec available-slots))
        _ (log-message "alive-assigned: " alive-assigned)
        _ (log-message "new-slots: " (vec new-slots))
        total-slots-to-use (+ (count alive-assigned) (count new-slots))
        num-executors (count all-executors)
        avg (quot num-executors total-slots-to-use)
        remainder (rem num-executors total-slots-to-use)
        _ (log-message "diff: " diff " " total-slots-to-use " " num-executors " " avg " " remainder)
        keep-assignment (->> (concat (repeat remainder (+ avg 1)) (repeat avg))
                             (map (fn [[k v] n] [k (take n v)]) alive-assigned)
                             reverse-slot->executors)
        _ (log-message "keep-assignment: " keep-assignment)
        reassign-executors (sort (set/difference all-executors
                                                 (set (keys keep-assignment))))
        reassignment (->> (repeat-seq (count reassign-executors) new-slots)
                          (map vector reassign-executors)
                          (into {}))
        _ (log-message "reassignment: " reassignment)]
    (into keep-assignment reassignment)))

(defn schedule-scale-down [^TopologyDetails topology ^Cluster cluster alive-assigned diff]
  (let [slots (set (keys alive-assigned))
        keep-slots (set (take (.getNumWorkers topology) slots))
        _ (log-message "keep-slots: " keep-slots)
        keep-assignment (reverse-slot->executors (filter-key keep-slots alive-assigned))
        _ (log-message "keep-assignment: " keep-assignment)
        drop-slots (set/difference slots keep-slots)
        _ (log-message "drop-slots: " drop-slots)
        reassign-executors (apply concat (map alive-assigned drop-slots))
        _ (log-message "reassign-executors " (vec reassign-executors))
        reassignment (->> (repeat-seq (count reassign-executors) keep-slots)
                          (map vector reassign-executors)
                          (into {}))
        _ (log-message "reassignment " reassignment)]
    (into keep-assignment reassignment)))

(defn schedule-migration [^TopologyDetails topology ^Cluster cluster zk]
  (let [topology-id (.getId topology)
        existing-assignment (get-zk-assignment cluster topology-id zk)]
    ;; only shedules migrating topologies: present in zk but missing in cluster
    (if (and existing-assignment (not (.getAssignmentById cluster topology-id)))
      (let [alive-assigned (get-alive-assigned-node+port->executors existing-assignment)
            slots-diff (- (.getNumWorkers topology) (count alive-assigned))]
            (if (> slots-diff 0)
              (schedule-scale-up topology cluster alive-assigned slots-diff)
              (schedule-scale-down topology cluster alive-assigned slots-diff))))))

(defn -schedule [this ^Topologies topologies ^Cluster cluster]
  (let [state @(.state this)
        needs-scheduling-topologies (.needsSchedulingTopologies cluster topologies)]
    (doseq [^TopologyDetails topology needs-scheduling-topologies
            :let [topology-id (.getId topology)
                  new-assignment (schedule-migration topology cluster (state :zk))]]
      (if new-assignment
        (do
          (log-message "new-assignment: " (reverse-map new-assignment))
          (doseq [[node+port executors] (reverse-map new-assignment)
                  :let [^WorkerSlot slot (WorkerSlot. (first node+port) (last node+port))
                        executors (for [[start-task end-task] executors]
                                    (ExecutorDetails. start-task end-task))]]
            (.assign cluster slot topology-id executors)))
        (DefaultScheduler/default-schedule (Topologies. {topology-id topology}) cluster)))))

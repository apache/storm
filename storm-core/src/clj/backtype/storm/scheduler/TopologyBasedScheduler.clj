(ns backtype.storm.scheduler.TopologyBasedScheduler
  (:use [backtype.storm util])
  (:import [backtype.storm.scheduler IScheduler Topologies
                                     Cluster TopologyDetails WorkerSlot ExecutorDetails]
           (backtype.storm.utils ThriftTopologyUtils))
  (:require [backtype.storm.scheduler.EvenScheduler :as EvenScheduler]
            [clojure.set :as set])
  (:gen-class
    :implements [backtype.storm.scheduler.IScheduler]))

(defn -prepare [this conf]
  )

;;construct the dependency graph
(defn- mk-component-dependency-graph
  [topology]
  (let [component-ids (ThriftTopologyUtils/getComponentIds topology)]
    (into
      {}
      (for [component-id component-ids]
        (let [common-component (ThriftTopologyUtils/getComponentCommon topology component-id)
              inputs (.get_inputs common-component)
              stream-ids (.keySet inputs)
              input-components (->> stream-ids (map (memfn .get_componentId)) set)]
          [component-id input-components])))))

(defn- mk-executors-graph
  [component-graph executor-component reassign-executors]
  (let [component->executors (reverse-map executor-component)
        nodes reassign-executors
        graph (->> nodes
                   (map (fn [node]
                          (for [[component executors] component-graph :when (contains? executors node)]
                            [[node] (->> (component->executors component) (filter #(contains? nodes %)) (map vector))])))
                   (into {}))]
    graph))

(defn- karger-min-cut
  [graph num-partitions]
  (let [all-edges (for [[start-node end-nodes] graph]
                    (for [end-node end-nodes] [start-node end-node]))
        all-nodes (let [start-nodes (keys graph)
                        end-nodes (concat (vals graph))]
                        (set (concat start-nodes end-nodes)))]
    (if (<= (count all-nodes) num-partitions)
      all-nodes
      (let [random-edge (rand-nth all-edges)
            start-node (first random-edge)
            end-node (last random-edge)
            new-node (concat start-node end-node)
            partial-graph (for [[node end-nodes] (filter-key #(complement #{start-node end-node}) graph)]
                            [node (if (pos? (count (filter #{start-node end-node} end-nodes)))
                                    (assoc (filter #(complement #{start-node end-node}) end-nodes) new-node)
                                    end-nodes)])
            new-graph (let [end-nodes-for-new (set (concat (graph start-node) (graph end-node)))]
                        (merge partial-graph {new-node end-nodes-for-new}))]
        (recur new-graph num-partitions)))))

;;Now think the allocation process as the Min-cut problem that we should minimize the flow while partition this graph into
;;predefined workers
(defn- schedule-topology
  [^TopologyDetails topology ^Cluster cluster]
  (let [topology-id (.getId topology)
        raw-topology (.getTopology topology)
        dag (mk-component-dependency-graph raw-topology)
        available-slots (->> (.getAvailableSlots cluster)
                             (map #(vector (.getNodeId %) (.getPort %))))
        executor->component (.getExecutorToComponent topology)
        all-executors (->> executor->component keys
                           (map #(vector (.getStartTask %) (.getEndTask %))))
        alive-assigned (EvenScheduler/get-alive-assigned-node+port->executors cluster topology-id)
        total-slots-to-use (min (.getNumWorkers topology)
                                (+ (count available-slots) (count alive-assigned)))
        reassign-slots (take (- total-slots-to-use (count alive-assigned))
                             (EvenScheduler/sort-slots available-slots))
        reassign-executors (set/difference (set all-executors) (set (apply concat (vals alive-assigned))))
        executors-graph (mk-executors-graph dag executor->component reassign-executors)
        executors-partitions (karger-min-cut executors-graph (count reassign-slots))
        reassignment (let [executors->slot (map executors-partitions reassign-slots)
                                 executor->slot (into {}
                                                      (for [[executors slot] executors->slot]
                                                        (for [executor executors]
                                                          [executor slot])))]
                             executor->slot)]
    reassignment))

(defn -scheduler [this ^Topologies toplogies ^Cluster cluster]
  (let [needs-scheduling-topolgoies (.needsScheduling cluster toplogies)]
        (doseq [^TopologyDetails topology needs-scheduling-topolgoies
                :let [topology-id (.getId topology)
                      new-assignment (schedule-topology topology cluster)
                      node+port->executors (reverse-map new-assignment)]]
          (doseq [[node+port executors] node+port->executors
                  :let [^WorkerSlot slot (WorkerSlot. (first node+port) (last node+port))
                        executors (for [[start-task end-task] executors] (ExecutorDetails. start-task end-task))]]
            (.assign cluster slot topology-id executors)))))


;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns org.apache.storm.stats-test
  (:use [clojure test])
  (:import [org.apache.storm.scheduler WorkerSlot])
  (:import [org.apache.storm.generated WorkerResources])
  (:require [org.apache.storm [stats :as stats]]))

(defn- make-topo-info-no-beats 
  []
  {:storm-name "testing", 
   :assignment {:executor->node+port {[1 3] ["node" 1234] 
                                      [4 4] ["node" 1234]}
                :node->host {"node" "host"}}})

(defn- make-topo-info
  []
  (merge 
    {:beats {[1 3] {:uptime 6}
             [4 4] {:uptime 6}}}
    {:task->component {1 "exclaim1", 2 "__sys", 3 "exclaim1", 4 "__sys2"}}
    (make-topo-info-no-beats)))

(defn- make-worker-resources
  []
  (doto (WorkerResources.)
    (.set_mem_on_heap 3)
    (.set_mem_off_heap 4)
    (.set_cpu 5)))

(deftest agg-worker-populates-worker-summary
  (let [storm-id "foo"
        topo-info (make-topo-info)
        worker->resources {(WorkerSlot. "node" 1234) (make-worker-resources)}
        include-sys? true 
        user-authorized true 
        worker-summaries (stats/agg-worker-stats storm-id 
                                                 topo-info 
                                                 worker->resources 
                                                 include-sys? 
                                                 user-authorized)]
    (let [summ (first worker-summaries)
          comps (.get_component_to_num_tasks summ)]
      (is (= 1 (count worker-summaries)))
      (is (= "host" (.get_host summ)))
      (is (= 6 (.get_uptime_secs summ)))
      (is (= "node" (.get_supervisor_id summ)))
      (is (= 1234 (.get_port summ)))
      (is (= "foo" (.get_topology_id summ)))
      (is (= "testing" (.get_topology_name summ)))
      (is (= 2 (.get_num_executors summ)))
      (is (= 3.0 (.get_assigned_memonheap summ)))
      (is (= 4.0 (.get_assigned_memoffheap summ)))
      (is (= 5.0 (.get_assigned_cpu summ)))
      ;; agg-worker-stats groups the components together
      (is (= 2 (get comps "exclaim1")))
      (is (= 1 (get comps "__sys"))))))

(deftest agg-worker-skips-sys-if-not-enabled
  (let [storm-id "foo"
        topo-info (make-topo-info)
        worker->resources {(WorkerSlot. "node" 1234) (make-worker-resources)}
        include-sys? false
        user-authorized true 
        worker-summaries (stats/agg-worker-stats storm-id 
                                                 topo-info 
                                                 worker->resources 
                                                 include-sys? 
                                                 user-authorized)]
    (let [summ (first worker-summaries)
          comps (.get_component_to_num_tasks summ)]
      (is (= nil (get comps "__sys")))
      (is (= 2 (.get_num_executors summ)))
      (is (= 2 (get comps "exclaim1"))))))

(deftest agg-worker-gracefully-handles-missing-beats
  (let [storm-id "foo"
        topo-info (make-topo-info-no-beats)
        worker->resources {(WorkerSlot. "node" 1234) (make-worker-resources)}
        include-sys? false
        user-authorized true 
        worker-summaries (stats/agg-worker-stats storm-id 
                                                 topo-info 
                                                 worker->resources 
                                                 include-sys? 
                                                 user-authorized)]
    (let [summ (first worker-summaries)]
      (is (= 0 (.get_uptime_secs summ))))))

(deftest agg-worker-stats-exclude-components-if-not-authorized
  (let [storm-id "foo"
        topo-info (make-topo-info-no-beats)
        worker->resources {(WorkerSlot. "node" 1234) (make-worker-resources)}
        include-sys? false
        user-authorized  false
        worker-summaries (stats/agg-worker-stats storm-id 
                                                 topo-info 
                                                 worker->resources 
                                                 include-sys? 
                                                 user-authorized)]
    (let [summ (first worker-summaries)]
      (is (= 0 (.get_uptime_secs summ)))
      (is (= nil (.get_component_to_num_tasks summ))))))

(deftest agg-worker-stats-can-handle-nil-worker->resources
  (let [storm-id "foo"
        topo-info (make-topo-info-no-beats)
        worker->resources nil
        include-sys? false
        user-authorized  false
        worker-summaries (stats/agg-worker-stats storm-id 
                                                 topo-info 
                                                 worker->resources 
                                                 include-sys? 
                                                 user-authorized)]
    (let [summ (first worker-summaries)]
      (is (= 0 (.get_uptime_secs summ)))
      (is (= 0.0 (.get_assigned_memonheap summ)))
      (is (= 0.0 (.get_assigned_memoffheap summ)))
      (is (= 0.0 (.get_assigned_cpu summ)))
      (is (= nil (.get_component_to_num_tasks summ))))))

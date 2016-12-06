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
(ns org.apache.storm.metrics-test
  (:use [clojure test])
  (:import [org.apache.storm.topology TopologyBuilder])
  (:import [org.apache.storm.generated InvalidTopologyException SubmitOptions TopologyInitialStatus])
  (:import [org.apache.storm.testing TestWordCounter TestWordSpout TestGlobalCount FeederSpout
            TestAggregatesCounter TestConfBolt AckFailMapTracker PythonShellMetricsBolt PythonShellMetricsSpout])
  (:import [org.apache.storm.task ShellBolt])
  (:import [org.apache.storm.spout ShellSpout])
  (:import [org.apache.storm.metric.api CountMetric IMetricsConsumer$DataPoint IMetricsConsumer$TaskInfo])
  (:import [org.apache.storm.metric.api.rpc CountShellMetric])
  (:import [org.apache.storm.utils Utils])
  (:import [org.apache.storm Testing Testing$Condition LocalCluster$Builder])
  
  (:use [org.apache.storm config])
  (:use [org.apache.storm.internal clojure])
  (:use [org.apache.storm.util])
  (:import [org.apache.storm Thrift])
  (:import [org.apache.storm.utils Utils]
           (org.apache.storm.metric FakeMetricConsumer)))

(defbolt acking-bolt {} {:prepare true}
  [conf context collector]  
  (bolt
   (execute [tuple]
            (ack! collector tuple))))

(defbolt ack-every-other {} {:prepare true}
  [conf context collector]
  (let [state (atom -1)]
    (bolt
      (execute [tuple]
        (let [val (swap! state -)]
          (when (pos? val)
            (ack! collector tuple)
            ))))))

(defn assert-loop [afn ids]
  (Testing/whileTimeout Testing/TEST_TIMEOUT_MS
    (reify Testing$Condition (exec [this] (not (every? afn ids))))
    (fn [] (Thread/sleep 1))))

(defn assert-acked [tracker & ids]
  (assert-loop #(.isAcked tracker %) ids))

(defn assert-failed [tracker & ids]
  (assert-loop #(.isFailed tracker %) ids))

(defbolt count-acks {} {:prepare true}
  [conf context collector]

  (let [mycustommetric (CountMetric.)]   
    (.registerMetric context "my-custom-metric" mycustommetric 5)
    (bolt
     (execute [tuple]
              (.incr mycustommetric)
              (ack! collector tuple)))))

(defn wait-for-atleast-N-buckets! [N comp-id metric-name cluster]
  (Testing/whileTimeout Testing/TEST_TIMEOUT_MS
    (reify Testing$Condition (exec [this]
      (let [taskid->buckets (clojurify-structure (FakeMetricConsumer/getTaskIdToBuckets comp-id metric-name))]
        (or
         (and (not= N 0) (nil? taskid->buckets))
         (not-every? #(<= N %) (map (comp count second) taskid->buckets))))))
    (fn []
      (if cluster
        (.advanceClusterTime cluster 1)
        (Thread/sleep 10)))))

(defn lookup-bucket-by-comp-id-&-metric-name! [comp-id metric-name]
  (-> (FakeMetricConsumer/getTaskIdToBuckets comp-id metric-name)
      (clojurify-structure)
      (first) ;; pick first task in the list, ignore other tasks' metric data.
      (second)
      (or [])))

(defmacro assert-buckets! [comp-id metric-name expected cluster]
  `(do
     (let [N# (count ~expected)]
       (wait-for-atleast-N-buckets! N# ~comp-id ~metric-name ~cluster)
       (is (= ~expected (subvec (lookup-bucket-by-comp-id-&-metric-name! ~comp-id ~metric-name) 0 N#))))))

(defmacro assert-metric-data-exists! [comp-id metric-name]
  `(is (not-empty (lookup-bucket-by-comp-id-&-metric-name! ~comp-id ~metric-name))))

(deftest test-custom-metric
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "org.apache.storm.metric.FakeMetricConsumer"}]
                           "storm.zookeeper.connection.timeout" 30000
                           "storm.zookeeper.session.timeout" 60000
                           })))]
    (let [feeder (FeederSpout. ["field1"])
          topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails feeder)}
                    {"2" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "1" nil)
                            (Thrift/prepareGlobalGrouping)}
                           count-acks)})]
      (.submitTopology cluster "metrics-tester" {} topology)

      (.feed feeder ["a"] 1)
      (.advanceClusterTime cluster 6)
      (assert-buckets! "2" "my-custom-metric" [1] cluster)
            
      (.advanceClusterTime cluster 5)
      (assert-buckets! "2" "my-custom-metric" [1 0] cluster)

      (.advanceClusterTime cluster 20)
      (assert-buckets! "2" "my-custom-metric" [1 0 0 0 0 0] cluster)
      
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)               
      (.advanceClusterTime cluster 5)
      (assert-buckets! "2" "my-custom-metric" [1 0 0 0 0 0 2] cluster))))

(deftest test-custom-metric-with-multi-tasks
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "org.apache.storm.metric.FakeMetricConsumer"}]
                           "storm.zookeeper.connection.timeout" 30000
                           "storm.zookeeper.session.timeout" 60000
                           })))]
    (let [feeder (FeederSpout. ["field1"])
          topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails feeder)}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareAllGrouping)}
                            count-acks (Integer. 1) {TOPOLOGY-TASKS 2})})]
      (.submitTopology cluster "metrics-tester-with-multitasks" {} topology)

      (.feed feeder ["a"] 1)
      (.advanceClusterTime cluster 6)
      (assert-buckets! "2" "my-custom-metric" [1] cluster)

      (.advanceClusterTime cluster 5)
      (assert-buckets! "2" "my-custom-metric" [1 0] cluster)

      (.advanceClusterTime cluster 20)
      (assert-buckets! "2" "my-custom-metric" [1 0 0 0 0 0] cluster)

      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)
      (.advanceClusterTime cluster 5)
      (assert-buckets! "2" "my-custom-metric" [1 0 0 0 0 0 2] cluster))))

(defn mk-shell-bolt-with-metrics-spec
  [inputs command file]
    (Thrift/prepareBoltDetails inputs
         (PythonShellMetricsBolt. command file)))

(deftest test-custom-metric-with-multilang-py
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "org.apache.storm.metric.FakeMetricConsumer"}]
                           "storm.zookeeper.connection.timeout" 30000
                           "storm.zookeeper.session.timeout" 60000
                           })))]
    (let [feeder (FeederSpout. ["field1"])
          topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails feeder)}
                     {"2" (mk-shell-bolt-with-metrics-spec
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareGlobalGrouping)}
                            "python" "tester_bolt_metrics.py")})]
      (.submitTopology cluster "shell-metrics-tester" {} topology)

      (.feed feeder ["a"] 1)
      (.advanceClusterTime cluster 6)
      (assert-buckets! "2" "my-custom-shell-metric" [1] cluster)
            
      (.advanceClusterTime cluster 5)
      (assert-buckets! "2" "my-custom-shell-metric" [1 0] cluster)

      (.advanceClusterTime cluster 20)
      (assert-buckets! "2" "my-custom-shell-metric" [1 0 0 0 0 0] cluster)
      
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)               
      (.advanceClusterTime cluster 5)
      (assert-buckets! "2" "my-custom-shell-metric" [1 0 0 0 0 0 2] cluster)
      )))

(defn mk-shell-spout-with-metrics-spec
  [command file]
    (Thrift/prepareSpoutDetails (PythonShellMetricsSpout. command file)))

(deftest test-custom-metric-with-spout-multilang-py
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "org.apache.storm.metric.FakeMetricConsumer"}]
                           "storm.zookeeper.connection.timeout" 30000
                           "storm.zookeeper.session.timeout" 60000
                           })))]
    (let [topology (Thrift/buildTopology
                     {"1" (mk-shell-spout-with-metrics-spec "python" "tester_spout_metrics.py")}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareAllGrouping)}
                            count-acks)})]
      (.submitTopology cluster "shell-spout-metrics-tester" {} topology)

      (.advanceClusterTime cluster 7)
      (assert-buckets! "1" "my-custom-shellspout-metric" [2] cluster)
      )))


(deftest test-builtin-metrics-1
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "org.apache.storm.metric.FakeMetricConsumer"}]
                           TOPOLOGY-STATS-SAMPLE-RATE 1.0
                           TOPOLOGY-BUILTIN-METRICS-BUCKET-SIZE-SECS 60})))]
    (let [feeder (FeederSpout. ["field1"])
          topology (Thrift/buildTopology
                    {"myspout" (Thrift/prepareSpoutDetails feeder)}
                    {"mybolt" (Thrift/prepareBoltDetails
                                {(Utils/getGlobalStreamId "myspout" nil)
                                 (Thrift/prepareShuffleGrouping)}
                                acking-bolt)})]
      (.submitTopology cluster "metrics-tester" {} topology)
      
      (.feed feeder ["a"] 1)
      (.advanceClusterTime cluster 61)
      (assert-buckets! "myspout" "__ack-count/default" [1] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1] cluster)            
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1] cluster)

      (.advanceClusterTime cluster 120)
      (assert-buckets! "myspout" "__ack-count/default" [1 0 0] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1 0 0] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1 0 0] cluster)
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1 0 0] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1 0 0] cluster)

      (.feed feeder ["b"] 1)
      (.feed feeder ["c"] 1)
      (.advanceClusterTime cluster 60)
      (assert-buckets! "myspout" "__ack-count/default" [1 0 0 2] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1 0 0 2] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1 0 0 2] cluster)      
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1 0 0 2] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1 0 0 2] cluster))))


(deftest test-builtin-metrics-2
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "org.apache.storm.metric.FakeMetricConsumer"}]
                           TOPOLOGY-STATS-SAMPLE-RATE 1.0
                           TOPOLOGY-BUILTIN-METRICS-BUCKET-SIZE-SECS 5})))]
    (let [feeder (FeederSpout. ["field1"])
          tracker (AckFailMapTracker.)
          _ (.setAckFailDelegate feeder tracker)
          topology (Thrift/buildTopology
                    {"myspout" (Thrift/prepareSpoutDetails feeder)}
                    {"mybolt" (Thrift/prepareBoltDetails
                                {(Utils/getGlobalStreamId "myspout" nil)
                                 (Thrift/prepareShuffleGrouping)}
                                ack-every-other)})]
      (.submitTopology cluster
                             "metrics-tester"
                             {}
                             topology)
      
      (.feed feeder ["a"] 1)
      (.advanceClusterTime cluster 6)
      (assert-buckets! "myspout" "__fail-count/default" [] cluster)
      (assert-buckets! "myspout" "__ack-count/default" [1] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1] cluster)            
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1] cluster)     
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1] cluster)
      (assert-acked tracker 1)

      (.feed feeder ["b"] 2)      
      (.advanceClusterTime cluster 5)
      (assert-buckets! "myspout" "__fail-count/default" [] cluster)
      (assert-buckets! "myspout" "__ack-count/default" [1 0] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1 1] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1 1] cluster)                  
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1 0] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1 1] cluster)

      (.advanceClusterTime cluster 15)      
      (assert-buckets! "myspout" "__ack-count/default" [1 0 0 0 0] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1 1 0 0 0] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1 1 0 0 0] cluster)
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1 0 0 0 0] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1 1 0 0 0] cluster)
      
      (.feed feeder ["c"] 3)            
      (.advanceClusterTime cluster 15)      
      (assert-buckets! "myspout" "__ack-count/default" [1 0 0 0 0 1 0 0] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [1 1 0 0 0 1 0 0] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [1 1 0 0 0 1 0 0] cluster)
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [1 0 0 0 0 1 0 0] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [1 1 0 0 0 1 0 0] cluster))))

(deftest test-builtin-metrics-3
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "org.apache.storm.metric.FakeMetricConsumer"}]
                           TOPOLOGY-STATS-SAMPLE-RATE 1.0
                           TOPOLOGY-BUILTIN-METRICS-BUCKET-SIZE-SECS 5
                           TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true})))]
    (let [feeder (FeederSpout. ["field1"])
          tracker (AckFailMapTracker.)
          _ (.setAckFailDelegate feeder tracker)
          topology (Thrift/buildTopology
                    {"myspout" (Thrift/prepareSpoutDetails feeder)}
                    {"mybolt" (Thrift/prepareBoltDetails
                                {(Utils/getGlobalStreamId "myspout" nil)
                                 (Thrift/prepareGlobalGrouping)}
                                ack-every-other)})]
      (.submitTopology cluster
                             "timeout-tester"
                             {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10}
                             topology)
      (.feed feeder ["a"] 1)
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)
      (.advanceClusterTime cluster 9)
      (assert-buckets! "myspout" "__ack-count/default" [2] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [3] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [3] cluster)
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [2] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [3] cluster)
      (assert-acked tracker 1 3)
      
      (is (not (.isFailed tracker 2)))
      (.advanceClusterTime cluster 30)
      (assert-failed tracker 2)
      (assert-buckets! "myspout" "__fail-count/default" [1] cluster)
      (assert-buckets! "myspout" "__ack-count/default" [2 0 0 0] cluster)
      (assert-buckets! "myspout" "__emit-count/default" [3 0 0 0] cluster)
      (assert-buckets! "myspout" "__transfer-count/default" [3 0 0 0] cluster)
      (assert-buckets! "mybolt" "__ack-count/myspout:default" [2 0 0 0] cluster)
      (assert-buckets! "mybolt" "__execute-count/myspout:default" [3 0 0 0] cluster))))

(deftest test-system-bolt
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-METRICS-CONSUMER-REGISTER
                           [{"class" "org.apache.storm.metric.FakeMetricConsumer"}]
                           TOPOLOGY-BUILTIN-METRICS-BUCKET-SIZE-SECS 60})))]
    (let [feeder (FeederSpout. ["field1"])
          topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails feeder)}
                    {})]      
      (.submitTopology cluster "metrics-tester" {} topology)

      (.feed feeder ["a"] 1)
      (.advanceClusterTime cluster 70)
      (assert-buckets! "__system" "newWorkerEvent" [1] cluster)
      (assert-metric-data-exists! "__system" "uptimeSecs")
      (assert-metric-data-exists! "__system" "startTimeSecs")

      (.advanceClusterTime cluster 180)
      (assert-buckets! "__system" "newWorkerEvent" [1 0 0 0] cluster)
      )))



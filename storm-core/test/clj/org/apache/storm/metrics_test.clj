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
  (:import [org.apache.storm Testing Testing$Condition LocalCluster$Builder])
  (:import [org.awaitility Awaitility])
  (:import [org.awaitility.core ConditionEvaluationListener ConditionTimeoutException])
  (:import [java.util.concurrent TimeUnit Callable])
  (:import [org.hamcrest CoreMatchers])
  
  (:use [org.apache.storm config])
  (:use [org.apache.storm clojure])
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
       
(defn assert-metric-running-sum! [comp-id metric-name expected min-buckets cluster]
  (try
    (do
      (wait-for-atleast-N-buckets! min-buckets comp-id metric-name cluster)
      (.until 
        (.atMost 
          (.conditionEvaluationListener
            (.pollInterval (Awaitility/with) 10 TimeUnit/MILLISECONDS)
            (reify ConditionEvaluationListener (conditionEvaluated [this condition]
              (.advanceClusterTime cluster 1))))
          Testing/TEST_TIMEOUT_MS TimeUnit/MILLISECONDS)
        (reify Callable (call [this]
          (reduce + (lookup-bucket-by-comp-id-&-metric-name! comp-id metric-name))))
        (CoreMatchers/equalTo expected)))
    (catch ConditionTimeoutException e (throw (AssertionError. (.getMessage e))))))

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
      (assert-metric-running-sum! "2" "my-custom-metric" 1 1 cluster)
            
      (.advanceClusterTime cluster 5)
      (assert-metric-running-sum! "2" "my-custom-metric" 1 2 cluster)

      (.advanceClusterTime cluster 20)
      (assert-metric-running-sum! "2" "my-custom-metric" 1 6 cluster)
      
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)               
      (.advanceClusterTime cluster 5)
      (assert-metric-running-sum! "2" "my-custom-metric" 3 7 cluster))))

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
      (assert-metric-running-sum! "2" "my-custom-metric" 1 1 cluster)

      (.advanceClusterTime cluster 5)
      (assert-metric-running-sum! "2" "my-custom-metric" 1 2 cluster)

      (.advanceClusterTime cluster 20)
      (assert-metric-running-sum! "2" "my-custom-metric" 1 6 cluster)

      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)
      (.advanceClusterTime cluster 5)
      (assert-metric-running-sum! "2" "my-custom-metric" 3 7 cluster))))

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
      (assert-metric-running-sum! "2" "my-custom-shell-metric" 1 1 cluster)
            
      (.advanceClusterTime cluster 5)
      (assert-metric-running-sum! "2" "my-custom-shell-metric" 1 2 cluster)

      (.advanceClusterTime cluster 20)
      (assert-metric-running-sum! "2" "my-custom-shell-metric" 1 6 cluster)
      
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)               
      (.advanceClusterTime cluster 5)
      (assert-metric-running-sum! "2" "my-custom-shell-metric" 3 7 cluster)
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
      (assert-metric-running-sum! "1" "my-custom-shellspout-metric" 2 1 cluster)
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
      (assert-metric-running-sum! "myspout" "__ack-count/default" 1 1 cluster)
      (assert-metric-running-sum! "myspout" "__emit-count/default" 1 1 cluster)
      (assert-metric-running-sum! "myspout" "__transfer-count/default" 1 1 cluster)            
      (assert-metric-running-sum! "mybolt" "__ack-count/myspout:default" 1 1 cluster)
      (assert-metric-running-sum! "mybolt" "__execute-count/myspout:default" 1 1 cluster)

      (.advanceClusterTime cluster 120)
      (assert-metric-running-sum! "myspout" "__ack-count/default" 1 3 cluster)
      (assert-metric-running-sum! "myspout" "__emit-count/default" 1 3 cluster)
      (assert-metric-running-sum! "myspout" "__transfer-count/default" 1 3 cluster)
      (assert-metric-running-sum! "mybolt" "__ack-count/myspout:default" 1 3 cluster)
      (assert-metric-running-sum! "mybolt" "__execute-count/myspout:default" 1 3 cluster)

      (.feed feeder ["b"] 1)
      (.feed feeder ["c"] 1)
      (.advanceClusterTime cluster 60)
      (assert-metric-running-sum! "myspout" "__ack-count/default" 3 4 cluster)
      (assert-metric-running-sum! "myspout" "__emit-count/default" 3 4 cluster)
      (assert-metric-running-sum! "myspout" "__transfer-count/default" 3 4 cluster)      
      (assert-metric-running-sum! "mybolt" "__ack-count/myspout:default" 3 4 cluster)
      (assert-metric-running-sum! "mybolt" "__execute-count/myspout:default" 3 4 cluster))))


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
      (assert-metric-running-sum! "myspout" "__fail-count/default" 0 0 cluster)
      (assert-metric-running-sum! "myspout" "__ack-count/default" 1 1 cluster)
      (assert-metric-running-sum! "myspout" "__emit-count/default" 1 1 cluster)
      (assert-metric-running-sum! "myspout" "__transfer-count/default" 1 1 cluster)            
      (assert-metric-running-sum! "mybolt" "__ack-count/myspout:default" 1 1 cluster)     
      (assert-metric-running-sum! "mybolt" "__execute-count/myspout:default" 1 1 cluster)
      (assert-acked tracker 1)

      (.feed feeder ["b"] 2)      
      (.advanceClusterTime cluster 5)
      (assert-metric-running-sum! "myspout" "__fail-count/default" 0 0 cluster)
      (assert-metric-running-sum! "myspout" "__ack-count/default" 1 2 cluster)
      (assert-metric-running-sum! "myspout" "__emit-count/default" 2 2 cluster)
      (assert-metric-running-sum! "myspout" "__transfer-count/default" 2 2 cluster)                  
      (assert-metric-running-sum! "mybolt" "__ack-count/myspout:default" 1 2 cluster)
      (assert-metric-running-sum! "mybolt" "__execute-count/myspout:default" 2 2 cluster)

      (.advanceClusterTime cluster 15)      
      (assert-metric-running-sum! "myspout" "__ack-count/default" 1 5 cluster)
      (assert-metric-running-sum! "myspout" "__emit-count/default" 2 5 cluster)
      (assert-metric-running-sum! "myspout" "__transfer-count/default" 2 5 cluster)
      (assert-metric-running-sum! "mybolt" "__ack-count/myspout:default" 1 5 cluster)
      (assert-metric-running-sum! "mybolt" "__execute-count/myspout:default" 2 5 cluster)
      
      (.feed feeder ["c"] 3)            
      (.advanceClusterTime cluster 15)      
      (assert-metric-running-sum! "myspout" "__ack-count/default" 2 8 cluster)
      (assert-metric-running-sum! "myspout" "__emit-count/default" 3 8 cluster)
      (assert-metric-running-sum! "myspout" "__transfer-count/default" 3 8 cluster)
      (assert-metric-running-sum! "mybolt" "__ack-count/myspout:default" 2 8 cluster)
      (assert-metric-running-sum! "mybolt" "__execute-count/myspout:default" 3 8 cluster))))

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
      (assert-metric-running-sum! "myspout" "__ack-count/default" 2 1 cluster)
      (assert-metric-running-sum! "myspout" "__emit-count/default" 3 1 cluster)
      (assert-metric-running-sum! "myspout" "__transfer-count/default" 3 1 cluster)
      (assert-metric-running-sum! "mybolt" "__ack-count/myspout:default" 2 1 cluster)
      (assert-metric-running-sum! "mybolt" "__execute-count/myspout:default" 3 1 cluster)
      (assert-acked tracker 1 3)
      
      (is (not (.isFailed tracker 2)))
      (.advanceClusterTime cluster 30)
      (assert-failed tracker 2)
      (assert-metric-running-sum! "myspout" "__fail-count/default" 1 1 cluster)
      (assert-metric-running-sum! "myspout" "__ack-count/default" 2 4 cluster)
      (assert-metric-running-sum! "myspout" "__emit-count/default" 3 4 cluster)
      (assert-metric-running-sum! "myspout" "__transfer-count/default" 3 4 cluster)
      (assert-metric-running-sum! "mybolt" "__ack-count/myspout:default" 2 4 cluster)
      (assert-metric-running-sum! "mybolt" "__execute-count/myspout:default" 3 4 cluster))))




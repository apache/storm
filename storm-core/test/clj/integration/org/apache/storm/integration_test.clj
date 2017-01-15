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
(ns integration.org.apache.storm.integration-test
  (:use [clojure test])
  (:import [org.apache.storm Testing LocalCluster$Builder Config Thrift])
  (:import [org.apache.storm.topology TopologyBuilder])
  (:import [org.apache.storm.generated InvalidTopologyException SubmitOptions TopologyInitialStatus RebalanceOptions])
  (:import [org.apache.storm.testing TrackedTopology MockedSources TestWordCounter TestWordSpout TestGlobalCount FeederSpout CompleteTopologyParam
            TestAggregatesCounter TestConfBolt AckFailMapTracker AckTracker TestPlannerSpout])
  (:import [org.apache.storm.utils Time])
  (:import [org.apache.storm.tuple Fields])
  (:import [org.apache.storm.cluster StormClusterStateImpl])
  (:use [org.apache.storm.internal clojure])
  (:use [org.apache.storm config util])
  (:import [org.apache.storm Thrift])
  (:import [org.apache.storm.utils Utils]) 
  (:import [org.apache.storm.daemon StormCommon]))

(deftest test-basic-topology
  (doseq [zmq-on? [true false]]
    (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                  (.withSimulatedTime)
                                  (.withSupervisors 4)
                                  (.withDaemonConf {STORM-LOCAL-MODE-ZMQ zmq-on?})))]
      (let [topology (Thrift/buildTopology
                      {"1" (Thrift/prepareSpoutDetails
                             (TestWordSpout. true) (Integer. 3))}
                      {"2" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "1" nil)
                              (Thrift/prepareFieldsGrouping ["word"])}
                             (TestWordCounter.) (Integer. 4))
                       "3" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "1" nil)
                              (Thrift/prepareGlobalGrouping)}
                             (TestGlobalCount.))
                       "4" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "2" nil)
                              (Thrift/prepareGlobalGrouping)}
                             (TestAggregatesCounter.))})
            results (Testing/completeTopology cluster
                                       topology
                                       (doto (CompleteTopologyParam.)
                                         (.setMockedSources (MockedSources. {"1" [["nathan"] ["bob"] ["joey"] ["nathan"]]}))
                                         (.setStormConf {TOPOLOGY-WORKERS 2
                                                    TOPOLOGY-TESTING-ALWAYS-TRY-SERIALIZE true})))]
        (is (Testing/multiseteq [["nathan"] ["bob"] ["joey"] ["nathan"]]
                 (Testing/readTuples results "1")))
        (is (Testing/multiseteq [["nathan" (int 1)] ["nathan" (int 2)] ["bob" (int 1)] ["joey" (int 1)]]
                 (Testing/readTuples results "2")))
        (is (= [[1] [2] [3] [4]]
               (Testing/readTuples results "3")))
        (is (= [[1] [2] [3] [4]]
               (Testing/readTuples results "4")))
        ))))

(defbolt emit-task-id ["tid"] {:prepare true}
  [conf context collector]
  (let [tid (.getThisTaskIndex context)]
    (bolt
      (execute [tuple]
        (emit-bolt! collector [tid] :anchor tuple)
        (ack! collector tuple)
        ))))

(deftest test-multi-tasks-per-executor
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                  (.withSimulatedTime)
                                (.withSupervisors 4)))]
    (let [topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails (TestWordSpout. true))}
                    {"2" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "1" nil)
                            (Thrift/prepareAllGrouping)}
                           emit-task-id
                           (Integer. 3)
                           {TOPOLOGY-TASKS 6})})
          results (Testing/completeTopology cluster
                                     topology
                                     (doto (CompleteTopologyParam.)
                                       (.setMockedSources (MockedSources. {"1" [["a"]]}))))]
      (is (Testing/multiseteq [[(int 0)] [(int 1)] [(int 2)] [(int 3)] [(int 4)] [(int 5)]]
               (Testing/readTuples results "2")))
      )))

(defbolt ack-every-other {} {:prepare true}
  [conf context collector]
  (let [state (atom -1)]
    (bolt
      (execute [tuple]
        (let [val (swap! state -)]
          (when (pos? val)
            (ack! collector tuple)
            ))))))

(defn assert-loop 
([afn ids] (assert-loop afn ids 10))
([afn ids timeout-secs]
  (loop [remaining-time (* timeout-secs 1000)]
    (let [start-time (System/currentTimeMillis)
          assertion-is-true (every? afn ids)]
      (if (or assertion-is-true (neg? remaining-time))
        (is assertion-is-true)
        (do
          (Thread/sleep 1)
          (recur (- remaining-time (- (System/currentTimeMillis) start-time)))
        ))))))

(defn assert-acked [tracker & ids]
  (assert-loop #(.isAcked tracker %) ids))

(defn assert-failed [tracker & ids]
  (assert-loop #(.isFailed tracker %) ids))

(deftest test-timeout
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withSupervisors 4)
                                (.withDaemonConf {TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true})))]
    (let [feeder (FeederSpout. ["field1"])
          tracker (AckFailMapTracker.)
          _ (.setAckFailDelegate feeder tracker)
          topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails feeder)}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareGlobalGrouping)} ack-every-other)})]
      (.submitTopology cluster
                             "timeout-tester"
                             {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10}
                             topology)
      (.advanceClusterTime cluster 11)
      (.feed feeder ["a"] 1)
      (.feed feeder ["b"] 2)
      (.feed feeder ["c"] 3)
      (.advanceClusterTime cluster 9)
      (assert-acked tracker 1 3)
      (is (not (.isFailed tracker 2)))
      (.advanceClusterTime cluster 12)
      (assert-failed tracker 2)
      )))

(defbolt extend-timeout-twice {} {:prepare true}
  [conf context collector]
  (let [state (atom -1)]
    (bolt
      (execute [tuple]
        (do
          (Time/sleep (* 8 1000))
          (reset-timeout! collector tuple)
          (Time/sleep (* 8 1000))
          (reset-timeout! collector tuple)
          (Time/sleep (* 8 1000))
          (ack! collector tuple)
        )))))

(deftest test-reset-timeout
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true})))]
    (let [feeder (FeederSpout. ["field1"])
          tracker (AckFailMapTracker.)
          _ (.setAckFailDelegate feeder tracker)
          topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails feeder)}
                     {"2" (Thrift/prepareBoltDetails 
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareGlobalGrouping)} extend-timeout-twice)})]
    (.submitTopology cluster
                           "timeout-tester"
                           {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10}
                           topology)
    (.advanceClusterTime cluster 11)
    (.feed feeder ["a"] 1)
    (.advanceClusterTime cluster 21)
    (is (not (.isFailed tracker 1)))
    (is (not (.isAcked tracker 1)))
    (.advanceClusterTime cluster 5)
    (assert-acked tracker 1)
    )))

(defn mk-validate-topology-1 []
  (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails (TestWordSpout. true) (Integer. 3))}
                    {"2" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "1" nil)
                            (Thrift/prepareFieldsGrouping ["word"])}
                           (TestWordCounter.) (Integer. 4))}))

(defn mk-invalidate-topology-1 []
  (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails (TestWordSpout. true) (Integer. 3))}
                    {"2" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "3" nil)
                            (Thrift/prepareFieldsGrouping ["word"])}
                           (TestWordCounter.) (Integer. 4))}))

(defn mk-invalidate-topology-2 []
  (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails (TestWordSpout. true) (Integer. 3))}
                    {"2" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "1" nil)
                            (Thrift/prepareFieldsGrouping ["non-exists-field"])}
                           (TestWordCounter.) (Integer. 4))}))

(defn mk-invalidate-topology-3 []
  (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails (TestWordSpout. true) (Integer. 3))}
                    {"2" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "1" "non-exists-stream")
                            (Thrift/prepareFieldsGrouping ["word"])}
                           (TestWordCounter.) (Integer. 4))}))

(defn try-complete-wc-topology [cluster topology]
  (try (do
         (Testing/completeTopology cluster
                            topology
                            (doto (CompleteTopologyParam.)
                              (.setMockedSources (MockedSources. {"1" [["nathan"] ["bob"] ["joey"] ["nathan"]]}))
                              (.setStormConf {TOPOLOGY-WORKERS 2})))
         false)
       (catch InvalidTopologyException e true)))

(deftest test-validate-topology-structure
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSupervisors 4)
                                (.withSimulatedTime)))]
    (let [any-error1? (try-complete-wc-topology cluster (mk-validate-topology-1))
          any-error2? (try-complete-wc-topology cluster (mk-invalidate-topology-1))
          any-error3? (try-complete-wc-topology cluster (mk-invalidate-topology-2))
          any-error4? (try-complete-wc-topology cluster (mk-invalidate-topology-3))]
      (is (= any-error1? false))
      (is (= any-error2? true))
      (is (= any-error3? true))
      (is (= any-error4? true)))))

(defbolt identity-bolt ["num"]
  [tuple collector]
  (emit-bolt! collector (.getValues tuple) :anchor tuple)
  (ack! collector tuple))

(deftest test-system-stream
  ;; this test works because mocking a spout splits up the tuples evenly among the tasks
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)))]
      (let [topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestWordSpout. true) (Integer. 3))}
                       {"2" (Thrift/prepareBoltDetails
                              {(Utils/getGlobalStreamId "1" nil)
                               (Thrift/prepareFieldsGrouping ["word"])
                               (Utils/getGlobalStreamId "1" "__system")
                               (Thrift/prepareGlobalGrouping)}
                               identity-bolt (Integer. 1))})
            results (Testing/completeTopology cluster
                                       topology
                                       (doto (CompleteTopologyParam.)
                                         (.setMockedSources (MockedSources. {"1" [["a"] ["b"] ["c"]]}))
                                         (.setStormConf {TOPOLOGY-WORKERS 2})))]
        (is (Testing/multiseteq [["a"] ["b"] ["c"] ["startup"] ["startup"] ["startup"]]
                 (Testing/readTuples results "2")))
        )))

(defn ack-tracking-feeder [fields]
  (let [tracker (AckTracker.)]
    [(doto (FeederSpout. fields)
       (.setAckFailDelegate tracker))
     (fn [val]
       (is (= (.getNumAcks tracker) val))
       (.resetNumAcks tracker)
       )]
    ))

(defbolt branching-bolt ["num"]
  {:params [amt]}
  [tuple collector]
  (doseq [i (range amt)]
    (emit-bolt! collector [i] :anchor tuple))
  (ack! collector tuple))

(defbolt agg-bolt ["num"] {:prepare true :params [amt]}
  [conf context collector]
  (let [seen (atom [])]
    (bolt
      (execute [tuple]
        (swap! seen conj tuple)
        (when (= (count @seen) amt)
          (emit-bolt! collector [1] :anchor @seen)
          (doseq [s @seen]
            (ack! collector s))
          (reset! seen [])
          )))
      ))

(defbolt ack-bolt {}
  [tuple collector]
  (ack! collector tuple))

(deftest test-acking
  (with-open [cluster (.build (.withSimulatedTime (.withTracked (LocalCluster$Builder. ))))]
    (let [[feeder1 checker1] (ack-tracking-feeder ["num"])
          [feeder2 checker2] (ack-tracking-feeder ["num"])
          [feeder3 checker3] (ack-tracking-feeder ["num"])
          tracked (TrackedTopology.
                   (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails feeder1)
                      "2" (Thrift/prepareSpoutDetails feeder2)
                      "3" (Thrift/prepareSpoutDetails feeder3)}
                     {"4" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareShuffleGrouping)}
                            (branching-bolt 2))
                      "5" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "2" nil)
                             (Thrift/prepareShuffleGrouping)}
                            (branching-bolt 4))
                      "6" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "3" nil)
                             (Thrift/prepareShuffleGrouping)}
                            (branching-bolt 1))
                      "7" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "4" nil)
                             (Thrift/prepareShuffleGrouping)
                             (Utils/getGlobalStreamId "5" nil)
                             (Thrift/prepareShuffleGrouping)
                             (Utils/getGlobalStreamId "6" nil)
                             (Thrift/prepareShuffleGrouping)}
                            (agg-bolt 3))
                      "8" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "7" nil)
                             (Thrift/prepareShuffleGrouping)}
                            (branching-bolt 2))
                      "9" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "8" nil)
                             (Thrift/prepareShuffleGrouping)}
                            ack-bolt)}
                     )
                     cluster)]
      (.submitTopology cluster
                       "acking-test1"
                       {}
                       tracked)
      (.advanceClusterTime cluster 11)
      (.feed feeder1 [1])
      (Testing/trackedWait tracked (int 1))
      (checker1 0)
      (.feed feeder2 [1])
      (Testing/trackedWait tracked (int 1))
      (checker1 1)
      (checker2 1)
      (.feed feeder1 [1])
      (Testing/trackedWait tracked (int 1))
      (checker1 0)
      (.feed feeder1 [1])
      (Testing/trackedWait tracked (int 1))
      (checker1 1)
      (.feed feeder3 [1])
      (Testing/trackedWait tracked (int 1))
      (checker1 0)
      (checker3 0)
      (.feed feeder2 [1])
      (Testing/trackedWait tracked (int 1))
      (checker1 1)
      (checker2 1)
      (checker3 1))))

(deftest test-ack-branching
  (with-open [cluster (.build (.withSimulatedTime (.withTracked (LocalCluster$Builder. ))))]
    (let [[feeder checker] (ack-tracking-feeder ["num"])
          tracked (TrackedTopology.
                   (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails feeder)}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareShuffleGrouping)}
                            identity-bolt)
                      "3" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareShuffleGrouping)}
                            identity-bolt)
                      "4" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "2" nil)
                             (Thrift/prepareShuffleGrouping)
                             (Utils/getGlobalStreamId "3" nil)
                             (Thrift/prepareShuffleGrouping)}
                             (agg-bolt 4))})
                    cluster)]
      (.submitTopology cluster
                       "test-acking2"
                       {}
                       tracked)
      (.advanceClusterTime cluster 11)
      (.feed feeder [1])
      (Testing/trackedWait tracked (int 1))
      (checker 0)
      (.feed feeder [1])
      (Testing/trackedWait tracked (int 1))
      (checker 2))))

(defbolt dup-anchor ["num"]
  [tuple collector]
  (emit-bolt! collector [1] :anchor [tuple tuple])
  (ack! collector tuple))

(def bolt-prepared? (atom false))
(defbolt prepare-tracked-bolt [] {:prepare true}
  [conf context collector]  
  (bolt
   (execute [tuple]
            (reset! bolt-prepared? true)
            (ack! collector tuple))))

(def spout-opened? (atom false))
(defspout open-tracked-spout ["val"]
  [conf context collector]
  (spout
    (nextTuple []
      (reset! spout-opened? true)
      )))

(deftest test-submit-inactive-topology
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true})))]
    (let [feeder (FeederSpout. ["field1"])
          tracker (AckFailMapTracker.)
          _ (.setAckFailDelegate feeder tracker)
          topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails feeder)
                     "2" (Thrift/prepareSpoutDetails open-tracked-spout)}
                    {"3" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "1" nil)
                            (Thrift/prepareGlobalGrouping)}
                           prepare-tracked-bolt)})]
      (reset! bolt-prepared? false)
      (reset! spout-opened? false)      
      
      (.submitTopologyWithOpts cluster
        "test"
        {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10}
        topology
        (SubmitOptions. TopologyInitialStatus/INACTIVE))
      (.advanceClusterTime cluster 11)
      (.feed feeder ["a"] 1)
      (.advanceClusterTime cluster 9)
      (is (not @bolt-prepared?))
      (is (not @spout-opened?))        
      (.activate (.getNimbus cluster) "test")

      (.advanceClusterTime cluster 12)
      (assert-acked tracker 1)
      (is @bolt-prepared?)
      (is @spout-opened?))))

(deftest test-acking-self-anchor
  (with-open [cluster (.build (.withSimulatedTime (.withTracked (LocalCluster$Builder. ))))]
    (let [[feeder checker] (ack-tracking-feeder ["num"])
          tracked (TrackedTopology.
                   (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails feeder)}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareShuffleGrouping)}
                            dup-anchor)
                      "3" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "2" nil)
                             (Thrift/prepareShuffleGrouping)}
                            ack-bolt)})
                   cluster)]
      (.submitTopology cluster
                       "test"
                       {}
                       tracked)
      (.advanceClusterTime cluster 11)
      (.feed feeder [1])
      (Testing/trackedWait tracked (int 1))
      (checker 1)
      (.feed feeder [1])
      (.feed feeder [1])
      (.feed feeder [1])
      (Testing/trackedWait tracked (int 3))
      (checker 3))))

(defspout IncSpout ["word"]
  [conf context collector]
  (let [state (atom 0)]
    (spout
     (nextTuple []
       (Thread/sleep 100)
       (emit-spout! collector [@state] :id 1)         
       )
     (ack [id]
       (swap! state inc))
     )))


(defspout IncSpout2 ["word"] {:params [prefix]}
  [conf context collector]
  (let [state (atom 0)]
    (spout
     (nextTuple []
       (Thread/sleep 100)
       (swap! state inc)
       (emit-spout! collector [(str prefix "-" @state)])         
       )
     )))

(deftest test-kryo-decorators-config
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS true
                                                  TOPOLOGY-KRYO-DECORATORS ["this-is-overriden"]})))]
    (let [builder (TopologyBuilder.)
          _ (.setSpout builder "1" (TestPlannerSpout. (Fields. ["conf"])))
          _ (-> builder (.setBolt "2" (TestConfBolt. {TOPOLOGY-KRYO-DECORATORS ["one" "two"]})) (.shuffleGrouping "1"))
          results (Testing/completeTopology cluster
                                     (.createTopology builder)
                                     (doto (CompleteTopologyParam.)
                                       (.setStormConf {TOPOLOGY-KRYO-DECORATORS ["one" "three"]})
                                       (.setMockedSources (MockedSources. {"1" [[TOPOLOGY-KRYO-DECORATORS]]}))))]
      (is (= {"topology.kryo.decorators" (list "one" "two" "three")}
             (->> (Testing/readTuples results "2") (apply concat) (apply hash-map)))))))

(deftest test-component-specific-config
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)
                                (.withDaemonConf {TOPOLOGY-SKIP-MISSING-KRYO-REGISTRATIONS true})))]
    (let [builder (TopologyBuilder.)
          _ (.setSpout builder "1" (TestPlannerSpout. (Fields. ["conf"])))
          _ (-> builder
                (.setBolt "2"
                          (TestConfBolt.
                            {"fake.config" 123
                             TOPOLOGY-MAX-TASK-PARALLELISM 20
                             TOPOLOGY-MAX-SPOUT-PENDING 30
                             TOPOLOGY-KRYO-REGISTER [{"fake.type" "bad.serializer"}
                                                     {"fake.type2" "a.serializer"}]}))
                (.shuffleGrouping "1")
                (.setMaxTaskParallelism (int 2))
                (.addConfiguration "fake.config2" 987))
          results (Testing/completeTopology cluster
                                     (.createTopology builder)
                                     (doto (CompleteTopologyParam.)
                                       (.setStormConf {TOPOLOGY-KRYO-REGISTER [{"fake.type" "good.serializer", "fake.type3" "a.serializer3"}]})
                                        (.setMockedSources (MockedSources. {"1" [["fake.config"]
                                                         [TOPOLOGY-MAX-TASK-PARALLELISM]
                                                         [TOPOLOGY-MAX-SPOUT-PENDING]
                                                         ["fake.config2"]
                                                         [TOPOLOGY-KRYO-REGISTER]]}))))]
      (is (= {"fake.config" 123
              "fake.config2" 987
              TOPOLOGY-MAX-TASK-PARALLELISM 2
              TOPOLOGY-MAX-SPOUT-PENDING 30
              TOPOLOGY-KRYO-REGISTER {"fake.type" "good.serializer"
                                      "fake.type2" "a.serializer"
                                      "fake.type3" "a.serializer3"}}
             (->> (Testing/readTuples results "2")
                  (apply concat)
                  (apply hash-map)))))))

(defbolt hooks-bolt ["emit" "ack" "fail" "executed"] {:prepare true}
  [conf context collector]
  (let [acked (atom 0)
        failed (atom 0)
        executed (atom 0)
        emitted (atom 0)]
    (.addTaskHook context
                  (reify org.apache.storm.hooks.ITaskHook
                    (prepare [this conf context]
                      )
                    (cleanup [this]
                      )
                    (emit [this info]
                      (swap! emitted inc))
                    (boltAck [this info]
                      (swap! acked inc))
                    (boltFail [this info]
                      (swap! failed inc))
                    (boltExecute [this info]
                      (swap! executed inc))
                      ))
    (bolt
     (execute [tuple]
        (emit-bolt! collector [@emitted @acked @failed @executed])
        (if (= 0 (- @acked @failed))
          (ack! collector tuple)
          (fail! collector tuple))
        ))))

(deftest test-hooks
  (with-open [cluster (.build (doto (LocalCluster$Builder.)
                                (.withSimulatedTime)))]
    (let [topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails
                            (TestPlannerSpout. (Fields. ["conf"])))}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareShuffleGrouping)}
                            hooks-bolt)})
          results (Testing/completeTopology cluster
                                     topology
                                     (doto (CompleteTopologyParam.)
                                       (.setMockedSources (MockedSources. {"1" [[1]
                                                         [1]
                                                         [1]
                                                         [1]
                                                         ]}))))]
      (is (= [[0 0 0 0]
              [2 1 0 1]
              [4 1 1 2]
              [6 2 1 3]]
             (Testing/readTuples results "2")
             )))))

(defbolt report-errors-bolt {}
  [tuple collector]
  (doseq [i (range (.getValue tuple 0))]
    (report-error! collector (RuntimeException.)))
  (ack! collector tuple))


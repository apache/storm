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
(ns org.apache.storm.nimbus-test
  (:use [clojure test])
  (:require [org.apache.storm [util :as util]])
  (:import [java.util.function UnaryOperator])
  (:import [org.apache.storm.testing InProcessZookeeper MockLeaderElector TestWordCounter TestWordSpout TestGlobalCount
            TestAggregatesCounter TestPlannerSpout TestPlannerBolt]
           [org.apache.storm.blobstore BlobStore]
           [org.apache.storm.nimbus InMemoryTopologyActionNotifier]
           [org.apache.storm.daemon.nimbus Nimbus Nimbus$StandaloneINimbus]
           [org.apache.storm.generated GlobalStreamId TopologyStatus SupervisorInfo StormTopology StormBase]
           [org.apache.storm LocalCluster LocalCluster$Builder Thrift MockAutoCred Testing Testing$Condition]
           [org.apache.storm.stats BoltExecutorStats StatsUtil]
           [org.apache.storm.security.auth IGroupMappingServiceProvider IAuthorizer])
  (:import [org.apache.storm.testing.staticmocking MockedZookeeper])
  (:import [org.apache.storm.testing TmpPath])
  (:import [org.apache.storm.scheduler INimbus])
  (:import [org.mockito Mockito Matchers])
  (:import [org.mockito.exceptions.base MockitoAssertionError])
  (:import [org.apache.storm.nimbus ILeaderElector NimbusInfo])
  (:import [org.apache.storm.testing.staticmocking MockedCluster])
  (:import [org.apache.storm.generated Credentials NotAliveException SubmitOptions
            TopologyInitialStatus TopologyStatus AlreadyAliveException KillOptions RebalanceOptions
            InvalidTopologyException AuthorizationException
            LogConfig LogLevel LogLevelAction Assignment NodeInfo])
  (:import [java.util Map HashMap HashSet Optional])
  (:import [java.io File])
  (:import [javax.security.auth Subject])
  (:import [org.apache.storm.utils Time Time$SimulatedTime IPredicate StormCommonInstaller Utils$UptimeComputer ReflectionUtils Utils ConfigUtils ServerConfigUtils]
           [org.apache.storm.utils.staticmocking ServerConfigUtilsInstaller ReflectionUtilsInstaller UtilsInstaller])
  (:import [org.apache.storm.zookeeper Zookeeper])
  (:import [org.apache.commons.io FileUtils])
  (:import [org.json.simple JSONValue])
  (:import [org.apache.storm.daemon StormCommon])
  (:import [org.apache.storm.cluster IStormClusterState StormClusterStateImpl ClusterStateContext ClusterUtils])
  (:use [org.apache.storm util daemon-config config log])
  (:require [conjure.core])

  (:use [conjure core]))

(def ^:dynamic *STORM-CONF* (clojurify-structure (ConfigUtils/readStormConfig)))

(defn- mk-nimbus
  [conf inimbus blob-store leader-elector group-mapper cluster-state]
  (Nimbus. conf inimbus cluster-state nil blob-store leader-elector group-mapper))

(defn- from-json
       [^String str]
       (if str
         (clojurify-structure
           (JSONValue/parse str))
         nil))

(defn storm-component->task-info [cluster storm-name]
  (let [storm-id (StormCommon/getStormId (.getClusterState cluster) storm-name)
        nimbus (.getNimbus cluster)]
    (-> (.getUserTopology nimbus storm-id)
        (#(StormCommon/stormTaskInfo % (from-json (.getTopologyConf nimbus storm-id))))
        (Utils/reverseMap)
        clojurify-structure)))

(defn getCredentials [cluster storm-name]
  (let [storm-id (StormCommon/getStormId (.getClusterState cluster) storm-name)
        creds (.credentials (.getClusterState cluster) storm-id nil)]
    (if creds (into {} (.get_creds creds)))))

(defn storm-component->executor-info [cluster storm-name]
  (let [storm-id (StormCommon/getStormId (.getClusterState cluster) storm-name)
        nimbus (.getNimbus cluster)
        storm-conf (from-json (.getTopologyConf nimbus storm-id))
        topology (.getUserTopology nimbus storm-id)
        task->component (clojurify-structure (StormCommon/stormTaskInfo topology storm-conf))
        state (.getClusterState cluster)
        get-component (comp task->component first)]
    (->> (.assignmentInfo state storm-id nil)
         .get_executor_node_port
         .keySet
         clojurify-structure
         (map (fn [e] {e (get-component e)}))
         (apply merge)
         (Utils/reverseMap)
         clojurify-structure)))

(defn storm-num-workers [state storm-name]
  (let [storm-id (StormCommon/getStormId state storm-name)
        assignment (.assignmentInfo state storm-id nil)]
    (.size (Utils/reverseMap (.get_executor_node_port assignment)))))

(defn topology-nodes [state storm-name]
  (let [storm-id (StormCommon/getStormId state storm-name)
        assignment (.assignmentInfo state storm-id nil)]
    (->> assignment
         .get_executor_node_port
         .values
         (map (fn [np] (.get_node np)))
         set
         )))

(defn topology-slots [state storm-name]
  (let [storm-id (StormCommon/getStormId state storm-name)
        assignment (.assignmentInfo state storm-id nil)]
    (->> assignment
         .get_executor_node_port
         .values
         set
         )))

;TODO: when translating this function, don't call map-val, but instead use an inline for loop.
; map-val is a temporary kluge for clojure.
(defn topology-node-distribution [state storm-name]
  (let [storm-id (StormCommon/getStormId state storm-name)
        assignment (.assignmentInfo state storm-id nil)]
    (->> assignment
         .get_executor_node_port
         .values
         set
         (group-by (fn [np] (.get_node np)))
         (map-val count)
         (map (fn [[_ amt]] {amt 1}))
         (apply merge-with +)
         )))

(defn topology-num-nodes [state storm-name]
  (count (topology-nodes state storm-name)))

(defn executor-assignment [cluster storm-id executor-id]
  (let [state (.getClusterState cluster)
        assignment (.assignmentInfo state storm-id nil)]
    (.get (.get_executor_node_port assignment) executor-id)
    ))

(defn executor-start-times [cluster storm-id]
  (let [state (.getClusterState cluster)
        assignment (.assignmentInfo state storm-id nil)]
    (clojurify-structure (.get_executor_start_time_secs assignment))))

(defn do-executor-heartbeat [cluster storm-id executor]
  (let [state (.getClusterState cluster)
        executor->node+port (.get_executor_node_port (.assignmentInfo state storm-id nil))
        np (.get executor->node+port executor)
        node (.get_node np)
        port (first (.get_port np))
        curr-beat (StatsUtil/convertZkWorkerHb (.getWorkerHeartbeat state storm-id node port))
        stats (if (get curr-beat "executor-stats")
                (get curr-beat "executor-stats")
                (HashMap.))]
    (log-warn "curr-beat:" (prn-str curr-beat) ",stats:" (prn-str stats))
    (log-warn "stats type:" (type stats))
    (.put stats (StatsUtil/convertExecutor executor) (.renderStats (BoltExecutorStats. 20 (*STORM-CONF* NUM-STAT-BUCKETS))))
    (log-warn "merged:" stats)

    (.workerHeartbeat state storm-id node port
      (StatsUtil/thriftifyZkWorkerHb (StatsUtil/mkZkWorkerHb storm-id stats (int 10))))))

(defn slot-assignments [cluster storm-id]
  (let [state (.getClusterState cluster)
        assignment (.assignmentInfo state storm-id nil)]
    (clojurify-structure (Utils/reverseMap (.get_executor_node_port assignment)))))

(defn task-ids [cluster storm-id]
  (let [nimbus (.getNimbus cluster)]
    (-> (.getUserTopology nimbus storm-id)
        (#(StormCommon/stormTaskInfo % (from-json (.getTopologyConf nimbus storm-id))))
        clojurify-structure
        keys)))

(defn topology-executors [cluster storm-id]
  (let [state (.getClusterState cluster)
        assignment (.assignmentInfo state storm-id nil)
    ret-keys (keys (.get_executor_node_port assignment))
        _ (log-message "ret-keys: " (pr-str ret-keys)) ]
    ret-keys
    ))

(defn check-distribution [items distribution]
  (let [counts (map long (map count items))]
    (is (Testing/multiseteq counts (map long distribution)))))

(defn disjoint? [& sets]
  (let [combined (apply concat sets)]
    (= (count combined) (count (set combined)))
    ))

(defn executor->tasks [executor-id]
  clojurify-structure (StormCommon/executorIdToTasks executor-id))

(defnk check-consistency [cluster storm-name :assigned? true]
  (let [state (.getClusterState cluster)
        storm-id (StormCommon/getStormId state storm-name)
        task-ids (task-ids cluster storm-id)
        assignment (.assignmentInfo state storm-id nil)
        executor->node+port (.get_executor_node_port assignment)
        task->node+port (StormCommon/taskToNodeport executor->node+port)
        assigned-task-ids (mapcat executor->tasks (keys executor->node+port))
        all-nodes (set (map (fn [np] (.get_node np)) (.values executor->node+port)))]
    (when assigned?
      (is (= (sort task-ids) (sort assigned-task-ids)))
      (doseq [t task-ids]
        (is (not-nil? (.get task->node+port t)))))
    (doseq [[e s] executor->node+port]
      (is (not-nil? s)))

    (is (= all-nodes (set (keys (.get_node_host assignment)))))
    (doseq [[e s] executor->node+port]
      (is (not-nil? (.get (.get_executor_start_time_secs assignment) e))))
    ))

(deftest test-bogusId
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSupervisors 4)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (let [state (.getClusterState cluster)
          nimbus (.getNimbus cluster)]
       (is (thrown? NotAliveException (.getTopologyConf nimbus "bogus-id")))
       (is (thrown? NotAliveException (.getTopology nimbus "bogus-id")))
       (is (thrown? NotAliveException (.getUserTopology nimbus "bogus-id")))
       (is (thrown? NotAliveException (.getTopologyInfo nimbus "bogus-id")))
       (is (thrown? NotAliveException (.uploadNewCredentials nimbus "bogus-id" (Credentials.))))
      )))

(deftest test-assignment
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 4)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (let [state (.getClusterState cluster)
          topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails
                            (TestPlannerSpout. false) (Integer. 3))}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.) (Integer. 4))
                      "3" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "2" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.))})
          topology2 (Thrift/buildTopology
                      {"1" (Thrift/prepareSpoutDetails
                             (TestPlannerSpout. true) (Integer. 12))}
                      {"2" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "1" nil)
                              (Thrift/prepareNoneGrouping)}
                             (TestPlannerBolt.) (Integer. 6))
                       "3" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "1" nil)
                              (Thrift/prepareGlobalGrouping)}
                             (TestPlannerBolt.) (Integer. 8))
                       "4" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "1" nil)
                              (Thrift/prepareGlobalGrouping)
                              (Utils/getGlobalStreamId "2" nil)
                              (Thrift/prepareNoneGrouping)}
                             (TestPlannerBolt.) (Integer. 4))})
          _ (.submitTopology cluster "mystorm" {TOPOLOGY-WORKERS 4} topology)
          _ (.advanceClusterTime cluster 11)
          task-info (storm-component->task-info cluster "mystorm")]
      (check-consistency cluster "mystorm")
      ;; 3 should be assigned once (if it were optimized, we'd have
      ;; different topology)
      (is (= 1 (count (.assignments state nil))))
      (is (= 1 (count (task-info "1"))))
      (is (= 4 (count (task-info "2"))))
      (is (= 1 (count (task-info "3"))))
      (is (= 4 (storm-num-workers state "mystorm")))
      (.submitTopology cluster "storm2" {TOPOLOGY-WORKERS 20} topology2)
      (.advanceClusterTime cluster 11)
      (check-consistency cluster "storm2")
      (is (= 2 (count (.assignments state nil))))
      (let [task-info (storm-component->task-info cluster "storm2")]
        (is (= 12 (count (task-info "1"))))
        (is (= 6 (count (task-info "2"))))
        (is (= 8 (count (task-info "3"))))
        (is (= 4 (count (task-info "4"))))
        (is (= 8 (storm-num-workers state "storm2")))
        )
      )))

(defn isolation-nimbus []
  (let [standalone (Nimbus$StandaloneINimbus.)]
    (reify INimbus
      (prepare [this conf local-dir]
        (.prepare standalone conf local-dir)
        )
      (allSlotsAvailableForScheduling [this supervisors topologies topologies-missing-assignments]
        (.allSlotsAvailableForScheduling standalone supervisors topologies topologies-missing-assignments))
      (assignSlots [this topology slots]
        (.assignSlots standalone topology slots)
        )
      (getForcedScheduler [this]
        (.getForcedScheduler standalone))
      (getHostName [this supervisors node-id]
        node-id
      ))))


(deftest test-auto-credentials
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 6)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false
                                                    TOPOLOGY-ACKER-EXECUTORS 0
                                                    TOPOLOGY-EVENTLOGGER-EXECUTORS 0
                                                    NIMBUS-CREDENTIAL-RENEW-FREQ-SECS 10
                                                    NIMBUS-CREDENTIAL-RENEWERS (list "org.apache.storm.MockAutoCred")
                                                    NIMBUS-AUTO-CRED-PLUGINS (list "org.apache.storm.MockAutoCred")
                                                    })))]
    (let [state (.getClusterState cluster)
          topology-name "test-auto-cred-storm"
          submitOptions (SubmitOptions. TopologyInitialStatus/INACTIVE)
          - (.set_creds submitOptions (Credentials. (HashMap.)))
          topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails
                            (TestPlannerSpout. false) (Integer. 3))}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.) (Integer. 4))
                      "3" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "2" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.))})
          _ (.submitTopologyWithOpts cluster topology-name {TOPOLOGY-WORKERS 4
                                                               TOPOLOGY-AUTO-CREDENTIALS (list "org.apache.storm.MockAutoCred")
                                                               } topology submitOptions)
          credentials (getCredentials cluster topology-name)]
      ; check that the credentials have nimbus auto generated cred
      (is (= (.get credentials MockAutoCred/NIMBUS_CRED_KEY) MockAutoCred/NIMBUS_CRED_VAL))
      ;advance cluster time so the renewers can execute
      (.advanceClusterTime cluster 20)
      ;check that renewed credentials replace the original credential.
      (is (= (.get (getCredentials cluster topology-name) MockAutoCred/NIMBUS_CRED_KEY) MockAutoCred/NIMBUS_CRED_RENEW_VAL))
      (is (= (.get (getCredentials cluster topology-name) MockAutoCred/GATEWAY_CRED_KEY) MockAutoCred/GATEWAY_CRED_RENEW_VAL)))))

(defmacro letlocals
  [& body]
  (let [[tobind lexpr] (split-at (dec (count body)) body)
        binded (vec (mapcat (fn [e]
                              (if (and (list? e) (= 'bind (first e)))
                                [(second e) (last e)]
                                ['_ e]
                                ))
                            tobind))]
    `(let ~binded
       ~(first lexpr))))

(deftest test-isolated-assignment
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 6)
                                      (.withINimbus (isolation-nimbus))
                                      (.withDaemonConf {SUPERVISOR-ENABLE false
                                             TOPOLOGY-ACKER-EXECUTORS 0
                                             TOPOLOGY-EVENTLOGGER-EXECUTORS 0
                                             STORM-SCHEDULER "org.apache.storm.scheduler.IsolationScheduler"
                                             ISOLATION-SCHEDULER-MACHINES {"tester1" 3 "tester2" 2}
                                             NIMBUS-MONITOR-FREQ-SECS 10
                                             })))]
    (letlocals
      (bind state (.getClusterState cluster))
      (bind topology (Thrift/buildTopology
                      {"1" (Thrift/prepareSpoutDetails
                             (TestPlannerSpout. false) (Integer. 3))}
                      {"2" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "1" nil)
                              (Thrift/prepareNoneGrouping)}
                             (TestPlannerBolt.) (Integer. 5))
                       "3" (Thrift/prepareBoltDetails
                             {(Utils/getGlobalStreamId "2" nil)
                              (Thrift/prepareNoneGrouping)}
                             (TestPlannerBolt.))}))

      (.submitTopology cluster "noniso" {TOPOLOGY-WORKERS 4} topology)
      (.advanceClusterTime cluster 11)
      (is (= 4 (topology-num-nodes state "noniso")))
      (is (= 4 (storm-num-workers state "noniso")))

      (.submitTopology cluster "tester1" {TOPOLOGY-WORKERS 6} topology)
      (.submitTopology cluster "tester2" {TOPOLOGY-WORKERS 6} topology)
      (.advanceClusterTime cluster 11)

      (bind task-info-tester1 (storm-component->task-info cluster "tester1"))
      (bind task-info-tester2 (storm-component->task-info cluster "tester2"))


      (is (= 1 (topology-num-nodes state "noniso")))
      (is (= 3 (storm-num-workers state "noniso")))

      (is (= {2 3} (topology-node-distribution state "tester1")))
      (is (= {3 2} (topology-node-distribution state "tester2")))

      (is (apply disjoint? (map (partial topology-nodes state) ["noniso" "tester1" "tester2"])))

      (check-consistency cluster "tester1")
      (check-consistency cluster "tester2")
      (check-consistency cluster "noniso")

      ;;check that nothing gets reassigned
      (bind tester1-slots (topology-slots state "tester1"))
      (bind tester2-slots (topology-slots state "tester2"))
      (bind noniso-slots (topology-slots state "noniso"))
      (.advanceClusterTime cluster 20)
      (is (= tester1-slots (topology-slots state "tester1")))
      (is (= tester2-slots (topology-slots state "tester2")))
      (is (= noniso-slots (topology-slots state "noniso")))

      )))

(deftest test-zero-executor-or-tasks
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 6)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (let [state (.getClusterState cluster)
          topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails
                           (TestPlannerSpout. false) (Integer. 3)
                           {TOPOLOGY-TASKS 0})}
                    {"2" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "1" nil)
                            (Thrift/prepareNoneGrouping)}
                           (TestPlannerBolt.) (Integer. 1)
                           {TOPOLOGY-TASKS 2})
                     "3" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "2" nil)
                            (Thrift/prepareNoneGrouping)}
                           (TestPlannerBolt.) nil
                           {TOPOLOGY-TASKS 5})})
          _ (.submitTopology cluster "mystorm" {TOPOLOGY-WORKERS 4} topology)
          _ (.advanceClusterTime cluster 11)
          task-info (storm-component->task-info cluster "mystorm")]
      (check-consistency cluster "mystorm")
      (is (= 0 (count (task-info "1"))))
      (is (= 2 (count (task-info "2"))))
      (is (= 5 (count (task-info "3"))))
      (is (= 2 (storm-num-workers state "mystorm"))) ;; because only 2 executors
      )))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(deftest test-executor-assignments
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (let [topology (Thrift/buildTopology
                    {"1" (Thrift/prepareSpoutDetails
                           (TestPlannerSpout. true) (Integer. 3)
                           {TOPOLOGY-TASKS 5})}
                    {"2" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "1" nil)
                            (Thrift/prepareNoneGrouping)}
                           (TestPlannerBolt.) (Integer. 8)
                           {TOPOLOGY-TASKS 2})
                     "3" (Thrift/prepareBoltDetails
                           {(Utils/getGlobalStreamId "2" nil)
                            (Thrift/prepareNoneGrouping)}
                           (TestPlannerBolt.) (Integer. 3))})
          _ (.submitTopology cluster "mystorm" {TOPOLOGY-WORKERS 4} topology)
          _ (.advanceClusterTime cluster 11)
          task-info (storm-component->task-info cluster "mystorm")
          executor-info (->> (storm-component->executor-info cluster "mystorm")
                             (map-val #(map executor->tasks %)))]
      (check-consistency cluster "mystorm")
      (is (= 5 (count (task-info "1"))))
      (check-distribution (executor-info "1") [2 2 1])

      (is (= 2 (count (task-info "2"))))
      (check-distribution (executor-info "2") [1 1])

      (is (= 3 (count (task-info "3"))))
      (check-distribution (executor-info "3") [1 1 1])
      )))

(deftest test-over-parallelism-assignment
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 2)
                                      (.withPortsPerSupervisor 5)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (let [state (.getClusterState cluster)
          topology (Thrift/buildTopology
                     {"1" (Thrift/prepareSpoutDetails
                            (TestPlannerSpout. true) (Integer. 21))}
                     {"2" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.) (Integer. 9))
                      "3" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.) (Integer. 2))
                      "4" (Thrift/prepareBoltDetails
                            {(Utils/getGlobalStreamId "1" nil)
                             (Thrift/prepareNoneGrouping)}
                            (TestPlannerBolt.) (Integer. 10))})
          _ (.submitTopology cluster "test" {TOPOLOGY-WORKERS 7} topology)
          _ (.advanceClusterTime cluster 11)
          task-info (storm-component->task-info cluster "test")]
      (check-consistency cluster "test")
      (is (= 21 (count (task-info "1"))))
      (is (= 9 (count (task-info "2"))))
      (is (= 2 (count (task-info "3"))))
      (is (= 10 (count (task-info "4"))))
      (is (= 7 (storm-num-workers state "test")))
    )))

(deftest test-topo-history
  (let [group-mapper (Mockito/mock IGroupMappingServiceProvider)]
    (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                        (.withSimulatedTime)
                                        (.withSupervisors 2)
                                        (.withPortsPerSupervisor 5)
                                        (.withGroupMapper group-mapper)
                                        (.withDaemonConf {SUPERVISOR-ENABLE false
                                                      NIMBUS-ADMINS ["admin-user"]
                                                      NIMBUS-TASK-TIMEOUT-SECS 30
                                                      NIMBUS-MONITOR-FREQ-SECS 10
                                                      TOPOLOGY-ACKER-EXECUTORS 0})))]
      (.thenReturn (Mockito/when (.getGroups group-mapper (Mockito/anyObject))) #{"alice-group"})
      (letlocals
        (bind conf (.getDaemonConf cluster))
        (bind topology (Thrift/buildTopology
                         {"1" (Thrift/prepareSpoutDetails
                                (TestPlannerSpout. true) (Integer. 4))}
                         {}))
        (bind state (.getClusterState cluster))
        ; get topology history when there's no topology history
        (let [hist-topo-ids (vec (sort (.get_topo_ids (.getTopologyHistory (.getNimbus cluster) (System/getProperty "user.name")))))]
             (log-message "Checking user " (System/getProperty "user.name") " " hist-topo-ids)
             (is (= 0 (count hist-topo-ids))))
        (.submitTopology cluster "test" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 20, LOGS-USERS ["alice", (System/getProperty "user.name")]} topology)
        (bind storm-id (StormCommon/getStormId state "test"))
        (.advanceClusterTime cluster 5)
        (is (not-nil? (.stormBase state storm-id nil)))
        (is (not-nil? (.assignmentInfo state storm-id nil)))
        (.killTopology (.getNimbus cluster) "test")
        ;; check that storm is deactivated but alive
        (is (= TopologyStatus/KILLED (.get_status (.stormBase state storm-id nil))))
        (is (not-nil? (.assignmentInfo state storm-id nil)))
        (.advanceClusterTime cluster 35)
        ;; kill topology read on group
        (.submitTopology cluster "killgrouptest" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 20, LOGS-GROUPS ["alice-group"]} topology)
        (bind storm-id-killgroup (StormCommon/getStormId state "killgrouptest"))
        (.advanceClusterTime cluster 5)
        (is (not-nil? (.stormBase state storm-id-killgroup nil)))
        (is (not-nil? (.assignmentInfo state storm-id-killgroup nil)))
        (.killTopology (.getNimbus cluster) "killgrouptest")
        ;; check that storm is deactivated but alive
        (is (= TopologyStatus/KILLED (.get_status (.stormBase state storm-id-killgroup nil))))
        (is (not-nil? (.assignmentInfo state storm-id-killgroup nil)))
        (.advanceClusterTime cluster 35)
        ;; kill topology can't read
        (.submitTopology cluster "killnoreadtest" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 20} topology)
        (bind storm-id-killnoread (StormCommon/getStormId state "killnoreadtest"))
        (.advanceClusterTime cluster 5)
        (is (not-nil? (.stormBase state storm-id-killnoread nil)))
        (is (not-nil? (.assignmentInfo state storm-id-killnoread nil)))
        (.killTopology (.getNimbus cluster) "killnoreadtest")
        ;; check that storm is deactivated but alive
        (is (= TopologyStatus/KILLED (.get_status (.stormBase state storm-id-killnoread nil))))
        (is (not-nil? (.assignmentInfo state storm-id-killnoread nil)))
        (.advanceClusterTime cluster 35)

        ;; active topology can read
        (.submitTopology cluster "2test" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10, LOGS-USERS ["alice", (System/getProperty "user.name")]} topology)
        (.advanceClusterTime cluster 11)
        (bind storm-id2 (StormCommon/getStormId state "2test"))
        (is (not-nil? (.stormBase state storm-id2 nil)))
        (is (not-nil? (.assignmentInfo state storm-id2 nil)))
        ;; active topology can not read
        (.submitTopology cluster "testnoread" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10, LOGS-USERS ["alice"]} topology)
        (.advanceClusterTime cluster 11)
        (bind storm-id3 (StormCommon/getStormId state "testnoread"))
        (is (not-nil? (.stormBase state storm-id3 nil)))
        (is (not-nil? (.assignmentInfo state storm-id3 nil)))
        ;; active topology can read based on group
        (.submitTopology cluster "testreadgroup" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10, LOGS-GROUPS ["alice-group"]} topology)
        (.advanceClusterTime cluster 11)
        (bind storm-id4 (StormCommon/getStormId state "testreadgroup"))
        (is (not-nil? (.stormBase state storm-id4 nil)))
        (is (not-nil? (.assignmentInfo state storm-id4 nil)))
        ;; at this point have 1 running, 1 killed topo
        (let [hist-topo-ids (vec (sort (.get_topo_ids (.getTopologyHistory (.getNimbus cluster) (System/getProperty "user.name")))))]
          (log-message "Checking user " (System/getProperty "user.name") " " hist-topo-ids)
          (is (= 4 (count hist-topo-ids)))
          (is (= storm-id2 (get hist-topo-ids 0)))
          (is (= storm-id-killgroup (get hist-topo-ids 1)))
          (is (= storm-id (get hist-topo-ids 2)))
          (is (= storm-id4 (get hist-topo-ids 3))))
        (let [hist-topo-ids (vec (sort (.get_topo_ids (.getTopologyHistory (.getNimbus cluster) "alice"))))]
          (log-message "Checking user alice " hist-topo-ids)
          (is (= 5 (count hist-topo-ids)))
          (is (= storm-id2 (get hist-topo-ids 0)))
          (is (= storm-id-killgroup (get hist-topo-ids 1)))
          (is (= storm-id (get hist-topo-ids 2)))
          (is (= storm-id3 (get hist-topo-ids 3)))
          (is (= storm-id4 (get hist-topo-ids 4))))
        (let [hist-topo-ids (vec (sort (.get_topo_ids (.getTopologyHistory (.getNimbus cluster) "admin-user"))))]
          (log-message "Checking user admin-user " hist-topo-ids)
          (is (= 6 (count hist-topo-ids)))
          (is (= storm-id2 (get hist-topo-ids 0)))
          (is (= storm-id-killgroup (get hist-topo-ids 1)))
          (is (= storm-id-killnoread (get hist-topo-ids 2)))
          (is (= storm-id (get hist-topo-ids 3)))
          (is (= storm-id3 (get hist-topo-ids 4)))
          (is (= storm-id4 (get hist-topo-ids 5))))
        (let [hist-topo-ids (vec (sort (.get_topo_ids (.getTopologyHistory (.getNimbus cluster) "group-only-user"))))]
          (log-message "Checking user group-only-user " hist-topo-ids)
          (is (= 2 (count hist-topo-ids)))
          (is (= storm-id-killgroup (get hist-topo-ids 0)))
          (is (= storm-id4 (get hist-topo-ids 1))))))))

(deftest test-kill-storm
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 2)
                                      (.withPortsPerSupervisor 5)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-TIMEOUT-SECS 30
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (letlocals
      (bind conf (.getDaemonConf cluster))
      (bind topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 14))}
                       {}))
      (bind state (.getClusterState cluster))
      (.submitTopology cluster "test" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 20} topology)
      (bind storm-id (StormCommon/getStormId state "test"))
      (.advanceClusterTime cluster 15)
      (is (not-nil? (.stormBase state storm-id nil)))
      (is (not-nil? (.assignmentInfo state storm-id nil)))
      (.killTopology (.getNimbus cluster) "test")
      ;; check that storm is deactivated but alive
      (is (= TopologyStatus/KILLED (.get_status (.stormBase state storm-id nil))))
      (is (not-nil? (.assignmentInfo state storm-id nil)))
      (.advanceClusterTime cluster 18)
      ;; check that storm is deactivated but alive
      (is (= 1 (count (.heartbeatStorms state))))
      (.advanceClusterTime cluster 3)
      (is (nil? (.stormBase state storm-id nil)))
      (is (nil? (.assignmentInfo state storm-id nil)))

      ;; cleanup happens on monitoring thread
      (.advanceClusterTime cluster 11)
      (is (empty? (.heartbeatStorms state)))
      ;; TODO: check that code on nimbus was cleaned up locally...

      (is (thrown? NotAliveException (.killTopology (.getNimbus cluster) "lalala")))
      (.submitTopology cluster "2test" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 10} topology)
      (.advanceClusterTime cluster 11)
      (is (thrown? AlreadyAliveException (.submitTopology cluster "2test" {} topology)))
      (.advanceClusterTime cluster 11)
      (bind storm-id (StormCommon/getStormId state "2test"))
      (is (not-nil? (.stormBase state storm-id nil)))
      (.killTopology (.getNimbus cluster) "2test")
      (is (thrown? AlreadyAliveException (.submitTopology cluster "2test" {} topology)))
      (.advanceClusterTime cluster 11)
      (is (= 1 (count (.heartbeatStorms state))))

      (.advanceClusterTime cluster 6)
      (is (nil? (.stormBase state storm-id nil)))
      (is (nil? (.assignmentInfo state storm-id nil)))
      (.advanceClusterTime cluster 11)
      (is (= 0 (count (.heartbeatStorms state))))

      (.submitTopology cluster "test3" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 5} topology)
      (bind storm-id3 (StormCommon/getStormId state "test3"))
      (.advanceClusterTime cluster 11)
      (.removeStorm state storm-id3)
      (is (nil? (.stormBase state storm-id3 nil)))
      (is (nil? (.assignmentInfo state storm-id3 nil)))

      (.advanceClusterTime cluster 11)
      (is (= 0 (count (.heartbeatStorms state))))

      ;; this guarantees that monitor thread won't trigger for 10 more seconds
      (Time/advanceTimeSecs 11)
      (.waitForIdle cluster)

      (.submitTopology cluster "test3" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 5} topology)
      (bind storm-id3 (StormCommon/getStormId state "test3"))

      (.advanceClusterTime cluster 11)
      (bind executor-id (first (topology-executors cluster storm-id3)))

      (do-executor-heartbeat cluster storm-id3 executor-id)

      (.killTopology (.getNimbus cluster) "test3")
      (.advanceClusterTime cluster 6)
      (is (= 1 (count (.heartbeatStorms state))))
      (.advanceClusterTime cluster 5)
      (is (= 0 (count (.heartbeatStorms state))))

      ;; test kill with opts
      (.submitTopology cluster "test4" {TOPOLOGY-MESSAGE-TIMEOUT-SECS 100} topology)
      (.advanceClusterTime cluster 11)
      (.killTopologyWithOpts (.getNimbus cluster) "test4" (doto (KillOptions.) (.set_wait_secs 10)))
      (bind storm-id4 (StormCommon/getStormId state "test4"))
      (.advanceClusterTime cluster 9)
      (is (not-nil? (.assignmentInfo state storm-id4 nil)))
      (.advanceClusterTime cluster 2)
      (is (nil? (.assignmentInfo state storm-id4 nil)))
      )))

(deftest test-reassignment
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 2)
                                      (.withPortsPerSupervisor 5)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-LAUNCH-SECS 60
                  NIMBUS-TASK-TIMEOUT-SECS 20
                  NIMBUS-MONITOR-FREQ-SECS 10
                  NIMBUS-SUPERVISOR-TIMEOUT-SECS 100
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (letlocals
      (bind conf (.getDaemonConf cluster))
      (bind topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 2))}
                       {}))
      (bind state (.getClusterState cluster))
      (.submitTopology cluster "test" {TOPOLOGY-WORKERS 2} topology)
      (.advanceClusterTime cluster 11)
      (check-consistency cluster "test")
      (bind storm-id (StormCommon/getStormId state "test"))
      (bind [executor-id1 executor-id2]  (topology-executors cluster storm-id))
      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
      (bind _ (log-message "ass1, t0: " (pr-str ass1)))
      (bind _ (log-message "ass2, t0: " (pr-str ass2)))

      (.advanceClusterTime cluster 30)
      (bind _ (log-message "ass1, t30, pre beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t30, pre beat: " (pr-str ass2)))
      (do-executor-heartbeat cluster storm-id executor-id1)
      (do-executor-heartbeat cluster storm-id executor-id2)
      (bind _ (log-message "ass1, t30, post beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t30, post beat: " (pr-str ass2)))

      (.advanceClusterTime cluster 13)
      (bind _ (log-message "ass1, t43, pre beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t43, pre beat: " (pr-str ass2)))
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (= ass2 (executor-assignment cluster storm-id executor-id2)))
      (do-executor-heartbeat cluster storm-id executor-id1)
      (bind _ (log-message "ass1, t43, post beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t43, post beat: " (pr-str ass2)))

      (.advanceClusterTime cluster 11)
      (bind _ (log-message "ass1, t54, pre beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t54, pre beat: " (pr-str ass2)))
      (do-executor-heartbeat cluster storm-id executor-id1)
      (bind _ (log-message "ass1, t54, post beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t54, post beat: " (pr-str ass2)))
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (check-consistency cluster "test")

      ; have to wait an extra 10 seconds because nimbus may not
      ; resynchronize its heartbeat time till monitor-time secs after
      (.advanceClusterTime cluster 11)
      (bind _ (log-message "ass1, t65, pre beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t65, pre beat: " (pr-str ass2)))
      (do-executor-heartbeat cluster storm-id executor-id1)
      (bind _ (log-message "ass1, t65, post beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t65, post beat: " (pr-str ass2)))
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (check-consistency cluster "test")

      (.advanceClusterTime cluster 11)
      (bind _ (log-message "ass1, t76, pre beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t76, pre beat: " (pr-str ass2)))
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (not= ass2 (executor-assignment cluster storm-id executor-id2)))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
      (bind _ (log-message "ass1, t76, post beat: " (pr-str ass1)))
      (bind _ (log-message "ass2, t76, post beat: " (pr-str ass2)))
      (check-consistency cluster "test")

      (.advanceClusterTime cluster 31)
      (is (not= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (= ass2 (executor-assignment cluster storm-id executor-id2)))  ; tests launch timeout
      (check-consistency cluster "test")


      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind active-supervisor (.get_node ass2))
      (.killSupervisor cluster active-supervisor)

      (doseq [i (range 12)]
        (do-executor-heartbeat cluster storm-id executor-id1)
        (do-executor-heartbeat cluster storm-id executor-id2)
        (.advanceClusterTime cluster 10)
        )
      ;; tests that it doesn't reassign executors if they're heartbeating even if supervisor times out
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (= ass2 (executor-assignment cluster storm-id executor-id2)))
      (check-consistency cluster "test")

      (.advanceClusterTime cluster 30)

      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
      (is (not-nil? ass1))
      (is (not-nil? ass2))
      (is (not= active-supervisor (.get_node (executor-assignment cluster storm-id executor-id2))))
      (is (not= active-supervisor (.get_node (executor-assignment cluster storm-id executor-id1))))
      (check-consistency cluster "test")

      (doseq [supervisor-id (.supervisors state nil)]
        (.killSupervisor cluster supervisor-id))

      (.advanceClusterTime cluster 90)
      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))
      (is (nil? ass1))
      (is (nil? ass2))
      (check-consistency cluster "test" :assigned? false)

      (.addSupervisor cluster)
      (.advanceClusterTime cluster 11)
      (check-consistency cluster "test")
      )))


(deftest test-reassignment-to-constrained-cluster
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 0)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-LAUNCH-SECS 60
                  NIMBUS-TASK-TIMEOUT-SECS 20
                  NIMBUS-MONITOR-FREQ-SECS 10
                  NIMBUS-SUPERVISOR-TIMEOUT-SECS 100
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (letlocals
      (.addSupervisor cluster 1 "a")
      (.addSupervisor cluster 1 "b")
      (bind conf (.getDaemonConf cluster))
      (bind topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 2))}
                       {}))
      (bind state (.getClusterState cluster))
      (.submitTopology cluster "test" {TOPOLOGY-WORKERS 2} topology)
      (.advanceClusterTime cluster 11)
      (check-consistency cluster "test")
      (bind storm-id (StormCommon/getStormId state "test"))
      (bind [executor-id1 executor-id2]  (topology-executors cluster storm-id))
      (bind ass1 (executor-assignment cluster storm-id executor-id1))
      (bind ass2 (executor-assignment cluster storm-id executor-id2))

      (.advanceClusterTime cluster 30)
      (do-executor-heartbeat cluster storm-id executor-id1)
      (do-executor-heartbeat cluster storm-id executor-id2)

      (.advanceClusterTime cluster 13)
      (is (= ass1 (executor-assignment cluster storm-id executor-id1)))
      (is (= ass2 (executor-assignment cluster storm-id executor-id2)))
      (.killSupervisor cluster "b")
      (do-executor-heartbeat cluster storm-id executor-id1)

      (.advanceClusterTime cluster 11)
      (do-executor-heartbeat cluster storm-id executor-id1)

      (.advanceClusterTime cluster 11)
      (do-executor-heartbeat cluster storm-id executor-id1)

      (.advanceClusterTime cluster 11)
      (do-executor-heartbeat cluster storm-id executor-id1)

      (.advanceClusterTime cluster 11)
      (do-executor-heartbeat cluster storm-id executor-id1)

      (check-consistency cluster "test")
      (is (= 1 (storm-num-workers state "test")))
      )))

(defn check-executor-distribution [slot-executors distribution]
  (check-distribution (vals slot-executors) distribution))

(defn check-num-nodes [slot-executors num-nodes]
  (let [nodes (->> slot-executors keys (map (fn [np] (.get_node np))) set)]
    (is (= num-nodes (count nodes)))
    ))

(deftest test-reassign-squeezed-topology
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 1)
                                      (.withPortsPerSupervisor 1)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false
                  NIMBUS-TASK-LAUNCH-SECS 60
                  NIMBUS-TASK-TIMEOUT-SECS 20
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (letlocals
      (bind topology (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 9))}
                        {}))
      (bind state (.getClusterState cluster))
      (.submitTopology cluster "test" {TOPOLOGY-WORKERS 4} topology)  ; distribution should be 2, 2, 2, 3 ideally
      (.advanceClusterTime cluster 11)
      (bind storm-id (StormCommon/getStormId state "test"))
      (bind slot-executors (slot-assignments cluster storm-id))
      (check-executor-distribution slot-executors [9])
      (check-consistency cluster "test")

      (.addSupervisor cluster 2)
      (.advanceClusterTime cluster 11)
      (bind slot-executors (slot-assignments cluster storm-id))
      (bind executor->start (executor-start-times cluster storm-id))
      (check-executor-distribution slot-executors [3 3 3])
      (check-consistency cluster "test")

      (.addSupervisor cluster 8)
      ;; this actually works for any time > 0, since zookeeper fires an event causing immediate reassignment
      ;; doesn't work for time = 0 because it's not waiting for cluster yet, so test might happen before reassignment finishes
      (.advanceClusterTime cluster 11)
      (bind slot-executors2 (slot-assignments cluster storm-id))
      (bind executor->start2 (executor-start-times cluster storm-id))
      (check-executor-distribution slot-executors2 [2 2 2 3])
      (check-consistency cluster "test")

      (bind common (first (Utils/findOne (proxy [IPredicate] []
                                           (test [[k v]] (= 3 (count v)))) slot-executors2)))
      (is (not-nil? common))
      (is (= (slot-executors2 common) (slot-executors common)))

      ;; check that start times are changed for everything but the common one
      (bind same-executors (slot-executors2 common))
      (bind changed-executors (apply concat (vals (dissoc slot-executors2 common))))
      (doseq [t same-executors]
        (is (= (executor->start t) (executor->start2 t))))
      (doseq [t changed-executors]
        (is (not= (executor->start t) (executor->start2 t))))
      )))

(deftest test-get-owner-resource-summaries
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                    (.withSimulatedTime)
                                    (.withSupervisors 1)
                                    (.withPortsPerSupervisor 12)
                                    (.withDaemonConf
                                      {SUPERVISOR-ENABLE false
                                       NIMBUS-MONITOR-FREQ-SECS 10
                                       TOPOLOGY-MESSAGE-TIMEOUT-SECS 30
                                       TOPOLOGY-ACKER-EXECUTORS 0
                                       TOPOLOGY-EVENTLOGGER-EXECUTORS 0
                                       })))]
    (letlocals
      ;;test for 0-topology case
      (.advanceClusterTime cluster 11)
      (bind owner-resource-summaries (.getOwnerResourceSummaries (.getNimbus cluster) nil))
      (bind summary (first owner-resource-summaries))
      (is (nil? summary))

      ;;test for 1-topology case
      (bind topology (Thrift/buildTopology
                       {"1" (Thrift/prepareSpoutDetails
                              (TestPlannerSpout. true) (Integer. 3))}
                       {}))
      (.submitTopology cluster
                       "test"
                       {TOPOLOGY-WORKERS              3
                        TOPOLOGY-MESSAGE-TIMEOUT-SECS 90} topology)
      (.advanceClusterTime cluster 11)

      (bind owner-resource-summaries (.getOwnerResourceSummaries (.getNimbus cluster) nil))
      (bind summary (first owner-resource-summaries))
      (is (= (.get_total_workers summary) 3))
      (is (= (.get_total_executors summary)) 3)
      (is (= (.get_total_topologies summary)) 1)

      ;;test for many-topology case
      (bind topology2 (Thrift/buildTopology
                        {"2" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 4))}
                        {}))
      (bind topology3 (Thrift/buildTopology
                        {"3" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 5))}
                        {}))

      (.submitTopology cluster
                       "test2"
                       {TOPOLOGY-WORKERS              4
                        TOPOLOGY-MESSAGE-TIMEOUT-SECS 90} topology2)

      (.submitTopology cluster
                       "test3"
                       {TOPOLOGY-WORKERS              3
                        TOPOLOGY-MESSAGE-TIMEOUT-SECS 90} topology3)
      (.advanceClusterTime cluster 11)

      (bind owner-resource-summaries (.getOwnerResourceSummaries (.getNimbus cluster) nil))
      (bind summary (first owner-resource-summaries))
      (is (= (.get_total_workers summary) 10))
      (is (= (.get_total_executors summary)) 12)
      (is (= (.get_total_topologies summary)) 3)

      ;;test for specific owner
      (bind owner-resource-summaries (.getOwnerResourceSummaries (.getNimbus cluster) (System/getProperty "user.name")))
      (bind summary (first owner-resource-summaries))
      (is (= (.get_total_workers summary) 10))
      (is (= (.get_total_executors summary)) 12)
      (is (= (.get_total_topologies summary)) 3)

      ;;test for other user
      (bind other-user (str "not-" (System/getProperty "user.name")))
      (bind owner-resource-summaries (.getOwnerResourceSummaries (.getNimbus cluster) other-user))
      (bind summary (first owner-resource-summaries))
      (is (= (.get_total_workers summary) 0))
      (is (= (.get_total_executors summary)) 0)
      (is (= (.get_total_topologies summary)) 0)
      )))

(deftest test-rebalance
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 1)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-MESSAGE-TIMEOUT-SECS 30
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (letlocals
      (bind topology (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 3))}
                        {}))
      (bind state (.getClusterState cluster))
      (.submitTopology cluster
                             "test"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 60} topology)
      (.advanceClusterTime cluster 11)
      (bind storm-id (StormCommon/getStormId state "test"))
      (.addSupervisor cluster 3)
      (.addSupervisor cluster 3)

      (.advanceClusterTime cluster 11)

      (bind slot-executors (slot-assignments cluster storm-id))
      ;; check that all workers are on one machine
      (check-executor-distribution slot-executors [1 1 1])
      (check-num-nodes slot-executors 1)
      (.rebalance (.getNimbus cluster) "test" (RebalanceOptions.))

      (.advanceClusterTime cluster 30)
      (check-executor-distribution slot-executors [1 1 1])
      (check-num-nodes slot-executors 1)


      (.advanceClusterTime cluster 30)
      (bind slot-executors (slot-assignments cluster storm-id))
      (check-executor-distribution slot-executors [1 1 1])
      (check-num-nodes slot-executors 3)

      (is (thrown? InvalidTopologyException
                   (.rebalance (.getNimbus cluster) "test"
                     (doto (RebalanceOptions.)
                       (.set_num_executors {"1" (int 0)})
                       ))))
      )))

;TODO: when translating this function, you should replace the map-val with a proper for loop HERE
(deftest test-rebalance-change-parallelism
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 4)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (letlocals
      (bind topology (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 6)
                                {TOPOLOGY-TASKS 12})}
                        {}))
      (bind state (.getClusterState cluster))
      (.submitTopology cluster
                             "test"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 30} topology)
      (.advanceClusterTime cluster 11)
      (bind storm-id (StormCommon/getStormId state "test"))
      (bind checker (fn [distribution]
                      (check-executor-distribution
                        (slot-assignments cluster storm-id)
                        distribution)))
      (checker [2 2 2])

      (.rebalance (.getNimbus cluster) "test"
                  (doto (RebalanceOptions.)
                    (.set_num_workers (int 6))
                    ))
      (.advanceClusterTime cluster 29)
      (checker [2 2 2])
      (.advanceClusterTime cluster 3)
      (checker [1 1 1 1 1 1])

      (.rebalance (.getNimbus cluster) "test"
                  (doto (RebalanceOptions.)
                    (.set_num_executors {"1" (int 1)})
                    ))
      (.advanceClusterTime cluster 29)
      (checker [1 1 1 1 1 1])
      (.advanceClusterTime cluster 3)
      (checker [1])

      (.rebalance (.getNimbus cluster) "test"
                  (doto (RebalanceOptions.)
                    (.set_num_executors {"1" (int 8)})
                    (.set_num_workers 4)
                    ))
      (.advanceClusterTime cluster 32)
      (checker [2 2 2 2])
      (check-consistency cluster "test")

      (bind executor-info (->> (storm-component->executor-info cluster "test")
                               (map-val #(map executor->tasks %))))
      (check-distribution (executor-info "1") [2 2 2 2 1 1 1 1])

      )))


(defn check-for-collisions [state]
 (log-message "Checking for collision")
 (let [assignments (.assignments state nil)]
   (log-message "Assignemts: " assignments)
   (let [id->node->ports (into {} (for [id assignments
                                                :let [executor->node+port (.get_executor_node_port (.assignmentInfo state id nil))
                                                      node+ports (set (.values executor->node+port))
                                                      node->ports (apply merge-with (fn [a b] (distinct (concat a b))) (for [np node+ports] {(.get_node np) [(first (.get_port np))]}))]]
                                                {id node->ports}))
         _ (log-message "id->node->ports: " id->node->ports)
         all-nodes (apply merge-with (fn [a b]
                                        (let [ret (concat a b)]
                                              (log-message "Can we combine " (pr-str a) " and " (pr-str b) " without collisions? " (apply distinct? ret) " => " (pr-str ret))
                                              (is (apply distinct? ret))
                                              (distinct ret)))
                          (.values id->node->ports))]
)))

(deftest test-rebalance-constrained-cluster
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withSupervisors 1)
                                      (.withPortsPerSupervisor 4)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false
                  NIMBUS-MONITOR-FREQ-SECS 10
                  TOPOLOGY-MESSAGE-TIMEOUT-SECS 30
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (letlocals
      (bind topology (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 3))}
                        {}))
      (bind topology2 (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 3))}
                        {}))
      (bind topology3 (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 3))}
                        {}))
      (bind state (.getClusterState cluster))
      (.submitTopology cluster
                             "test"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 90} topology)
      (.submitTopology cluster
                             "test2"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 90} topology2)
      (.submitTopology cluster
                             "test3"
                             {TOPOLOGY-WORKERS 3
                              TOPOLOGY-MESSAGE-TIMEOUT-SECS 90} topology3)

      (.advanceClusterTime cluster 11)

      (check-for-collisions state)
      (.rebalance (.getNimbus cluster) "test" (doto (RebalanceOptions.)
                    (.set_num_workers 4)
                    (.set_wait_secs 0)
                    ))

      (.advanceClusterTime cluster 11)
      (check-for-collisions state)

      (.advanceClusterTime cluster 30)
      (check-for-collisions state)
      )))


(deftest test-submit-invalid
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withSimulatedTime)
                                      (.withDaemonConf {SUPERVISOR-ENABLE false
                  TOPOLOGY-ACKER-EXECUTORS 0
                  TOPOLOGY-EVENTLOGGER-EXECUTORS 0
                  NIMBUS-EXECUTORS-PER-TOPOLOGY 8
                  NIMBUS-SLOTS-PER-TOPOLOGY 8})))]
    (letlocals
      (bind topology (Thrift/buildTopology
                        {"1" (Thrift/prepareSpoutDetails
                               (TestPlannerSpout. true) (Integer. 1)
                               {TOPOLOGY-TASKS 1})}
                        {}))
      (is (thrown? InvalidTopologyException
        (.submitTopology cluster
                               "test/aaa"
                               {}
                               topology)))
      (bind topology (Thrift/buildTopology
                      {"1" (Thrift/prepareSpoutDetails
                             (TestPlannerSpout. true) (Integer. 16)
                             {TOPOLOGY-TASKS 16})}
                      {}))
      (bind state (.getClusterState cluster))
      (is (thrown? InvalidTopologyException
                   (.submitTopology cluster
                                          "test"
                                          {TOPOLOGY-WORKERS 3}
                                          topology)))
      (bind topology (Thrift/buildTopology
                      {"1" (Thrift/prepareSpoutDetails
                             (TestPlannerSpout. true) (Integer. 5)
                             {TOPOLOGY-TASKS 5})}
                      {}))
      (is (thrown? InvalidTopologyException
                   (.submitTopology cluster
                                          "test"
                                          {TOPOLOGY-WORKERS 16}
                                          topology))))))

(deftest test-clean-inbox
  "Tests that the inbox correctly cleans jar files."
  (with-open [_ (Time$SimulatedTime.)
              tmp-path (TmpPath. )]
    (let [dir-location (.getPath tmp-path)
          dir (File. dir-location)
          mk-file (fn [name seconds-ago]
                    (let [f (File. (str dir-location "/" name))
                          t (- (Time/currentTimeMillis) (* seconds-ago 1000))]
                      (FileUtils/touch f)
                      (.setLastModified f t)))
          assert-files-in-dir (fn [compare-file-names]
                                (let [file-names (map #(.getName %) (file-seq dir))]
                                  (is (= (sort compare-file-names)
                                        (sort (filter #(.endsWith % ".jar") file-names))
                                        ))))]
      ;; Make three files a.jar, b.jar, c.jar.
      ;; a and b are older than c and should be deleted first.
      (Time/advanceTimeSecs 100)
      (doseq [fs [["a.jar" 20] ["b.jar" 20] ["c.jar" 0]]]
        (apply mk-file fs))
      (assert-files-in-dir ["a.jar" "b.jar" "c.jar"])
      (Nimbus/cleanInbox dir-location 10)
      (assert-files-in-dir ["c.jar"])
      ;; Cleanit again, c.jar should stay
      (Time/advanceTimeSecs 5)
      (Nimbus/cleanInbox dir-location 10)
      (assert-files-in-dir ["c.jar"])
      ;; Advance time, clean again, c.jar should be deleted.
      (Time/advanceTimeSecs 5)
      (Nimbus/cleanInbox dir-location 10)
      (assert-files-in-dir [])
      )))

(defn wait-for-status [nimbus name status]
  (Testing/whileTimeout 5000
    (reify Testing$Condition
      (exec [this]
        (let [topo-summary (first (filter (fn [topo] (= name (.get_name topo))) (.get_topologies (.getClusterInfo nimbus))))
              topo-status (if topo-summary (.get_status topo-summary) "NOT-RUNNING")]
          (log-message "WAITING FOR "name" TO BE " status " CURRENT " topo-status)
          (not= topo-status status))))
    (fn [] (Thread/sleep 100))))

(deftest test-leadership
  "Tests that leader actions can only be performed by master and non leader fails to perform the same actions."
  (with-open [zk (InProcessZookeeper. )]
    (with-open [tmp-nimbus-dir (TmpPath.)
                _ (MockedZookeeper. (proxy [Zookeeper] []
                      (zkLeaderElectorImpl [conf blob-store] (MockLeaderElector. ))))]
      (let [nimbus-dir (.getPath tmp-nimbus-dir)]
        (letlocals
          (bind conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                       {STORM-ZOOKEEPER-SERVERS ["localhost"]
                        STORM-CLUSTER-MODE "local"
                        STORM-ZOOKEEPER-PORT (.getPort zk)
                        STORM-LOCAL-DIR nimbus-dir}))
          (bind cluster-state (ClusterUtils/mkStormClusterState conf nil (ClusterStateContext.)))
          (bind nimbus (mk-nimbus conf (Nimbus$StandaloneINimbus.) nil nil nil nil))
          (.launchServer nimbus)
          (bind topology (Thrift/buildTopology
                           {"1" (Thrift/prepareSpoutDetails
                                  (TestPlannerSpout. true) (Integer. 3))}
                           {}))

          (with-open [_ (MockedZookeeper. (proxy [Zookeeper] []
                          (zkLeaderElectorImpl [conf blob-store] (MockLeaderElector. false))))]

            (letlocals
              (bind non-leader-cluster-state (ClusterUtils/mkStormClusterState conf nil (ClusterStateContext.)))
              (bind non-leader-nimbus (mk-nimbus conf (Nimbus$StandaloneINimbus.) nil nil nil nil))
              (.launchServer non-leader-nimbus)

              ;first we verify that the master nimbus can perform all actions, even with another nimbus present.
              (.submitTopology nimbus "t1" nil "{}" topology)
              ;; Instead of sleeping until topology is scheduled, rebalance topology so mk-assignments is called.
              (.rebalance nimbus "t1" (doto (RebalanceOptions.) (.set_wait_secs 0)))
              (wait-for-status nimbus "t1" "ACTIVE")
              (.deactivate nimbus "t1")
              (.activate nimbus "t1")
              (.rebalance nimbus "t1" (RebalanceOptions.))
              (.killTopology nimbus "t1")

              ;now we verify that non master nimbus can not perform any of the actions.
              (is (thrown? RuntimeException
                    (.submitTopology non-leader-nimbus
                      "failing"
                      nil
                      "{}"
                      topology)))

              (is (thrown? RuntimeException
                    (.killTopology non-leader-nimbus
                      "t1")))

              (is (thrown? RuntimeException
                    (.activate non-leader-nimbus "t1")))

              (is (thrown? RuntimeException
                    (.deactivate non-leader-nimbus "t1")))

              (is (thrown? RuntimeException
                    (.rebalance non-leader-nimbus "t1" (RebalanceOptions.))))
              (.shutdown non-leader-nimbus)
              (.disconnect non-leader-cluster-state)
              ))
          (.shutdown nimbus)
          (.disconnect cluster-state))))))

(deftest test-nimbus-iface-submitTopologyWithOpts-checks-authorization
  (with-open [cluster (.build (doto (LocalCluster$Builder. )
                                      (.withDaemonConf {NIMBUS-AUTHORIZER
                          "org.apache.storm.security.auth.authorizer.DenyAuthorizer"})))]
    (let [
          topology (Thrift/buildTopology {} {})
         ]
      (is (thrown? AuthorizationException
          (.submitTopologyWithOpts cluster "mystorm" {} topology
            (SubmitOptions. TopologyInitialStatus/INACTIVE))
        ))
    )
  )
)

(deftest test-nimbus-iface-methods-check-authorization
  (let [cluster-state (Mockito/mock IStormClusterState)
        blob-store (Mockito/mock BlobStore)]
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder. )
                            (.withClusterState cluster-state)
                            (.withBlobStore blob-store)
                            (.withDaemonConf {NIMBUS-AUTHORIZER "org.apache.storm.security.auth.authorizer.DenyAuthorizer"})))]
      (let [nimbus (.getNimbus cluster)
            topology-name "test"
            topology-id "test-id"]
        (.thenReturn (Mockito/when (.getTopoId cluster-state topology-name)) (Optional/of topology-id))
        (is (thrown? AuthorizationException
          (.rebalance nimbus topology-name (RebalanceOptions.))))
        (is (thrown? AuthorizationException
          (.activate nimbus topology-name)))
        (is (thrown? AuthorizationException
          (.deactivate nimbus topology-name)))))))

(deftest test-nimbus-check-authorization-params
  (let [cluster-state (Mockito/mock IStormClusterState)
        blob-store (Mockito/mock BlobStore)]
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder. )
                            (.withClusterState cluster-state)
                            (.withBlobStore blob-store)
                            (.withNimbusWrapper (reify UnaryOperator (apply [this nimbus] (Mockito/spy nimbus))))
                            (.withDaemonConf {NIMBUS-AUTHORIZER "org.apache.storm.security.auth.authorizer.NoopAuthorizer"})))]
    (let [nimbus (.getNimbus cluster)
          topology-name "test-nimbus-check-autho-params"
          topology-id "fake-id"
          topology (Thrift/buildTopology {} {})
          expected-name topology-name
          expected-conf {TOPOLOGY-NAME expected-name
                         "foo" "bar"}]
      (.thenReturn (Mockito/when (.getTopoId cluster-state topology-name)) (Optional/of topology-id))
      (.thenReturn (Mockito/when (.readTopologyConf blob-store (Mockito/any String) (Mockito/anyObject))) expected-conf)
      (.thenReturn (Mockito/when (.readTopology blob-store (Mockito/any String) (Mockito/anyObject))) nil)
      (testing "getTopologyConf calls check-authorization! with the correct parameters."
      (let [expected-operation "getTopologyConf"]
          (try
            (is (= expected-conf
                   (->> (.getTopologyConf nimbus topology-id)
                        JSONValue/parse
                        clojurify-structure)))
            (catch NotAliveException e)
            (finally
              (.checkAuthorization (Mockito/verify nimbus) nil nil "getClusterInfo")
              (.checkAuthorization (Mockito/verify nimbus) (Mockito/eq topology-name) (Mockito/any Map) (Mockito/eq expected-operation))))))

      (testing "getTopology calls check-authorization! with the correct parameters."
        (let [expected-operation "getTopology"
              common-spy (->>
                           (proxy [StormCommon] []
                                  (systemTopologyImpl [conf topology] nil))
                         Mockito/spy)]
          (with-open [- (StormCommonInstaller. common-spy)]
            (try
              (.getTopology nimbus topology-id)
              (catch NotAliveException e)
              (finally
                (.checkAuthorization (Mockito/verify nimbus) (Mockito/eq topology-name) (Mockito/any Map) (Mockito/eq expected-operation))
                (. (Mockito/verify common-spy)
                  (systemTopologyImpl (Matchers/any Map)
                                      (Matchers/any))))))))

      (testing "getUserTopology calls check-authorization with the correct parameters."
        (let [expected-operation "getUserTopology"]
          (try
            (.getUserTopology nimbus topology-id)
            (catch NotAliveException e)
            (finally
              (.checkAuthorization (Mockito/verify nimbus) (Mockito/eq topology-name) (Mockito/any Map) (Mockito/eq expected-operation))
              ;;One for this time and one for getTopology call
              (.readTopology (Mockito/verify blob-store (Mockito/times 2)) (Mockito/eq topology-id) (Mockito/anyObject))))))))))

(deftest test-check-authorization-getSupervisorPageInfo
  (let [cluster-state (Mockito/mock IStormClusterState)
        blob-store (Mockito/mock BlobStore)]
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder. )
                            (.withClusterState cluster-state)
                            (.withBlobStore blob-store)
                            (.withNimbusWrapper (reify UnaryOperator (apply [this nimbus] (Mockito/spy nimbus))))
                            (.withDaemonConf {NIMBUS-AUTHORIZER "org.apache.storm.security.auth.authorizer.NoopAuthorizer"})))]
    (let [nimbus (.getNimbus cluster)
          expected-name "test-nimbus-check-autho-params"
          expected-conf {TOPOLOGY-NAME expected-name
                         TOPOLOGY-WORKERS 1
                         TOPOLOGY-MESSAGE-TIMEOUT-SECS 30
                         "foo" "bar"}
          expected-operation "getTopology"
          assignment (doto (Assignment.)
                       (.set_executor_node_port {[1 1] (NodeInfo. "super1" #{1}),
                                                 [2 2] (NodeInfo. "super2" #{2})}))
          topology (doto (StormTopology. )
                     (.set_spouts {})
                     (.set_bolts {})
                     (.set_state_spouts {}))
          topo-assignment {expected-name assignment}
          check-auth-state (atom [])
          mock-check-authorization (fn [nimbus storm-name storm-conf operation]
                                     (swap! check-auth-state conj {:nimbus nimbus
                                                                   :storm-name storm-name
                                                                   :storm-conf storm-conf
                                                                   :operation operation}))
          all-supervisors (doto (HashMap.)
                            (.put "super1" (doto (SupervisorInfo.) (.set_hostname "host1") (.set_meta [(long 1234)])
                                             (.set_uptime_secs (long 123)) (.set_meta [1 2 3]) (.set_used_ports []) (.set_resources_map {})))
                            (.put "super2" (doto (SupervisorInfo.) (.set_hostname "host2") (.set_meta [(long 1234)])
                                             (.set_uptime_secs (long 123)) (.set_meta [1 2 3]) (.set_used_ports []) (.set_resources_map {}))))]
      (.thenReturn (Mockito/when (.allSupervisorInfo cluster-state)) all-supervisors)
      (.thenReturn (Mockito/when (.readTopologyConf blob-store (Mockito/any String) (Mockito/any Subject))) expected-conf)
      (.thenReturn (Mockito/when (.readTopology blob-store (Mockito/any String) (Mockito/any Subject))) topology)
      (.thenReturn (Mockito/when (.topologyAssignments cluster-state)) topo-assignment)
      (.getSupervisorPageInfo nimbus "super1" nil true)

      ;; afterwards, it should get called twice
      (.checkAuthorization (Mockito/verify nimbus) (Mockito/eq expected-name) (Mockito/any Map) (Mockito/eq "getSupervisorPageInfo"))
      (.checkAuthorization (Mockito/verify nimbus) nil nil "getClusterInfo")
      (.checkAuthorization (Mockito/verify nimbus) (Mockito/eq expected-name) (Mockito/any Map) (Mockito/eq "getTopology"))))))

(deftest test-nimbus-iface-getTopology-methods-throw-correctly
  (with-open [cluster (LocalCluster. )]
    (let [
          nimbus (.getNimbus cluster)
          id "bogus ID"
         ]
      (is (thrown? NotAliveException (.getTopology nimbus id)))
      (try
        (.getTopology nimbus id)
        (catch NotAliveException e
           (is (= id (.get_msg e)))
        )
      )

      (is (thrown? NotAliveException (.getTopologyConf nimbus id)))
      (try (.getTopologyConf nimbus id)
        (catch NotAliveException e
           (is (= id (.get_msg e)))
        )
      )

      (is (thrown? NotAliveException (.getTopologyInfo nimbus id)))
      (try (.getTopologyInfo nimbus id)
        (catch NotAliveException e
           (is (= id (.get_msg e)))
        )
      )

      (is (thrown? NotAliveException (.getUserTopology nimbus id)))
      (try (.getUserTopology nimbus id)
        (catch NotAliveException e
           (is (= id (.get_msg e)))
        )
      )
    )
  )
)

(defn mkStormBase [launch-time-secs storm-name status]
  (doto (StormBase.)
    (.set_name storm-name)
    (.set_launch_time_secs (int launch-time-secs))
    (.set_status status)))

(deftest test-nimbus-iface-getClusterInfo-filters-topos-without-bases
  (let [cluster-state (Mockito/mock IStormClusterState)
        blob-store (Mockito/mock BlobStore)]
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder. )
                            (.withClusterState cluster-state)
                            (.withBlobStore blob-store)))]
    (let [nimbus (.getNimbus cluster)
          bogus-secs 42
          bogus-type TopologyStatus/ACTIVE
          bogus-bases {
                 "1" nil
                 "2" (mkStormBase bogus-secs "id2-name" bogus-type)
                 "3" nil
                 "4" (mkStormBase bogus-secs "id4-name" bogus-type)
                }
          topo-name "test-topo"
          topo-conf {TOPOLOGY-NAME topo-name
                     TOPOLOGY-WORKERS 1
                     TOPOLOGY-MESSAGE-TIMEOUT-SECS 30}
          storm-base (StormBase. )
          topology (doto (StormTopology. )
                     (.set_spouts {})
                     (.set_bolts {})
                     (.set_state_spouts {}))
        ]
      (.thenReturn (Mockito/when (.stormBase cluster-state (Mockito/any String) (Mockito/anyObject))) storm-base)
      (.thenReturn (Mockito/when (.topologyBases cluster-state)) bogus-bases)
      (.thenReturn (Mockito/when (.readTopologyConf blob-store (Mockito/any String) (Mockito/any Subject))) topo-conf)
      (.thenReturn (Mockito/when (.readTopology blob-store (Mockito/any String) (Mockito/any Subject))) topology)

      (let [topos (.get_topologies (.getClusterInfo nimbus))]
        ; The number of topologies in the summary is correct.
        (is (= (count
          (filter (fn [b] (second b)) bogus-bases)) (count topos)))
        ; Each topology present has a valid name.
        (is (empty?
          (filter (fn [t] (or (nil? t) (nil? (.get_name t)))) topos)))
        ; The topologies are those with valid bases.
        (is (empty?
          (filter (fn [t]
            (or
              (nil? t)
              (not (number? (read-string (.get_id t))))
              (odd? (read-string (.get_id t)))
            )) topos)))
      )
    )
  )
))

(deftest test-nimbus-data-acls
  (testing "nimbus-data uses correct ACLs"
    (let [scheme "digest"
          digest "storm:thisisapoorpassword"
          auth-conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                    {STORM-ZOOKEEPER-AUTH-SCHEME scheme
                     STORM-ZOOKEEPER-AUTH-PAYLOAD digest
                     STORM-PRINCIPAL-TO-LOCAL-PLUGIN "org.apache.storm.security.auth.DefaultPrincipalToLocal"
                     NIMBUS-THRIFT-PORT 6666})
          expected-acls Nimbus/ZK_ACLS
          fake-inimbus (reify INimbus (getForcedScheduler [this] nil) (prepare [this conf dir] nil))
          fake-cu (proxy [ServerConfigUtils] []
                    (nimbusTopoHistoryStateImpl [conf] nil))
          fake-ru (proxy [ReflectionUtils] []
                    (newInstanceImpl [_]))
          fake-utils (proxy [Utils] []
                       (makeUptimeComputer [] (proxy [Utils$UptimeComputer] []
                                                (upTime [] 0))))
          cluster-utils (Mockito/mock ClusterUtils)
	  fake-common (proxy [StormCommon] []
                             (mkAuthorizationHandler [_] nil))]
      (with-open [_ (ServerConfigUtilsInstaller. fake-cu)
                  _ (ReflectionUtilsInstaller. fake-ru)
                  _ (UtilsInstaller. fake-utils)
                  - (StormCommonInstaller. fake-common)
                  zk-le (MockedZookeeper. (proxy [Zookeeper] []
                          (zkLeaderElectorImpl [conf blob-store] nil)))
                  mocked-cluster (MockedCluster. cluster-utils)]
          (Nimbus. auth-conf fake-inimbus)
          (.mkStormClusterStateImpl (Mockito/verify cluster-utils (Mockito/times 1)) (Mockito/any) (Mockito/eq expected-acls) (Mockito/any))
          ))))

(deftest test-file-bogus-download
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder. )
                            (.withDaemonConf {SUPERVISOR-ENABLE false TOPOLOGY-ACKER-EXECUTORS 0 TOPOLOGY-EVENTLOGGER-EXECUTORS 0})))]
    (let [nimbus (.getNimbus cluster)]
      (is (thrown-cause? AuthorizationException (.beginFileDownload nimbus nil)))
      (is (thrown-cause? AuthorizationException (.beginFileDownload nimbus "")))
      (is (thrown-cause? AuthorizationException (.beginFileDownload nimbus "/bogus-path/foo")))
      )))

(deftest test-validate-topo-config-on-submit
  (let [cluster-state (Mockito/mock IStormClusterState)
        blob-store (Mockito/mock BlobStore)]
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder. )
                            (.withClusterState cluster-state)
                            (.withBlobStore blob-store)
                            (.withDaemonConf {NIMBUS-AUTHORIZER "org.apache.storm.security.auth.authorizer.NoopAuthorizer"})))]
      (.thenReturn (Mockito/when (.getTopoId cluster-state "test")) (Optional/empty))
      (let [topology (Thrift/buildTopology {} {})
            bad-config {"topology.isolate.machines" "2"}]
        (is (thrown-cause? InvalidTopologyException
          (.submitTopologyWithOpts cluster "test" bad-config topology
                                           (SubmitOptions.))))))))

(deftest test-stateless-with-scheduled-topology-to-be-killed
  ; tests regression of STORM-856
  (with-open [zk (InProcessZookeeper. )]
    (with-open [tmp-nimbus-dir (TmpPath. )]
      (let [nimbus-dir (.getPath tmp-nimbus-dir)]
      (letlocals
        (bind conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                     {STORM-ZOOKEEPER-SERVERS ["localhost"]
                      STORM-CLUSTER-MODE "local"
                      STORM-ZOOKEEPER-PORT (.getPort zk)
                      STORM-LOCAL-DIR nimbus-dir}))
        (bind cluster-state (ClusterUtils/mkStormClusterState conf nil (ClusterStateContext.)))
        (bind nimbus (mk-nimbus conf (Nimbus$StandaloneINimbus.) nil nil nil nil))
        (.launchServer nimbus)
        (Time/sleepSecs 1)
        (bind topology (Thrift/buildTopology
                         {"1" (Thrift/prepareSpoutDetails
                                (TestPlannerSpout. true) (Integer. 3))}
                         {}))
        (.submitTopology nimbus "t1" nil (str "{\"" TOPOLOGY-MESSAGE-TIMEOUT-SECS "\": 30}") topology)
        ; make transition for topology t1 to be killed -> nimbus applies this event to cluster state
        (.killTopology nimbus "t1")
        ; shutdown nimbus immediately to achieve nimbus doesn't handle event right now
        (.shutdown nimbus)

        ; in startup of nimbus it reads cluster state and take proper actions
        ; in this case nimbus registers topology transition event to scheduler again
        ; before applying STORM-856 nimbus was killed with NPE
        (bind nimbus (mk-nimbus conf (Nimbus$StandaloneINimbus.) nil nil nil nil))
        (.launchServer nimbus)
        (.shutdown nimbus)
        (.disconnect cluster-state)
        )))))

(deftest test-topology-action-notifier
  (with-open [zk (InProcessZookeeper. )]
    (with-open [tmp-nimbus-dir (TmpPath.)
                _ (MockedZookeeper. (proxy [Zookeeper] []
                    (zkLeaderElectorImpl [conf blob-store] (MockLeaderElector. ))))]
      (let [nimbus-dir (.getPath tmp-nimbus-dir)]
        (letlocals
          (bind conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                       {STORM-ZOOKEEPER-SERVERS ["localhost"]
                        STORM-CLUSTER-MODE "local"
                        STORM-ZOOKEEPER-PORT (.getPort zk)
                        STORM-LOCAL-DIR nimbus-dir
                        NIMBUS-TOPOLOGY-ACTION-NOTIFIER-PLUGIN (.getName InMemoryTopologyActionNotifier)}))
          (bind cluster-state (ClusterUtils/mkStormClusterState conf nil (ClusterStateContext.)))
          (bind nimbus (mk-nimbus conf (Nimbus$StandaloneINimbus.) nil nil nil nil))
          (.launchServer nimbus)
          (bind notifier (InMemoryTopologyActionNotifier.))
          (Time/sleepSecs 1)
          (bind topology (Thrift/buildTopology
                           {"1" (Thrift/prepareSpoutDetails
                                  (TestPlannerSpout. true) (Integer. 3))}
                           {}))
          (.submitTopology nimbus "test-notification" nil (str "{\"" TOPOLOGY-MESSAGE-TIMEOUT-SECS "\": 30}") topology)

          (.deactivate nimbus "test-notification")

          (.activate nimbus "test-notification")

          (.rebalance nimbus "test-notification" (doto (RebalanceOptions.)
                                                   (.set_wait_secs 0)))

          (.killTopologyWithOpts nimbus "test-notification" (doto (KillOptions.)
                                                      (.set_wait_secs 0)))

          (.shutdown nimbus)

          ; ensure notifier was invoked for each action,and in the correct order.
          (is (= ["submitTopology", "activate", "deactivate", "activate", "rebalance", "killTopology"]
                (.getTopologyActions notifier "test-notification")))
          (.disconnect cluster-state)
          )))))

(deftest test-debug-on-component
  (with-open [cluster (LocalCluster. )]
    (let [nimbus (.getNimbus cluster)
          topology (Thrift/buildTopology
                     {"spout" (Thrift/prepareSpoutDetails
                                (TestPlannerSpout. true) (Integer. 3))}
                     {})]
        (.submitTopology cluster "t1" {TOPOLOGY-WORKERS 1} topology)
        (.debug nimbus "t1" "spout" true 100))))

(deftest test-debug-on-global
  (with-open [cluster (LocalCluster. )]
    (let [nimbus (.getNimbus cluster)
          topology (Thrift/buildTopology
                     {"spout" (Thrift/prepareSpoutDetails
                                (TestPlannerSpout. true) (Integer. 3))}
                     {})]
      (.submitTopology cluster "t1" {TOPOLOGY-WORKERS 1} topology)
      (.debug nimbus "t1" "" true 100))))

;; if the user sends an empty log config, nimbus will say that all 
;; log configs it contains are LogLevelAction/UNCHANGED
(deftest empty-save-config-results-in-all-unchanged-actions
  (let [cluster-state (Mockito/mock IStormClusterState)
        blob-store (Mockito/mock BlobStore)]
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder. )
                            (.withClusterState cluster-state)
                            (.withBlobStore blob-store)
                            (.withDaemonConf {NIMBUS-AUTHORIZER "org.apache.storm.security.auth.authorizer.NoopAuthorizer"})))]
      (let [nimbus (.getNimbus cluster)
            previous-config (LogConfig.)
            mock-config (LogConfig.)
            expected-config (LogConfig.)]
        ;; send something with content to nimbus beforehand
        (.put_to_named_logger_level previous-config "test"
          (doto (LogLevel.)
            (.set_target_log_level "ERROR")
            (.set_action LogLevelAction/UPDATE)))

        (.put_to_named_logger_level expected-config "test"
          (doto (LogLevel.)
            (.set_target_log_level "ERROR")
            (.set_action LogLevelAction/UNCHANGED)))

        (.thenReturn (Mockito/when (.readTopologyConf blob-store (Mockito/any String) (Mockito/anyObject))) {})
        (.thenReturn (Mockito/when (.topologyLogConfig cluster-state (Mockito/any String) (Mockito/anyObject))) previous-config)

        (.setLogConfig nimbus "foo" mock-config)
        (.setTopologyLogConfig (Mockito/verify cluster-state) (Mockito/any String) (Mockito/eq expected-config))))))

(deftest log-level-update-merges-and-flags-existent-log-level
  (let [cluster-state (Mockito/mock IStormClusterState)
        blob-store (Mockito/mock BlobStore)]
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder. )
                            (.withClusterState cluster-state)
                            (.withBlobStore blob-store)
                            (.withDaemonConf {NIMBUS-AUTHORIZER "org.apache.storm.security.auth.authorizer.NoopAuthorizer"})))]
      (let [nimbus (.getNimbus cluster)
            previous-config (LogConfig.)
            mock-config (LogConfig.)
            expected-config (LogConfig.)]
        ;; send something with content to nimbus beforehand
        (.put_to_named_logger_level previous-config "test"
          (doto (LogLevel.)
            (.set_target_log_level "ERROR")
            (.set_action LogLevelAction/UPDATE)))

        (.put_to_named_logger_level previous-config "other-test"
          (doto (LogLevel.)
            (.set_target_log_level "DEBUG")
            (.set_action LogLevelAction/UPDATE)))


        ;; only change "test"
        (.put_to_named_logger_level mock-config "test"
          (doto (LogLevel.)
            (.set_target_log_level "INFO")
            (.set_action LogLevelAction/UPDATE)))

        (.put_to_named_logger_level expected-config "test"
          (doto (LogLevel.)
            (.set_target_log_level "INFO")
            (.set_action LogLevelAction/UPDATE)))

        (.put_to_named_logger_level expected-config "other-test"
          (doto (LogLevel.)
            (.set_target_log_level "DEBUG")
            (.set_action LogLevelAction/UNCHANGED)))

        (.thenReturn (Mockito/when (.readTopologyConf blob-store (Mockito/any String) (Mockito/anyObject))) {})
        (.thenReturn (Mockito/when (.topologyLogConfig cluster-state (Mockito/any String) (Mockito/anyObject))) previous-config)

        (.setLogConfig nimbus "foo" mock-config)
        (.setTopologyLogConfig (Mockito/verify cluster-state) (Mockito/any String) (Mockito/eq expected-config))))))

(defn teardown-heartbeats [id])
(defn teardown-topo-errors [id])
(defn teardown-backpressure-dirs [id])

(defn mock-cluster-state
  ([]
    (mock-cluster-state nil nil))
  ([active-topos inactive-topos]
    (mock-cluster-state active-topos inactive-topos inactive-topos inactive-topos))
  ([active-topos hb-topos error-topos bp-topos]
    (reify IStormClusterState
      (teardownHeartbeats [this id] (teardown-heartbeats id))
      (teardownTopologyErrors [this id] (teardown-topo-errors id))
      (removeBackpressure [this id] (teardown-backpressure-dirs id))
      (activeStorms [this] active-topos)
      (heartbeatStorms [this] hb-topos)
      (errorTopologies [this] error-topos)
      (backpressureTopologies [this] bp-topos))))

(deftest cleanup-storm-ids-returns-inactive-topos
         (let [mock-state (mock-cluster-state (list "topo1") (list "topo1" "topo2" "topo3"))
               store (Mockito/mock BlobStore)]
              (.thenReturn (Mockito/when (.storedTopoIds store)) #{})
              (is (= (Nimbus/topoIdsToClean mock-state store) #{"topo2" "topo3"}))))

(deftest cleanup-storm-ids-performs-union-of-storm-ids-with-active-znodes
  (let [active-topos (list "hb1" "e2" "bp3")
        hb-topos (list "hb1" "hb2" "hb3")
        error-topos (list "e1" "e2" "e3")
        bp-topos (list "bp1" "bp2" "bp3")
        mock-state (mock-cluster-state active-topos hb-topos error-topos bp-topos)
        store (Mockito/mock BlobStore)]
    (.thenReturn (Mockito/when (.storedTopoIds store)) #{})
    (is (= (Nimbus/topoIdsToClean mock-state store)
           #{"hb2" "hb3" "e1" "e3" "bp1" "bp2"}))))

(deftest cleanup-storm-ids-returns-empty-set-when-all-topos-are-active
  (let [active-topos (list "hb1" "hb2" "hb3" "e1" "e2" "e3" "bp1" "bp2" "bp3")
        hb-topos (list "hb1" "hb2" "hb3")
        error-topos (list "e1" "e2" "e3")
        bp-topos (list "bp1" "bp2" "bp3")
        mock-state (mock-cluster-state active-topos hb-topos error-topos bp-topos)
        store (Mockito/mock BlobStore)]
    (.thenReturn (Mockito/when (.storedTopoIds store)) #{})
    (is (= (Nimbus/topoIdsToClean mock-state store)
           #{}))))

(deftest do-cleanup-removes-inactive-znodes
  (let [inactive-topos (list "topo2" "topo3")
        hb-cache (into {}(map vector inactive-topos '(nil nil)))
        mock-state (mock-cluster-state)
        mock-blob-store (Mockito/mock BlobStore)
        conf {}]
    (with-open [_ (MockedZookeeper. (proxy [Zookeeper] []
                    (zkLeaderElectorImpl [conf blob-store] (MockLeaderElector. ))))]
      (let [nimbus (Mockito/spy (Nimbus. conf nil mock-state nil mock-blob-store nil nil))]
        (.set (.getHeartbeatsCache nimbus) hb-cache)
        (.thenReturn (Mockito/when (.storedTopoIds mock-blob-store)) (HashSet. inactive-topos))
        (mocking
          [teardown-heartbeats
           teardown-topo-errors
           teardown-backpressure-dirs]

          (.doCleanup nimbus)

          ;; removed heartbeats znode
          (verify-nth-call-args-for 1 teardown-heartbeats "topo2")
          (verify-nth-call-args-for 2 teardown-heartbeats "topo3")

          ;; removed topo errors znode
          (verify-nth-call-args-for 1 teardown-topo-errors "topo2")
          (verify-nth-call-args-for 2 teardown-topo-errors "topo3")

          ;; removed backpressure znodes
          (verify-nth-call-args-for 1 teardown-backpressure-dirs "topo2")
          (verify-nth-call-args-for 2 teardown-backpressure-dirs "topo3")

          ;; removed topo directories
          (.forceDeleteTopoDistDir (Mockito/verify nimbus) "topo2")
          (.forceDeleteTopoDistDir (Mockito/verify nimbus) "topo3")

          ;; removed blob store topo keys
          (.rmTopologyKeys (Mockito/verify nimbus) "topo2")
          (.rmTopologyKeys (Mockito/verify nimbus) "topo3")

          ;; removed topology dependencies
          (.rmDependencyJarsInTopology (Mockito/verify nimbus) "topo2")
          (.rmDependencyJarsInTopology (Mockito/verify nimbus) "topo3")

          ;; remove topos from heartbeat cache
          (is (= (count (.get (.getHeartbeatsCache nimbus))) 0)))))))

(deftest do-cleanup-does-not-teardown-active-topos
  (let [inactive-topos ()
        hb-cache {"topo1" nil "topo2" nil}
        mock-state (mock-cluster-state)
        mock-blob-store (Mockito/mock BlobStore)
        conf {}]
    (with-open [_ (MockedZookeeper. (proxy [Zookeeper] []
                    (zkLeaderElectorImpl [conf blob-store] (MockLeaderElector. ))))]
      (let [nimbus (Mockito/spy (Nimbus. conf nil mock-state nil mock-blob-store nil nil))]
        (.set (.getHeartbeatsCache nimbus) hb-cache)
        (.thenReturn (Mockito/when (.storedTopoIds mock-blob-store)) (set inactive-topos))
        (mocking
          [teardown-heartbeats
           teardown-topo-errors
           teardown-backpressure-dirs]

          (.doCleanup nimbus)

          (verify-call-times-for teardown-heartbeats 0)
          (verify-call-times-for teardown-topo-errors 0)
          (verify-call-times-for teardown-backpressure-dirs 0)
          (.forceDeleteTopoDistDir (Mockito/verify nimbus (Mockito/times 0)) (Mockito/anyObject))
          (.rmTopologyKeys (Mockito/verify nimbus (Mockito/times 0)) (Mockito/anyObject))

          ;; hb-cache goes down to 1 because only one topo was inactive
          (is (= (count (.get (.getHeartbeatsCache nimbus))) 2))
          (is (contains? (.get (.getHeartbeatsCache nimbus)) "topo1"))
          (is (contains? (.get (.getHeartbeatsCache nimbus)) "topo2")))))))

(deftest user-topologies-for-supervisor
  (let [assignment (doto (Assignment.)
                     (.set_executor_node_port {[1 1] (NodeInfo. "super1" #{1}),
                                               [2 2] (NodeInfo. "super2" #{2})}))
        assignment2 (doto (Assignment.)
                     (.set_executor_node_port {[1 1] (NodeInfo. "super2" #{2}),
                                               [2 2] (NodeInfo. "super2" #{2})}))
        assignments {"topo1" assignment, "topo2" assignment2}
        mock-state (mock-cluster-state)
        mock-blob-store (Mockito/mock BlobStore)
        nimbus (Nimbus. {} nil mock-state nil mock-blob-store (MockLeaderElector. ) nil)]
    (let [supervisor1-topologies (clojurify-structure (Nimbus/topologiesOnSupervisor assignments "super1"))
          user1-topologies (clojurify-structure (.filterAuthorized nimbus "getTopology" supervisor1-topologies))
          supervisor2-topologies (clojurify-structure (Nimbus/topologiesOnSupervisor assignments "super2"))
          user2-topologies (clojurify-structure (.filterAuthorized nimbus "getTopology" supervisor2-topologies))]
      (is (= (list "topo1") supervisor1-topologies))
      (is (= #{"topo1"} user1-topologies))
      (is (= (list "topo1" "topo2") supervisor2-topologies))
      (is (= #{"topo1" "topo2"} user2-topologies)))))

(deftest user-topologies-for-supervisor-with-unauthorized-user
  (let [assignment (doto (Assignment.)
                     (.set_executor_node_port {[1 1] (NodeInfo. "super1" #{1}),
                                               [2 2] (NodeInfo. "super2" #{2})}))
        assignment2 (doto (Assignment.)
                     (.set_executor_node_port {[1 1] (NodeInfo. "super1" #{2}),
                                               [2 2] (NodeInfo. "super2" #{2})}))
        assignments {"topo1" assignment, "authorized" assignment2}
        mock-state (mock-cluster-state)
        mock-blob-store (Mockito/mock BlobStore)
        nimbus (Nimbus. {} nil mock-state nil mock-blob-store (MockLeaderElector. ) nil)]
    (.thenReturn (Mockito/when (.readTopologyConf mock-blob-store (Mockito/eq "authorized") (Mockito/anyObject))) {TOPOLOGY-NAME "authorized"})
    (.thenReturn (Mockito/when (.readTopologyConf mock-blob-store (Mockito/eq "topo1") (Mockito/anyObject))) {TOPOLOGY-NAME "topo1"})
    (.setAuthorizationHandler nimbus (reify IAuthorizer (permit [this context operation topo-conf] (= "authorized" (get topo-conf TOPOLOGY-NAME)))))
    (let [supervisor-topologies (clojurify-structure (Nimbus/topologiesOnSupervisor assignments "super1"))
          user-topologies (clojurify-structure (.filterAuthorized nimbus "getTopology" supervisor-topologies))]

      (is (= (list "topo1" "authorized") supervisor-topologies))
      (is (= #{"authorized"} user-topologies)))))

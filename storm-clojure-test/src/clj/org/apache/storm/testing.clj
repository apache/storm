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

(ns org.apache.storm.testing
  (:import [org.apache.storm LocalCluster$Builder])
  (:import [java.util.function UnaryOperator])
  (:import [org.apache.storm.utils Time Time$SimulatedTime RegisteredGlobalState Utils])
  (:import [org.apache.storm.testing InProcessZookeeper MkTupleParam TestJob MkClusterParam 
            TrackedTopology CompleteTopologyParam MockedSources])
  (:import [org.apache.storm Thrift Testing Testing$Condition])
  (:import [org.apache.storm.testing MockLeaderElector])
  (:import [org.json.simple JSONValue])
  (:use [org.apache.storm util config log])
  (:use [org.apache.storm thrift]))

(defnk add-supervisor
  [cluster-map :ports 2 :conf {} :id nil]
  (let [local-cluster (:local-cluster cluster-map)]
    (.addSupervisor local-cluster ports conf id)))

(defnk mock-leader-elector [:is-leader true :leader-name "test-host" :leader-port 9999]
  (MockLeaderElector. is-leader leader-name leader-port))

(defn local-cluster-state [local-cluster]
    {:nimbus (.getNimbus local-cluster)
     :daemon-conf (.getDaemonConf local-cluster)
     :storm-cluster-state (.getClusterState local-cluster)
     :local-cluster local-cluster})

(defnk mk-mocked-nimbus 
  [:daemon-conf {} :inimbus nil :blob-store nil :cluster-state nil 
   :leader-elector nil :group-mapper nil :nimbus-daemon false :nimbus-wrapper nil]
  (let [builder (doto (LocalCluster$Builder.)
                  (.withDaemonConf daemon-conf)
                  (.withINimbus inimbus)
                  (.withBlobStore blob-store)
                  (.withClusterState cluster-state)
                  (.withLeaderElector leader-elector)
                  (.withGroupMapper group-mapper)
                  (.withNimbusDaemon nimbus-daemon)
                  (.withNimbusWrapper (when nimbus-wrapper (reify UnaryOperator (apply [this nimbus] (nimbus-wrapper nimbus))))))
        local-cluster (.build builder)]
    (local-cluster-state local-cluster)))

(defnk mk-local-storm-cluster [:supervisors 2 :ports-per-supervisor 3 :daemon-conf {} :inimbus nil :group-mapper nil :supervisor-slot-port-min 1024 :nimbus-daemon false]
  (let [builder (doto (LocalCluster$Builder.)
                  (.withSupervisors supervisors)
                  (.withPortsPerSupervisor ports-per-supervisor)
                  (.withDaemonConf daemon-conf)
                  (.withINimbus inimbus)
                  (.withGroupMapper group-mapper)
                  (.withSupervisorSlotPortMin supervisor-slot-port-min)
                  (.withNimbusDaemon nimbus-daemon))
        local-cluster (.build builder)]
    (local-cluster-state local-cluster)))

(defn get-supervisor [cluster-map supervisor-id]
 (let [local-cluster (:local-cluster cluster-map)]
  (.getSupervisor local-cluster supervisor-id))) 

(defn kill-supervisor [cluster-map supervisor-id]
 (let [local-cluster (:local-cluster cluster-map)]
  (.killSupervisor local-cluster supervisor-id))) 

(defn kill-local-storm-cluster [cluster-map]
 (let [local-cluster (:local-cluster cluster-map)]
  (.close local-cluster))) 

(defmacro while-timeout [timeout-ms condition & body]
  `(Testing/whileTimeout ~timeout-ms
     (reify Testing$Condition (exec [this] ~condition))
     (fn [] ~@body)))

(defn wait-for-condition
  ([apredicate]
    (wait-for-condition Testing/TEST_TIMEOUT_MS apredicate))
  ([timeout-ms apredicate]
    (while-timeout timeout-ms (not (apredicate))
      (Time/sleep 100))))

(defn wait-until-cluster-waiting
  "Wait until the cluster is idle. Should be used with time simulation."
  ([cluster-map]
    (let [local-cluster (:local-cluster cluster-map)]
      (.waitForIdle local-cluster)))
  ([cluster-map timeout-ms]
    (let [local-cluster (:local-cluster cluster-map)]
      (.waitForIdle local-cluster timeout-ms))))

(defn advance-cluster-time
  ([cluster-map secs increment-secs]
    (let [local-cluster (:local-cluster cluster-map)]
      (.advanceClusterTime local-cluster secs increment-secs))) 
  ([cluster-map secs]
    (let [local-cluster (:local-cluster cluster-map)]
      (.advanceClusterTime local-cluster secs))))

(defmacro with-mocked-nimbus
  [[nimbus-sym & args] & body]
  `(let [~nimbus-sym (mk-mocked-nimbus ~@args)]
     (try
       ~@body
       (catch Throwable t#
         (log-error t# "Error in cluster")
         (throw t#))
       (finally
         (let [keep-waiting?# (atom true)
               f# (future (while @keep-waiting?# (simulate-wait ~nimbus-sym)))]
           (kill-local-storm-cluster ~nimbus-sym)
           (reset! keep-waiting?# false)
            @f#)))))

(defmacro with-local-cluster
  [[cluster-sym & args] & body]
  `(let [~cluster-sym (mk-local-storm-cluster ~@args)]
     (try
       ~@body
       (catch Throwable t#
         (log-error t# "Error in cluster")
         (throw t#))
       (finally
         (let [keep-waiting?# (atom true)
               f# (future (while @keep-waiting?# (simulate-wait ~cluster-sym)))]
           (kill-local-storm-cluster ~cluster-sym)
           (reset! keep-waiting?# false)
            @f#)))))

(defmacro with-simulated-time-local-cluster
  [& args]
  `(with-open [_# (Time$SimulatedTime.)]
     (with-local-cluster ~@args)))

(defmacro with-inprocess-zookeeper
  [port-sym & body]
  `(with-open [zks# (InProcessZookeeper. )]
     (let [~port-sym (.getPort zks#)]
       ~@body)))

(defn submit-local-topology
  [nimbus storm-name conf topology]
  (when-not (Utils/isValidConf conf)
    (throw (IllegalArgumentException. "Topology conf is not json-serializable")))
  (.submitTopology nimbus storm-name nil (JSONValue/toJSONString conf) topology))

(defn submit-local-topology-with-opts
  [nimbus storm-name conf topology submit-opts]
  (when-not (Utils/isValidConf conf)
    (throw (IllegalArgumentException. "Topology conf is not json-serializable")))
  (.submitTopologyWithOpts nimbus storm-name nil (JSONValue/toJSONString conf) topology submit-opts))

(defn simulate-wait
  [cluster-map]
  (Testing/simulateWait (:local-cluster cluster-map)))

(defn spout-objects [spec-map]
  (for [[_ spout-spec] spec-map]
    (-> spout-spec
        .get_spout_object
        (Thrift/deserializeComponentObject))))

(defn capture-topology
  [topology]
  (let [cap-topo (Testing/captureTopology topology)]
    {:topology (.topology cap-topo)
     :capturer (.capturer cap-topo)}))

(defnk complete-topology
  [cluster-map topology
   :mock-sources {}
   :storm-conf {}
   :cleanup-state true
   :topology-name nil
   :timeout-ms Testing/TEST_TIMEOUT_MS]
  (Testing/completeTopology (:local-cluster cluster-map) topology, 
      (doto (CompleteTopologyParam.)
            (.setStormConf storm-conf)
            (.setTopologyName topology-name)
            (.setTimeoutMs timeout-ms)
            (.setMockedSources (MockedSources. mock-sources))
            (.setCleanupState cleanup-state))))

(defn read-tuples
  ([results component-id stream-id]
   (Testing/readTuples results component-id stream-id))
  ([results component-id]
   (Testing/readTuples results component-id )))

(defn ms=
  [a b]
  (Testing/multiseteq a b))

(def TRACKER-BOLT-ID "+++tracker-bolt")

;; TODO: should override system-topology! and wrap everything there
(defn mk-tracked-topology
  ([tracked-cluster topology]
   (let [tt (TrackedTopology. topology (:local-cluster tracked-cluster))]
     {:topology (.getTopology tt)
      :tracked-topo tt})))

(defn increment-global!
  [id key amt]
  (-> (RegisteredGlobalState/getState id)
      (get key)
      (.addAndGet amt)))

(defn global-amt
  [id key]
  (-> (RegisteredGlobalState/getState id)
      (get key)
      .get))

(defnk mkClusterParam 
  [:supervisors 2 :ports-per-supervisor 3 :daemon-conf {} :nimbus-daemon false]
  ;;TODO do we need to support inimbus?, group-mapper, or supervisor-slot-port-min
  (doto (MkClusterParam. )
    (.setDaemonConf daemon-conf)
    (.setNimbusDaemon nimbus-daemon)
    (.setPortsPerSupervisor (int ports-per-supervisor))
    (.setSupervisors (int supervisors))))

(defmacro with-tracked-cluster
  [[cluster-sym & cluster-args] & body]
  `(Testing/withTrackedCluster
       (mkClusterParam ~@cluster-args)
       (reify TestJob
         (run [this# lc#]
           (let [~cluster-sym (local-cluster-state lc#)]
             ~@body)))))

(defn tracked-wait
  "Waits until topology is idle and 'amt' more tuples have been emitted by spouts."
  ([tracked-map]
     (Testing/trackedWait (:tracked-topo tracked-map)))
  ([tracked-map amt]
     (Testing/trackedWait (:tracked-topo tracked-map) (int amt)))
  ([tracked-map amt timeout-ms]
     (Testing/trackedWait (:tracked-topo tracked-map) (int amt) (int timeout-ms))))

(defnk test-tuple
  [values
   :stream Utils/DEFAULT_STREAM_ID
   :component "component"
   :fields nil]
  (Testing/testTuple
    values
    (doto (MkTupleParam. )
      (.setStream stream)
      (.setComponent component)
      (.setFieldsList fields))))

(defmacro with-timeout
  [millis unit & body]
  `(let [f# (future ~@body)]
     (try
       (.get f# ~millis ~unit)
       (finally (future-cancel f#)))))

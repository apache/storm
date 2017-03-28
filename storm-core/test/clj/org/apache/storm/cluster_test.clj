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
(ns org.apache.storm.cluster-test
  (:import [java.util Arrays]
           [org.apache.storm.nimbus NimbusInfo])
  (:import [org.apache.storm.generated SupervisorInfo StormBase Assignment NimbusSummary TopologyStatus NodeInfo Credentials])
  (:import [org.apache.zookeeper ZooDefs ZooDefs$Ids Watcher$Event$EventType])
  (:import [org.mockito Mockito])
  (:import [org.mockito.exceptions.base MockitoAssertionError])
  (:import [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory CuratorFrameworkFactory$Builder])
  (:import [org.apache.storm.utils Time Time$SimulatedTime ZookeeperAuthInfo ConfigUtils Utils CuratorUtils])
  (:import [org.apache.storm.cluster IStateStorage ZKStateStorage ClusterStateContext StormClusterStateImpl ClusterUtils])
  (:import [org.apache.storm.zookeeper Zookeeper ClientZookeeper])
  (:import [org.apache.storm.callback ZKStateChangedCallback])
  (:import [org.apache.storm.testing InProcessZookeeper])
  (:import [org.apache.storm.testing.staticmocking MockedZookeeper MockedCluster MockedClientZookeeper])
  (:require [conjure.core])
  (:use [conjure core])
  (:use [clojure test])
  (:use [org.apache.storm config util log])
  (:use [org.apache.storm.internal thrift]))

(defn mk-config [zk-port]
  (merge (clojurify-structure (ConfigUtils/readStormConfig))
    {STORM-ZOOKEEPER-PORT zk-port
     STORM-ZOOKEEPER-SERVERS ["localhost"]}))

(defn mk-state
  ([zk-port] (let [conf (mk-config zk-port)]
               (ClusterUtils/mkStateStorage conf conf nil (ClusterStateContext.))))
  ([zk-port cb]
    (let [ret (mk-state zk-port)]
      (.register ret cb)
      ret)))

(defn mk-storm-state [zk-port] (ClusterUtils/mkStormClusterState (mk-config zk-port) nil (ClusterStateContext.)))

(defn barr
  [& vals]
  (byte-array (map byte vals)))

(deftest test-basics
  (with-open [zk (InProcessZookeeper. )]
    (let [state (mk-state (.getPort zk))]
      (.set-data state "/root" (barr 1 2 3) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (Arrays/equals (barr 1 2 3) (.get-data state "/root" false)))
      (is (= nil (.get-data state "/a" false)))
      (.set-data state "/root/a" (barr 1 2) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (.set-data state "/root" (barr 1) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (Arrays/equals (barr 1) (.get-data state "/root" false)))
      (is (Arrays/equals (barr 1 2) (.get-data state "/root/a" false)))
      (.set-data state "/a/b/c/d" (barr 99) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (Arrays/equals (barr 99) (.get-data state "/a/b/c/d" false)))
      (.mkdirs state "/lalala" ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= [] (.get-children state "/lalala" false)))
      (is (= #{"root" "a" "lalala"} (set (.get-children state "/" false))))
      (.delete-node state "/a")
      (is (= #{"root" "lalala"} (set (.get-children state "/" false))))
      (is (= nil (.get-data state "/a/b/c/d" false)))
      (.close state)
      )))

(deftest test-multi-state
  (with-open [zk (InProcessZookeeper. )]
    (let [state1 (mk-state (.getPort zk))
          state2 (mk-state (.getPort zk))]
      (.set-data state1 "/root" (barr 1) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (Arrays/equals (barr 1) (.get-data state1 "/root" false)))
      (is (Arrays/equals (barr 1) (.get-data state2 "/root" false)))
      (.delete-node state2 "/root")
      (is (= nil (.get-data state1 "/root" false)))
      (is (= nil (.get-data state2 "/root" false)))
      (.close state1)
      (.close state2)
      )))

(deftest test-ephemeral
  (with-open [zk (InProcessZookeeper. )]
    (let [state1 (mk-state (.getPort zk))
          state2 (mk-state (.getPort zk))
          state3 (mk-state (.getPort zk))]
      (.set-ephemeral-node state1 "/a" (barr 1) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (Arrays/equals (barr 1) (.get-data state1 "/a" false)))
      (is (Arrays/equals (barr 1) (.get-data state2 "/a" false)))
      (.close state3)
      (is (Arrays/equals (barr 1) (.get-data state1 "/a" false)))
      (is (Arrays/equals (barr 1) (.get-data state2 "/a" false)))
      (.close state1)
      (is (= nil (.get-data state2 "/a" false)))
      (.close state2)
      )))

(defn mk-callback-tester []
  (let [last (atom nil)
        cb (reify
             ZKStateChangedCallback
             (changed
               [this type path]
               (reset! last {:type type :path path})))]
    [last cb]
    ))

(defn read-and-reset! [aatom]
  (let [time (System/currentTimeMillis)]
    (loop []
      (if-let [val @aatom]
        (do
          (reset! aatom nil)
          val)
        (do
          (when (> (- (System/currentTimeMillis) time) 30000)
            (throw (RuntimeException. "Waited too long for atom to change state")))
          (Thread/sleep 10)
          (recur))
        ))))

(deftest test-callbacks
  (with-open [zk (InProcessZookeeper. )]
    (let [[state1-last-cb state1-cb] (mk-callback-tester)
          state1 (mk-state (.getPort zk) state1-cb)
          [state2-last-cb state2-cb] (mk-callback-tester)
          state2 (mk-state (.getPort zk) state2-cb)]
      (.set-data state1 "/root" (barr 1) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (.get-data state2 "/root" true)
      (is (= nil @state1-last-cb))
      (is (= nil @state2-last-cb))
      (.set-data state2 "/root" (barr 2) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= {:type Watcher$Event$EventType/NodeDataChanged :path "/root"} (read-and-reset! state2-last-cb)))
      (is (= nil @state1-last-cb))

      (.set-data state2 "/root" (barr 3) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= nil @state2-last-cb))
      (.get-data state2 "/root" true)
      (.get-data state2 "/root" false)
      (.delete-node state1 "/root")
      (is (= {:type Watcher$Event$EventType/NodeDeleted :path "/root"} (read-and-reset! state2-last-cb)))
      (.get-data state2 "/root" true)
      (.set-ephemeral-node state1 "/root" (barr 1 2 3 4) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= {:type Watcher$Event$EventType/NodeCreated :path "/root"} (read-and-reset! state2-last-cb)))

      (.get-children state1 "/" true)
      (.set-data state2 "/a" (barr 9) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= nil @state2-last-cb))
      (is (= {:type Watcher$Event$EventType/NodeChildrenChanged :path "/"} (read-and-reset! state1-last-cb)))

      (.get-data state2 "/root" true)
      (.set-ephemeral-node state1 "/root" (barr 1 2) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= {:type Watcher$Event$EventType/NodeDataChanged :path "/root"} (read-and-reset! state2-last-cb)))

      (.mkdirs state1 "/ccc" ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (.get-children state1 "/ccc" true)
      (.get-data state2 "/ccc/b" true)
      (.set-data state2 "/ccc/b" (barr 8) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= {:type Watcher$Event$EventType/NodeCreated :path "/ccc/b"} (read-and-reset! state2-last-cb)))
      (is (= {:type Watcher$Event$EventType/NodeChildrenChanged :path "/ccc"} (read-and-reset! state1-last-cb)))

      (.get-data state2 "/root" true)
      (.get-data state2 "/root2" true)
      (.close state1)

      (is (= {:type Watcher$Event$EventType/NodeDeleted :path "/root"} (read-and-reset! state2-last-cb)))
      (.set-data state2 "/root2" (barr 9) ZooDefs$Ids/OPEN_ACL_UNSAFE)
      (is (= {:type Watcher$Event$EventType/NodeCreated :path "/root2"} (read-and-reset! state2-last-cb)))
      (.close state2)
      )))


(defn mkAssignment [master-code-dir node->host executor->node+port executor->start-time-secs worker->resources]
  (doto (Assignment.)
    (.set_executor_node_port executor->node+port)
    (.set_executor_start_time_secs executor->start-time-secs)
    (.set_worker_resources worker->resources)
    (.set_node_host node->host)
    (.set_master_code_dir master-code-dir)))

(defn mkStormBase [storm-name launch-time-secs status num-workers]
  (doto (StormBase.)
    (.set_name storm-name)
    (.set_launch_time_secs (int launch-time-secs))
    (.set_status status)
    (.set_num_workers (int num-workers))))

(deftest test-storm-cluster-state-basics
  (with-open [zk (InProcessZookeeper. )]
    (let [state (mk-storm-state (.getPort zk))
          assignment1 (mkAssignment "/aaa" {} {[1] (NodeInfo. "1" #{1001 1})} {} {})
          assignment2 (mkAssignment "/aaa" {} {[2] (NodeInfo. "2" #{2002})} {} {})
          nimbusInfo1 (NimbusInfo. "nimbus1" 6667 false)
          nimbusInfo2 (NimbusInfo. "nimbus2" 6667 false)
          nimbusSummary1 (NimbusSummary. "nimbus1" 6667 (Time/currentTimeSecs) false "v1")
          nimbusSummary2 (NimbusSummary. "nimbus2" 6667 (Time/currentTimeSecs) false "v2")
          base1 (mkStormBase "/tmp/storm1" 1 TopologyStatus/ACTIVE 2)
          base2 (mkStormBase "/tmp/storm2" 2 TopologyStatus/ACTIVE 2)]
      (is (= [] (.assignments state nil)))
      (.setAssignment state "storm1" assignment1)
      (is (= assignment1 (.assignmentInfo state "storm1" nil)))
      (is (= nil (.assignmentInfo state "storm3" nil)))
      (.setAssignment state "storm1" assignment2)
      (.setAssignment state "storm3" assignment1)
      (is (= #{"storm1" "storm3"} (set (.assignments state nil))))
      (is (= assignment2 (.assignmentInfo state "storm1" nil)))
      (is (= assignment1 (.assignmentInfo state "storm3" nil)))

      (is (= [] (.activeStorms state)))
      (.activateStorm state "storm1" base1)
      (is (= ["storm1"] (.activeStorms state)))
      (is (= base1 (.stormBase state "storm1" nil)))
      (is (= nil (.stormBase state "storm2" nil)))
      (.activateStorm state "storm2" base2)
      (is (= base1 (.stormBase state "storm1" nil)))
      (is (= base2 (.stormBase state "storm2" nil)))
      (is (= #{"storm1" "storm2"} (set (.activeStorms state))))
      (.removeStormBase state "storm1")
      (is (= base2 (.stormBase state "storm2" nil)))
      (is (= #{"storm2"} (set (.activeStorms state))))

      (is (nil? (.credentials state "storm1" nil)))
      (.setCredentials state "storm1" (doto (Credentials. ) (.set_creds {"a" "a"})) {})
      (is (= {"a" "a"} (.get_creds (.credentials state "storm1" nil))))
      (.setCredentials state "storm1" (doto (Credentials. ) (.set_creds {"b" "b"})) {})
      (is (= {"b" "b"} (.get_creds (.credentials state "storm1" nil))))

      (is (= [] (.blobstoreInfo state "")))
      (.setupBlobstore state "key1" nimbusInfo1 (Integer/parseInt "1"))
      (is (= ["key1"] (.blobstoreInfo state "")))
      (is (= [(str (.toHostPortString nimbusInfo1) "-1")] (.blobstoreInfo state "key1")))
      (.setupBlobstore state "key1" nimbusInfo2 (Integer/parseInt "1"))
      (is (= #{(str (.toHostPortString nimbusInfo1) "-1")
               (str (.toHostPortString nimbusInfo2) "-1")} (set (.blobstoreInfo state "key1"))))
      (.removeBlobstoreKey state "key1")
      (is (= [] (.blobstoreInfo state "")))

      (is (= [] (.nimbuses state)))
      (.addNimbusHost state "nimbus1:port" nimbusSummary1)
      (is (= [nimbusSummary1] (.nimbuses state)))
      (.addNimbusHost state "nimbus2:port" nimbusSummary2)
      (is (= #{nimbusSummary1 nimbusSummary2} (set (.nimbuses state))))

      ;; TODO add tests for task info and task heartbeat setting and getting
      (.disconnect state)
      )))

(defn- validate-errors! [state storm-id component errors-list]
  (let [errors (map clojurify-error (.errors state storm-id component))]
    (is (= (count errors) (count errors-list)))
    (doseq [[error target] (map vector errors errors-list)]
      (when-not (.contains (:error error) target)
        (println target " => " (:error error)))
      (is (.contains (:error error) target))
      )))

(defn- stringify-error [error]
  (let [result (java.io.StringWriter.)
        printer (java.io.PrintWriter. result)]
    (.printStackTrace error printer)
    (.toString result)))

(deftest test-storm-cluster-state-errors
  (with-open [zk (InProcessZookeeper. )]
    (with-open [_ (Time$SimulatedTime. )]
      (let [state (mk-storm-state (.getPort zk))]
        (.reportError state "a" "1" (Utils/localHostname) 6700 (RuntimeException.))
        (validate-errors! state "a" "1" ["RuntimeException"])
        (Time/advanceTimeSecs 1)
        (.reportError state "a" "1" (Utils/localHostname) 6700 (IllegalArgumentException.))
        (validate-errors! state "a" "1" ["IllegalArgumentException" "RuntimeException"])
        (doseq [i (range 10)]
          (.reportError state "a" "2" (Utils/localHostname) 6700 (RuntimeException.))
          (Time/advanceTimeSecs 2))
        (validate-errors! state "a" "2" (repeat 10 "RuntimeException"))
        (doseq [i (range 5)]
          (.reportError state "a" "2" (Utils/localHostname) 6700 (IllegalArgumentException.))
          (Time/advanceTimeSecs 2))
        (validate-errors! state "a" "2" (concat (repeat 5 "IllegalArgumentException")
                                          (repeat 5 "RuntimeException")
                                          ))

        (.disconnect state)
        ))))

(defn mkSupervisorInfo [time-secs hostname assignment-id used-ports meta scheduler-meta uptime-secs version resources-map]
  (doto (SupervisorInfo.)
    (.set_time_secs time-secs)
    (.set_hostname hostname)
    (.set_assignment_id assignment-id)
    (.set_used_ports used-ports)
    (.set_meta meta)
    (.set_scheduler_meta scheduler-meta)
    (.set_uptime_secs uptime-secs)
    (.set_version version)
    (.set_resources_map resources-map)))

(deftest test-supervisor-state
  (with-open [zk (InProcessZookeeper. )]
    (let [state1 (mk-storm-state (.getPort zk))
          state2 (mk-storm-state (.getPort zk))
          supervisor-info1 (mkSupervisorInfo 10 "hostname-1" "id1" [1 2] [] {} 1000 "0.9.2" nil)
          supervisor-info2 (mkSupervisorInfo 10 "hostname-2" "id2" [1 2] [] {} 1000 "0.9.2" nil)]
      (is (= [] (.supervisors state1 nil)))
      (.supervisorHeartbeat state2 "2" supervisor-info2)
      (.supervisorHeartbeat state1 "1" supervisor-info1)
      (is (= supervisor-info2 (.supervisorInfo state1 "2")))
      (is (= supervisor-info1 (.supervisorInfo state1 "1")))
      (is (= #{"1" "2"} (set (.supervisors state1 nil))))
      (is (= #{"1" "2"} (set (.supervisors state2 nil))))
      (.disconnect state2)
      (is (= #{"1"} (set (.supervisors state1 nil))))
      (.disconnect state1)
      )))

(deftest test-cluster-authentication
  (with-open [zk (InProcessZookeeper. )]
    (let [builder (Mockito/mock CuratorFrameworkFactory$Builder)
          conf (merge
                 (mk-config (.getPort zk))
                 {STORM-ZOOKEEPER-CONNECTION-TIMEOUT 10
                  STORM-ZOOKEEPER-SESSION-TIMEOUT 10
                  STORM-ZOOKEEPER-RETRY-INTERVAL 5
                  STORM-ZOOKEEPER-RETRY-TIMES 2
                  STORM-ZOOKEEPER-RETRY-INTERVAL-CEILING 15
                  STORM-ZOOKEEPER-AUTH-SCHEME "digest"
                  STORM-ZOOKEEPER-AUTH-PAYLOAD "storm:thisisapoorpassword"})]
      (. (Mockito/when (.connectString builder (Mockito/anyString))) (thenReturn builder))
      (. (Mockito/when (.connectionTimeoutMs builder (Mockito/anyInt))) (thenReturn builder))
      (. (Mockito/when (.sessionTimeoutMs builder (Mockito/anyInt))) (thenReturn builder))
      (CuratorUtils/testSetupBuilder builder (str (.getPort zk) "/") conf (ZookeeperAuthInfo. conf))
      (is (nil?
            (try
              (. (Mockito/verify builder) (authorization "digest" (.getBytes (conf STORM-ZOOKEEPER-AUTH-PAYLOAD))))
              (catch MockitoAssertionError e
                e)))))))

(deftest test-storm-state-callbacks
  ;; TODO finish
  )

(deftest test-cluster-state-default-acls
  (testing "The default ACLs are empty."
    (let [zk-mock (Mockito/mock ClientZookeeper)
          curator-frameworke (reify CuratorFramework (^void close [this] nil))]
      ;; No need for when clauses because we just want to return nil
      (with-open [_ (MockedClientZookeeper. zk-mock)]
        (. (Mockito/when (.mkClientImpl zk-mock (Mockito/anyMap) (Mockito/anyList) (Mockito/any) (Mockito/anyString) (Mockito/any) (Mockito/anyMap))) (thenReturn curator-frameworke))
        (ClusterUtils/mkStateStorage {} nil nil (ClusterStateContext.))
        (.mkdirsImpl (Mockito/verify zk-mock (Mockito/times 1)) (Mockito/any) (Mockito/anyString) (Mockito/eq nil))))
    (let [distributed-state-storage (reify IStateStorage
                                      (register [this callback] nil)
                                      (mkdirs [this path acls] nil))
          cluster-utils (Mockito/mock ClusterUtils)]
      (with-open [mocked-cluster (MockedCluster. cluster-utils)]
        (. (Mockito/when (.mkStateStorageImpl cluster-utils (Mockito/any) (Mockito/any) (Mockito/eq nil) (Mockito/any))) (thenReturn distributed-state-storage))
        (ClusterUtils/mkStormClusterState {} nil (ClusterStateContext.))))))

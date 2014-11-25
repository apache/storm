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
(ns backtype.storm.worker-test
  (:use [clojure test])
  (:import [backtype.storm.messaging TransportFactory]
           [backtype.storm.testing TestPlannerSpout TestPlannerBolt]
           [java.nio.channels Channels])
  (:import [org.apache.commons.io FileUtils])
  (:import [backtype.storm.utils Utils])

  (:use [backtype.storm bootstrap testing config])
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap testing])

  (:require [backtype.storm [zookeeper :as zk]])
  (:require [backtype.storm.daemon [worker :as worker]])
  )

(bootstrap)

(defn- write-supervisor-topology [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)
        topology-path (supervisor-stormcode-path stormroot)
        topology (thrift/mk-topology
                   {"1" (thrift/mk-spout-spec (TestPlannerSpout. false) :parallelism-hint 3 :conf {TOPOLOGY-TASKS 0})}
                   {"2" (thrift/mk-bolt-spec {"1" :none} (TestPlannerBolt.) :parallelism-hint 1 :conf {TOPOLOGY-TASKS 2})})
        serialize-topology (Utils/serialize topology)]
    (FileUtils/forceMkdir (File. stormroot))
    (FileUtils/writeByteArrayToFile (File. topology-path) serialize-topology)
    ))

(deftest test-launch-receive-thread-with-fixed-port
  (with-local-cluster [cluster :supervisors 0 :ports-per-supervisor 3
                       :daemon-conf {SUPERVISOR-ENABLE false
                                     TOPOLOGY-ACKER-EXECUTORS 0
                                     WORKER-DYNAMIC-PORT false
                                     STORM-LOCAL-DIR (local-temp-path)}]

    (let [daemon-conf (:daemon-conf cluster)
          state (:state cluster)
          storm-cluster-state (:storm-cluster-state cluster)
          storm-id "test-storm"
          _ (write-supervisor-topology daemon-conf storm-id)
          assignment-id (uuid)
          port 10081
          worker-id (uuid)
          worker-data (worker/worker-data daemon-conf nil storm-id assignment-id port worker-id daemon-conf state storm-cluster-state)
          receive-thread-shutdown1 (worker/launch-receive-thread worker-data)
          bind-port1 (:bind-port worker-data)]
      (is (not= nil bind-port1))
      (is (= port bind-port1))
      (is (thrown? org.jboss.netty.channel.ChannelException (worker/worker-data daemon-conf nil storm-id assignment-id port worker-id daemon-conf state storm-cluster-state)))
      (receive-thread-shutdown1)
      (delete-all [(daemon-conf STORM-LOCAL-DIR)])
    )))

(deftest test-launch-receive-thread-with-dynamic-port
  (with-local-cluster [cluster :supervisors 0 :ports-per-supervisor 3
                       :daemon-conf {SUPERVISOR-ENABLE false
                                     TOPOLOGY-ACKER-EXECUTORS 0
                                     WORKER-DYNAMIC-PORT true
                                     STORM-LOCAL-DIR (local-temp-path)}]
    (let [daemon-conf (:daemon-conf cluster)
          state (:state cluster)
          storm-cluster-state (:storm-cluster-state cluster)
          storm-id "test-storm"
          _ (write-supervisor-topology daemon-conf storm-id)
          assignment-id (uuid)
          port 10081
          worker-id (uuid)
          worker-data (worker/worker-data daemon-conf nil storm-id assignment-id port worker-id daemon-conf state storm-cluster-state)
          receive-thread-shutdown1 (worker/launch-receive-thread worker-data)
          bind-port1 (:bind-port worker-data)

          ; launch another receive thread, it will bind another port;
          worker-data2 (worker/worker-data daemon-conf nil storm-id assignment-id port worker-id daemon-conf state storm-cluster-state)
          receive-thread-shutdown2 (worker/launch-receive-thread worker-data2)
          bind-port2 (:bind-port worker-data2)]
      (is (not= nil bind-port1))
      (is (not= nil bind-port2))
      (is (not= bind-port1 bind-port2))
      (receive-thread-shutdown1)
      (receive-thread-shutdown2)
      (delete-all [(daemon-conf STORM-LOCAL-DIR)])
    )))

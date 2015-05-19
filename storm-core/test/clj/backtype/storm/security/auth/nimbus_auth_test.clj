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
(ns backtype.storm.security.auth.nimbus-auth-test
  (:use [backtype.storm cluster]
        [backtype.storm.daemon common nimbus])
  (:require [backtype.storm.testing :as t]
            [conjure.core]
            [backtype.storm.daemon [nimbus :as nimbus]]
            [backtype.storm [zookeeper :as zk]]
            [backtype.storm.log :refer [log-debug]]
            [backtype.storm.util :as util]
            [conjure.core :as conjure]
            [backtype.storm.config :as c]
            [clojure.test :refer :all])
  (:import [backtype.storm Config]
           [backtype.storm.utils NimbusClient]
           [backtype.storm.generated NotAliveException]
           [backtype.storm.security.auth AuthUtils ThriftServer ThriftClient
                                         ReqContext ThriftConnectionType]
           [backtype.storm.generated Nimbus Nimbus$Client Nimbus$Processor
                                     AuthorizationException SubmitOptions TopologyInitialStatus KillOptions]))

(def nimbus-timeout (Integer. 30))

(defn launch-test-cluster [nimbus-port login-cfg aznClass transportPluginClass]
  (let [conf {c/NIMBUS-AUTHORIZER             aznClass
              c/NIMBUS-THRIFT-PORT            nimbus-port
              c/STORM-THRIFT-TRANSPORT-PLUGIN transportPluginClass }
        conf (if login-cfg (merge conf {"java.security.auth.login.config" login-cfg}) conf)
        cluster-map (t/mk-local-storm-cluster :supervisors 0
                                            :ports-per-supervisor 0
                                            :daemon-conf conf)
        nimbus-server (ThriftServer. (:daemon-conf cluster-map)
                                     (Nimbus$Processor. (:nimbus cluster-map))
                                     ThriftConnectionType/NIMBUS)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop nimbus-server))))
    (.start (Thread. #(.serve nimbus-server)))
    (t/wait-for-condition #(.isServing nimbus-server))
    [cluster-map nimbus-server]))

(defmacro with-test-cluster [args & body]
  `(let [[cluster-map#  nimbus-server#] (launch-test-cluster  ~@args)]
      ~@body
      (log-debug "Shutdown cluster from macro")
      (t/kill-local-storm-cluster cluster-map#)
      (.stop nimbus-server#)))

(deftest Simple-authentication-test
  (let [port (util/available-port)]
    (with-test-cluster [port nil nil "backtype.storm.security.auth.SimpleTransportPlugin"]
      (let [storm-conf (merge (c/read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                               c/STORM-NIMBUS-RETRY-TIMES      0})
            client (NimbusClient. storm-conf "localhost" port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Positive authorization) Simple protocol w/o authentication/authorization enforcement"
                 (is (util/thrown-cause? NotAliveException
                              (.activate nimbus_client "topo-name"))))
        (.close client)))))

(deftest test-noop-authorization-w-simple-transport
  (let [port (util/available-port)]
    (with-test-cluster [port nil
                  "backtype.storm.security.auth.authorizer.NoopAuthorizer"
                  "backtype.storm.security.auth.SimpleTransportPlugin"]
      (let [storm-conf (merge (c/read-storm-config)
                               {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                                c/STORM-NIMBUS-RETRY-TIMES      0})
            client (NimbusClient. storm-conf "localhost" port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Positive authorization) Authorization plugin should accept client request"
                 (is (util/thrown-cause? NotAliveException
                              (.activate nimbus_client "topo-name"))))
        (.close client)))))

(deftest test-deny-authorization-w-simple-transport
  (let [port (util/available-port)]
    (with-test-cluster [port nil
                  "backtype.storm.security.auth.authorizer.DenyAuthorizer"
                  "backtype.storm.security.auth.SimpleTransportPlugin"]
      (let [storm-conf (merge (c/read-storm-config)
                               {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                               Config/NIMBUS_HOST "localhost"
                               Config/NIMBUS_THRIFT_PORT port
                                c/STORM-NIMBUS-RETRY-TIMES      0})
            client (NimbusClient/getConfiguredClient storm-conf)
            nimbus_client (.getClient client)
            topologyInitialStatus (TopologyInitialStatus/findByValue 2)
            submitOptions (SubmitOptions. topologyInitialStatus)]
        (is (util/thrown-cause? AuthorizationException (.submitTopology nimbus_client  "topo-name" nil nil nil)))
        (is (util/thrown-cause? AuthorizationException (.submitTopologyWithOpts nimbus_client  "topo-name" nil nil nil submitOptions)))
        (is (util/thrown-cause? AuthorizationException (.beginFileUpload nimbus_client)))
        (is (util/thrown-cause? AuthorizationException (.uploadChunk nimbus_client nil nil)))
        (is (util/thrown-cause? AuthorizationException (.finishFileUpload nimbus_client nil)))
        (is (util/thrown-cause? AuthorizationException (.beginFileDownload nimbus_client nil)))
        (is (util/thrown-cause? AuthorizationException (.downloadChunk nimbus_client nil)))
        (is (util/thrown-cause? AuthorizationException (.getNimbusConf nimbus_client)))
        (is (util/thrown-cause? AuthorizationException (.getClusterInfo nimbus_client)))
        (conjure/stubbing [nimbus/check-storm-active! nil
                   nimbus/try-read-storm-conf-from-name {}]
          (is (util/thrown-cause? AuthorizationException (.killTopology nimbus_client "topo-name")))
          (is (util/thrown-cause? AuthorizationException (.killTopologyWithOpts nimbus_client "topo-name" (KillOptions.))))
          (is (util/thrown-cause? AuthorizationException (.activate nimbus_client "topo-name")))
          (is (util/thrown-cause? AuthorizationException (.deactivate nimbus_client "topo-name")))
          (is (util/thrown-cause? AuthorizationException (.rebalance nimbus_client "topo-name" nil)))
        )
        (conjure/stubbing [nimbus/try-read-storm-conf {}]
          (is (util/thrown-cause? AuthorizationException (.getTopologyConf nimbus_client "topo-ID")))
          (is (util/thrown-cause? AuthorizationException (.getTopology nimbus_client "topo-ID")))
          (is (util/thrown-cause? AuthorizationException (.getUserTopology nimbus_client "topo-ID")))
          (is (util/thrown-cause? AuthorizationException (.getTopologyInfo nimbus_client "topo-ID"))))
        (.close client)))))

(deftest test-noop-authorization-w-sasl-digest
  (let [port (util/available-port)]
    (with-test-cluster [port
                  "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                  "backtype.storm.security.auth.authorizer.NoopAuthorizer"
                  "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"]
      (let [storm-conf (merge (c/read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN   "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                               Config/NIMBUS_HOST "localhost"
                               Config/NIMBUS_THRIFT_PORT port
                               c/STORM-NIMBUS-RETRY-TIMES        0})
            client (NimbusClient/getConfiguredClient storm-conf)
            nimbus_client (.getClient client)]
        (testing "(Positive authorization) Authorization plugin should accept client request"
                 (is (util/thrown-cause? NotAliveException
                              (.activate nimbus_client "topo-name"))))
        (.close client)))))

(deftest test-deny-authorization-w-sasl-digest
  (let [port (util/available-port)]
    (with-test-cluster [port
                  "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                  "backtype.storm.security.auth.authorizer.DenyAuthorizer"
                  "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"]
      (let [storm-conf (merge (c/read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN   "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                               Config/NIMBUS_HOST "localhost"
                               Config/NIMBUS_THRIFT_PORT port
                               c/STORM-NIMBUS-RETRY-TIMES        0})
            client (NimbusClient/getConfiguredClient storm-conf)
            nimbus_client (.getClient client)
            topologyInitialStatus (TopologyInitialStatus/findByValue 2)
            submitOptions (SubmitOptions. topologyInitialStatus)]
        (is (util/thrown-cause? AuthorizationException (.submitTopology nimbus_client  "topo-name" nil nil nil)))
        (is (util/thrown-cause? AuthorizationException (.submitTopologyWithOpts nimbus_client  "topo-name" nil nil nil submitOptions)))
        (is (util/thrown-cause? AuthorizationException (.beginFileUpload nimbus_client)))
        (is (util/thrown-cause? AuthorizationException (.uploadChunk nimbus_client nil nil)))
        (is (util/thrown-cause? AuthorizationException (.finishFileUpload nimbus_client nil)))
        (is (util/thrown-cause? AuthorizationException (.beginFileDownload nimbus_client nil)))
        (is (util/thrown-cause? AuthorizationException (.downloadChunk nimbus_client nil)))
        (is (util/thrown-cause? AuthorizationException (.getNimbusConf nimbus_client)))
        (is (util/thrown-cause? AuthorizationException (.getClusterInfo nimbus_client)))
        (conjure/stubbing [nimbus/check-storm-active! nil
                   nimbus/try-read-storm-conf-from-name {}]
          (is (util/thrown-cause? AuthorizationException (.killTopology nimbus_client "topo-name")))
          (is (util/thrown-cause? AuthorizationException (.killTopologyWithOpts nimbus_client "topo-name" (KillOptions.))))
          (is (util/thrown-cause? AuthorizationException (.activate nimbus_client "topo-name")))
          (is (util/thrown-cause? AuthorizationException (.deactivate nimbus_client "topo-name")))
          (is (util/thrown-cause? AuthorizationException (.rebalance nimbus_client "topo-name" nil))))
        (conjure/stubbing [nimbus/try-read-storm-conf {}]
          (is (util/thrown-cause? AuthorizationException (.getTopologyConf nimbus_client "topo-ID")))
          (is (util/thrown-cause? AuthorizationException (.getTopology nimbus_client "topo-ID")))
          (is (util/thrown-cause? AuthorizationException (.getUserTopology nimbus_client "topo-ID")))
          (is (util/thrown-cause? AuthorizationException (.getTopologyInfo nimbus_client "topo-ID"))))
        (.close client)))))


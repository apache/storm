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
(ns org.apache.storm.security.auth.nimbus-auth-test
  (:use [clojure test])
  (:require [org.apache.storm.security.auth [auth-test :refer [nimbus-timeout]]])
  (:import [java.nio ByteBuffer])
  (:import [java.util Optional])
  (:import [org.apache.storm LocalCluster$Builder DaemonConfig Config])
  (:import [org.apache.storm.blobstore BlobStore])
  (:import [org.apache.storm.utils NimbusClient])
  (:import [org.apache.storm.generated NotAliveException StormBase])
  (:import [org.apache.storm.security.auth AuthUtils ThriftServer ThriftClient
                                         ReqContext ThriftConnectionType])
  (:import [org.apache.storm.generated Nimbus Nimbus$Client Nimbus$Processor
            AuthorizationException SubmitOptions TopologyInitialStatus KillOptions])
  (:import [org.apache.storm.utils ConfigUtils Utils])
  (:import [org.apache.storm.cluster IStormClusterState])
  (:import [org.mockito Mockito Matchers])
  (:use [org.apache.storm util config daemon-config log])
  (:require [conjure.core])
  (:use [conjure core]))

(defn to-conf [nimbus-port login-cfg aznClass transportPluginClass]
  (let [conf {NIMBUS-AUTHORIZER aznClass
              NIMBUS-THRIFT-PORT nimbus-port
              STORM-THRIFT-TRANSPORT-PLUGIN transportPluginClass }
         conf (if login-cfg (merge conf {"java.security.auth.login.config" login-cfg}) conf)]
    conf))

(defmacro with-test-cluster [[cluster-sym & args] & body]
  `(let [conf# (to-conf ~@args)]
     (with-open [~cluster-sym (.build (doto (LocalCluster$Builder. )
                      (.withNimbusDaemon)
                      (.withDaemonConf conf#)
                      (.withSupervisors 0)
                      (.withPortsPerSupervisor 0)))]
       ~@body)))

(deftest Simple-authentication-test
  (with-test-cluster [cluster 0 nil nil "org.apache.storm.security.auth.SimpleTransportPlugin"]
    (let [storm-conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                            {STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.SimpleTransportPlugin"
                             STORM-NIMBUS-RETRY-TIMES 0})
          client (NimbusClient. storm-conf "localhost" (.getThriftServerPort cluster) nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Simple protocol w/o authentication/authorization enforcement"
               (is (thrown-cause? NotAliveException
                            (.activate nimbus_client "topo-name"))))
      (.close client))))

(deftest test-noop-authorization-w-simple-transport
  (let [cluster-state (Mockito/mock IStormClusterState)
        blob-store (Mockito/mock BlobStore)
        topo-name "topo-name"]
    (.thenReturn (Mockito/when (.getTopoId cluster-state topo-name)) (Optional/empty))
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder.)
                            (.withClusterState cluster-state)
                            (.withBlobStore blob-store)
                            (.withNimbusDaemon)
                            (.withDaemonConf
                               {NIMBUS-AUTHORIZER "org.apache.storm.security.auth.authorizer.NoopAuthorizer"
                                NIMBUS-THRIFT-PORT 0
                                STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.SimpleTransportPlugin"})))]
      (let [storm-conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                               {STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.SimpleTransportPlugin"
                                STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" (.getThriftServerPort cluster) nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Positive authorization) Authorization plugin should accept client request"
                 (is (thrown-cause? NotAliveException
                              (.activate nimbus_client topo-name))))
        (.close client)))))

(deftest test-deny-authorization-w-simple-transport
  (let [cluster-state (Mockito/mock IStormClusterState)
        blob-store (Mockito/mock BlobStore)
        topo-name "topo-name"
        topo-id "topo-name-1"]
    (.thenReturn (Mockito/when (.getTopoId cluster-state topo-name)) (Optional/of topo-id))
    (.thenReturn (Mockito/when (.readTopologyConf blob-store (Mockito/any String) (Mockito/anyObject))) {})
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder.)
                            (.withClusterState cluster-state)
                            (.withBlobStore blob-store)
                            (.withNimbusDaemon)
                            (.withDaemonConf
                               {NIMBUS-AUTHORIZER "org.apache.storm.security.auth.authorizer.DenyAuthorizer"
                                NIMBUS-THRIFT-PORT 0
                                STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.SimpleTransportPlugin"})))]
      (let [storm-conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                              {STORM-THRIFT-TRANSPORT-PLUGIN   "org.apache.storm.security.auth.SimpleTransportPlugin"
                               Config/NIMBUS_THRIFT_PORT (.getThriftServerPort cluster)
                               STORM-NIMBUS-RETRY-TIMES        0})
            client (NimbusClient. storm-conf "localhost" (.getThriftServerPort cluster) nimbus-timeout)
            nimbus_client (.getClient client)
            topologyInitialStatus (TopologyInitialStatus/findByValue 2)
            submitOptions (SubmitOptions. topologyInitialStatus)]
        (is (thrown-cause? AuthorizationException (.submitTopology nimbus_client topo-name nil nil nil)))
        (is (thrown-cause? AuthorizationException (.submitTopologyWithOpts nimbus_client topo-name nil nil nil submitOptions)))
        (is (thrown-cause? AuthorizationException (.beginFileUpload nimbus_client)))

        (is (thrown-cause? AuthorizationException (.uploadChunk nimbus_client nil nil)))
        (is (thrown-cause? AuthorizationException (.finishFileUpload nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.beginFileDownload nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.downloadChunk nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.getNimbusConf nimbus_client)))
        (is (thrown-cause? AuthorizationException (.getClusterInfo nimbus_client)))
        (is (thrown-cause? AuthorizationException (.killTopology nimbus_client topo-name)))
        (is (thrown-cause? AuthorizationException (.killTopologyWithOpts nimbus_client topo-name (KillOptions.))))
        (is (thrown-cause? AuthorizationException (.activate nimbus_client topo-name)))
        (is (thrown-cause? AuthorizationException (.deactivate nimbus_client topo-name)))
        (is (thrown-cause? AuthorizationException (.rebalance nimbus_client topo-name nil)))
        (is (thrown-cause? AuthorizationException (.getTopologyConf nimbus_client topo-id)))
        (is (thrown-cause? AuthorizationException (.getTopology nimbus_client topo-id)))
        (is (thrown-cause? AuthorizationException (.getUserTopology nimbus_client topo-id)))
        (is (thrown-cause? AuthorizationException (.getTopologyInfo nimbus_client topo-id)))
        (.close client)))))

(deftest test-noop-authorization-w-sasl-digest
  (with-test-cluster [cluster 0
                "test/clj/org/apache/storm/security/auth/jaas_digest.conf"
                "org.apache.storm.security.auth.authorizer.NoopAuthorizer"
                "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"]
    (let [storm-conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                            {STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"
                             "java.security.auth.login.config" "test/clj/org/apache/storm/security/auth/jaas_digest.conf"
                             Config/NIMBUS_THRIFT_PORT (.getThriftServerPort cluster)
                             STORM-NIMBUS-RETRY-TIMES 0})
          client (NimbusClient. storm-conf "localhost" (.getThriftServerPort cluster) nimbus-timeout)
          nimbus_client (.getClient client)]
      (testing "(Positive authorization) Authorization plugin should accept client request"
               (is (thrown-cause? NotAliveException
                            (.activate nimbus_client "topo-name"))))
      (.close client))))

(deftest test-deny-authorization-w-sasl-digest
  (let [cluster-state (Mockito/mock IStormClusterState)
        blob-store (Mockito/mock BlobStore)
        topo-name "topo-name"
        topo-id "topo-name-1"]
    (.thenReturn (Mockito/when (.getTopoId cluster-state topo-name)) (Optional/of topo-id))
    (.thenReturn (Mockito/when (.readTopologyConf blob-store (Mockito/any String) (Mockito/anyObject))) {})
    (with-open [cluster (.build
                          (doto (LocalCluster$Builder.)
                            (.withClusterState cluster-state)
                            (.withBlobStore blob-store)
                            (.withNimbusDaemon)
                            (.withDaemonConf
                               {NIMBUS-AUTHORIZER "org.apache.storm.security.auth.authorizer.DenyAuthorizer"
                                NIMBUS-THRIFT-PORT 0
                                "java.security.auth.login.config" "test/clj/org/apache/storm/security/auth/jaas_digest.conf"
                                STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"})))]
      (let [storm-conf (merge (clojurify-structure (ConfigUtils/readStormConfig))
                               {STORM-THRIFT-TRANSPORT-PLUGIN "org.apache.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/org/apache/storm/security/auth/jaas_digest.conf"
                               Config/NIMBUS_THRIFT_PORT (.getThriftServerPort cluster)
                               STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" (.getThriftServerPort cluster) nimbus-timeout)
            nimbus_client (.getClient client)
            topologyInitialStatus (TopologyInitialStatus/findByValue 2)
            submitOptions (SubmitOptions. topologyInitialStatus)]
        (is (thrown-cause? AuthorizationException (.submitTopology nimbus_client topo-name nil nil nil)))
        (is (thrown-cause? AuthorizationException (.submitTopologyWithOpts nimbus_client topo-name nil nil nil submitOptions)))
        (is (thrown-cause? AuthorizationException (.beginFileUpload nimbus_client)))

        (is (thrown-cause? AuthorizationException (.uploadChunk nimbus_client nil nil)))
        (is (thrown-cause? AuthorizationException (.finishFileUpload nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.beginFileDownload nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.downloadChunk nimbus_client nil)))
        (is (thrown-cause? AuthorizationException (.getNimbusConf nimbus_client)))
        (is (thrown-cause? AuthorizationException (.getClusterInfo nimbus_client)))
        (is (thrown-cause? AuthorizationException (.killTopology nimbus_client topo-name)))
        (is (thrown-cause? AuthorizationException (.killTopologyWithOpts nimbus_client topo-name (KillOptions.))))
        (is (thrown-cause? AuthorizationException (.activate nimbus_client topo-name)))
        (is (thrown-cause? AuthorizationException (.deactivate nimbus_client topo-name)))
        (is (thrown-cause? AuthorizationException (.rebalance nimbus_client topo-name nil)))
        (is (thrown-cause? AuthorizationException (.getTopologyConf nimbus_client topo-id)))
        (is (thrown-cause? AuthorizationException (.getTopology nimbus_client topo-id)))
        (is (thrown-cause? AuthorizationException (.getUserTopology nimbus_client topo-id)))
        (is (thrown-cause? AuthorizationException (.getTopologyInfo nimbus_client topo-id)))
        (.close client)))))


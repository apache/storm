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
(ns backtype.storm.security.auth.auth-test
  (:use [backtype.storm.config]
        [backtype.storm.daemon.common]
        [backtype.storm.testing])
  (:require [backtype.storm.daemon.nimbus :as nimbus]
            [backtype.storm.util :as util]
            [backtype.storm.config :as c]
            [clojure.test :refer :all])
  (:import [backtype.storm.security.auth.authorizer ImpersonationAuthorizer]
           [org.apache.thrift.transport TTransportException]
           [java.nio ByteBuffer]
           [java.security Principal]
           [javax.security.auth Subject]
           [java.net InetAddress]
           [backtype.storm Config]
           [backtype.storm.utils NimbusClient]
           [backtype.storm.security.auth.authorizer SimpleWhitelistAuthorizer SimpleACLAuthorizer]
           [backtype.storm.security.auth AuthUtils ThriftServer ShellBasedGroupsMapping
                                         ReqContext KerberosPrincipalToLocal ThriftConnectionType]
           [backtype.storm.generated Nimbus Nimbus$Iface StormTopology SubmitOptions
                                     KillOptions RebalanceOptions ClusterSummary TopologyInfo Nimbus$Processor
                                     AuthorizationException]))

(defn mk-principal [name]
  (reify Principal
    (equals [this other]
      (= name (.getName other)))
    (getName [this] name)
    (toString [this] name)
    (hashCode [this] (.hashCode name))))

(defn mk-subject [name]
  (Subject. true #{(mk-principal name)} #{} #{}))

(def nimbus-timeout (Integer. 120))

(defn nimbus-data [storm-conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)]
    {:conf storm-conf
     :inimbus inimbus
     :authorization-handler (mk-authorization-handler (storm-conf c/NIMBUS-AUTHORIZER) storm-conf)
     :submitted-count (atom 0)
     :storm-cluster-state nil
     :submit-lock (Object.)
     :heartbeats-cache (atom {})
     :downloaders nil
     :uploaders nil
     :uptime (util/uptime-computer)
     :validator nil
     :timer nil
     :scheduler nil
     }))

(defn dummy-service-handler
  ([conf inimbus auth-context]
     (let [nimbus-d (nimbus-data conf inimbus)
           topo-conf (atom nil)]
       (reify Nimbus$Iface
         (^void submitTopologyWithOpts [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
                                        ^SubmitOptions submitOptions]
           (if (not (nil? serializedConf)) (swap! topo-conf (fn [prev new] new) (util/from-json serializedConf)))
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "submitTopology" auth-context))

         (^void killTopology [this ^String storm-name]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "killTopology" auth-context))

         (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "killTopology" auth-context))

         (^void rebalance [this ^String storm-name ^RebalanceOptions options]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "rebalance" auth-context))

         (activate [this storm-name]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "activate" auth-context))

         (deactivate [this storm-name]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "deactivate" auth-context))

         (uploadNewCredentials [this storm-name creds]
           (nimbus/check-authorization! nimbus-d storm-name @topo-conf "uploadNewCredentials" auth-context))

         (beginFileUpload [this])

         (^void uploadChunk [this ^String location ^ByteBuffer chunk])

         (^void finishFileUpload [this ^String location])

         (^String beginFileDownload [this ^String file]
           (nimbus/check-authorization! nimbus-d nil nil "fileDownload" auth-context)
           "Done!")

         (^ByteBuffer downloadChunk [this ^String id])

         (^String getNimbusConf [this])

         (^String getTopologyConf [this ^String id])

         (^StormTopology getTopology [this ^String id])

         (^StormTopology getUserTopology [this ^String id])

         (^ClusterSummary getClusterInfo [this])

         (^TopologyInfo getTopologyInfo [this ^String storm-id]))))
  ([conf inimbus]
     (dummy-service-handler conf inimbus nil)))


(defn launch-server [server-port login-cfg aznClass transportPluginClass serverConf]
  (let [conf1 (merge (c/read-storm-config)
                     {c/NIMBUS-AUTHORIZER aznClass
                      c/NIMBUS-HOST "localhost"
                      c/NIMBUS-THRIFT-PORT server-port
                      c/STORM-THRIFT-TRANSPORT-PLUGIN transportPluginClass})
        conf2 (if login-cfg (merge conf1 {"java.security.auth.login.config" login-cfg}) conf1)
        conf (if serverConf (merge conf2 serverConf) conf2)
        nimbus (nimbus/standalone-nimbus)
        service-handler (dummy-service-handler conf nimbus)
        server (ThriftServer.
                conf
                (Nimbus$Processor. service-handler)
                ThriftConnectionType/NIMBUS)]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.stop server))))
    (.start (Thread. #(.serve server)))
    (wait-for-condition #(.isServing server))
    server ))

(defmacro with-server [args & body]
  `(let [server# (launch-server ~@args)]
     ~@body
     (.stop server#)
     ))

(deftest kerb-to-local-test
  (let [kptol (KerberosPrincipalToLocal. )]
    (.prepare kptol {})
    (is (= "me" (.toLocal kptol (mk-principal "me@realm"))))
    (is (= "simple" (.toLocal kptol (mk-principal "simple"))))
    (is (= "someone" (.toLocal kptol (mk-principal "someone/host@realm"))))))

(deftest Simple-authentication-test
  (let [a-port (util/available-port)]
    (with-server [a-port nil nil "backtype.storm.security.auth.SimpleTransportPlugin" nil]
      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
            client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
            nimbus_client (.getClient client)]
        (.activate nimbus_client "security_auth_test_topology")
        (.close client))

      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                               c/STORM-NIMBUS-RETRY-TIMES 0})]
        (testing "(Negative authentication) Server: Simple vs. Client: Digest"
          (is (util/thrown-cause?  org.apache.thrift.transport.TTransportException
                              (NimbusClient. storm-conf "localhost" a-port nimbus-timeout))))))))

(deftest negative-whitelist-authorization-test
  (let [a-port (util/available-port)]
    (with-server [a-port nil
                  "backtype.storm.security.auth.authorizer.SimpleWhitelistAuthorizer"
                  "backtype.storm.testing.SingleUserSimpleTransport" nil]
      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
            client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Negative authorization) Authorization plugin should reject client request"
          (is (util/thrown-cause? AuthorizationException
                             (.activate nimbus_client "security_auth_test_topology"))))
        (.close client)))))

(deftest positive-whitelist-authorization-test
    (let [a-port (util/available-port)]
      (with-server [a-port nil
                    "backtype.storm.security.auth.authorizer.SimpleWhitelistAuthorizer"
                    "backtype.storm.testing.SingleUserSimpleTransport" {SimpleWhitelistAuthorizer/WHITELIST_USERS_CONF ["user"]}]
        (let [storm-conf (merge (read-storm-config)
                                {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.testing.SingleUserSimpleTransport"})
              client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
              nimbus_client (.getClient client)]
          (testing "(Positive authorization) Authorization plugin should accept client request"
            (.activate nimbus_client "security_auth_test_topology"))
          (.close client)))))

(deftest simple-acl-user-auth-test
  (let [cluster-conf (merge (read-storm-config)
                       {c/NIMBUS-ADMINS ["admin"]
                        c/NIMBUS-SUPERVISOR-USERS ["supervisor"]})
        authorizer (SimpleACLAuthorizer. )
        admin-user (mk-subject "admin")
        supervisor-user (mk-subject "supervisor")
        user-a (mk-subject "user-a")
        user-b (mk-subject "user-b")]
  (.prepare authorizer cluster-conf)
  (is (= true (.permit authorizer (ReqContext. user-a) "submitTopology" {})))
  (is (= true (.permit authorizer (ReqContext. user-b) "submitTopology" {})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "submitTopology" {})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "submitTopology" {})))

  (is (= true (.permit authorizer (ReqContext. user-a) "fileUpload" nil)))
  (is (= true (.permit authorizer (ReqContext. user-b) "fileUpload" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "fileUpload" nil)))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "fileUpload" nil)))

  (is (= true (.permit authorizer (ReqContext. user-a) "getNimbusConf" nil)))
  (is (= true (.permit authorizer (ReqContext. user-b) "getNimbusConf" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getNimbusConf" nil)))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getNimbusConf" nil)))

  (is (= true (.permit authorizer (ReqContext. user-a) "getClusterInfo" nil)))
  (is (= true (.permit authorizer (ReqContext. user-b) "getClusterInfo" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getClusterInfo" nil)))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getClusterInfo" nil)))

  (is (= false (.permit authorizer (ReqContext. user-a) "fileDownload" nil)))
  (is (= false (.permit authorizer (ReqContext. user-b) "fileDownload" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "fileDownload" nil)))
  (is (= true (.permit authorizer (ReqContext. supervisor-user) "fileDownload" nil)))

  (is (= true (.permit authorizer (ReqContext. user-a) "killTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "killTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "killTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "killTopolgy" {c/TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "uploadNewCredentials" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "uploadNewCredentials" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "uploadNewCredentials" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "uploadNewCredentials" {c/TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "rebalance" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "rebalance" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "rebalance" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "rebalance" {c/TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "activate" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "activate" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "activate" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "activate" {c/TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "deactivate" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "deactivate" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "deactivate" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "deactivate" {c/TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "getTopologyConf" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "getTopologyConf" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopologyConf" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getTopologyConf" {c/TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "getTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "getTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getTopology" {c/TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "getUserTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "getUserTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getUserTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getUserTopology" {c/TOPOLOGY-USERS ["user-a"]})))

  (is (= true (.permit authorizer (ReqContext. user-a) "getTopologyInfo" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. user-b) "getTopologyInfo" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopologyInfo" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= false (.permit authorizer (ReqContext. supervisor-user) "getTopologyInfo" {c/TOPOLOGY-USERS ["user-a"]})))
))

(deftest simple-acl-nimbus-users-auth-test
  (let [cluster-conf (merge (read-storm-config)
                            {c/NIMBUS-ADMINS ["admin"]
                             c/NIMBUS-SUPERVISOR-USERS ["supervisor"]
                             c/NIMBUS-USERS ["user-a"]})
        authorizer (SimpleACLAuthorizer. )
        admin-user (mk-subject "admin")
        supervisor-user (mk-subject "supervisor")
        user-a (mk-subject "user-a")
        user-b (mk-subject "user-b")]
    (.prepare authorizer cluster-conf)
    (is (= true (.permit authorizer (ReqContext. user-a) "submitTopology" {})))
    (is (= false (.permit authorizer (ReqContext. user-b) "submitTopology" {})))
    (is (= true (.permit authorizer (ReqContext. admin-user) "fileUpload" nil)))
    (is (= true (.permit authorizer (ReqContext. supervisor-user) "fileDownload" nil)))))

(deftest shell-based-groups-mapping-test
  (let [cluster-conf (read-storm-config)
        groups (ShellBasedGroupsMapping. )
        user-name (System/getProperty "user.name")]
    (.prepare groups cluster-conf)
    (is (<= 0 (.size (.getGroups groups user-name))))
    (is (= 0 (.size (.getGroups groups "userDoesNotExist"))))
    (is (= 0 (.size (.getGroups groups nil))))))

(deftest simple-acl-same-user-auth-test
  (let [cluster-conf (merge (read-storm-config)
                       {c/NIMBUS-ADMINS ["admin"]
                        c/NIMBUS-SUPERVISOR-USERS ["admin"]})
        authorizer (SimpleACLAuthorizer. )
        admin-user (mk-subject "admin")]
  (.prepare authorizer cluster-conf)
  (is (= true (.permit authorizer (ReqContext. admin-user) "submitTopology" {})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "fileUpload" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getNimbusConf" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getClusterInfo" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "fileDownload" nil)))
  (is (= true (.permit authorizer (ReqContext. admin-user) "killTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "uploadNewCredentials" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "rebalance" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "activate" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "deactivate" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopologyConf" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getUserTopology" {c/TOPOLOGY-USERS ["user-a"]})))
  (is (= true (.permit authorizer (ReqContext. admin-user) "getTopologyInfo" {c/TOPOLOGY-USERS ["user-a"]})))
))


(deftest positive-authorization-test
  (let [a-port (util/available-port)]
    (with-server [a-port nil
                  "backtype.storm.security.auth.authorizer.NoopAuthorizer"
                  "backtype.storm.security.auth.SimpleTransportPlugin" nil]
      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"})
            client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Positive authorization) Authorization plugin should accept client request"
          (.activate nimbus_client "security_auth_test_topology"))
        (.close client)))))

(deftest deny-authorization-test
  (let [a-port (util/available-port)]
    (with-server [a-port nil
                  "backtype.storm.security.auth.authorizer.DenyAuthorizer"
                  "backtype.storm.security.auth.SimpleTransportPlugin" nil]
      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                               Config/NIMBUS_HOST "localhost"
                               Config/NIMBUS_THRIFT_PORT a-port
                               Config/NIMBUS_TASK_TIMEOUT_SECS nimbus-timeout})
            client (NimbusClient/getConfiguredClient storm-conf)
            nimbus_client (.getClient client)]
        (testing "(Negative authorization) Authorization plugin should reject client request"
          (is (util/thrown-cause? AuthorizationException
                             (.activate nimbus_client "security_auth_test_topology"))))
        (.close client)))))

(deftest digest-authentication-test
  (let [a-port (util/available-port)]
    (with-server [a-port
                  "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                  nil
                  "backtype.storm.security.auth.digest.DigestSaslTransportPlugin" nil]
      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest.conf"
                               c/STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Positive authentication) valid digest authentication"
          (.activate nimbus_client "security_auth_test_topology"))
        (.close client))

      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.SimpleTransportPlugin"
                               c/STORM-NIMBUS-RETRY-TIMES 0})
            client (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)
            nimbus_client (.getClient client)]
        (testing "(Negative authentication) Server: Digest vs. Client: Simple"
          (is (util/thrown-cause? org.apache.thrift.transport.TTransportException
                             (.activate nimbus_client "security_auth_test_topology"))))
        (.close client))

      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_bad_password.conf"
                               c/STORM-NIMBUS-RETRY-TIMES 0})]
        (testing "(Negative authentication) Invalid  password"
          (is (util/thrown-cause? TTransportException
                             (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)))))

      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_unknown_user.conf"
                               c/STORM-NIMBUS-RETRY-TIMES 0})]
        (testing "(Negative authentication) Unknown user"
          (is (util/thrown-cause? TTransportException
                             (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)))))

      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/nonexistent.conf"
                               c/STORM-NIMBUS-RETRY-TIMES 0})]
        (testing "(Negative authentication) nonexistent configuration file"
          (is (util/thrown-cause? RuntimeException
                             (NimbusClient. storm-conf "localhost" a-port nimbus-timeout)))))

      (let [storm-conf (merge (read-storm-config)
                              {c/STORM-THRIFT-TRANSPORT-PLUGIN "backtype.storm.security.auth.digest.DigestSaslTransportPlugin"
                               "java.security.auth.login.config" "test/clj/backtype/storm/security/auth/jaas_digest_missing_client.conf"
                               c/STORM-NIMBUS-RETRY-TIMES 0})]
        (testing "(Negative authentication) Missing client"
          (is (util/thrown-cause? java.io.IOException
                             (NimbusClient. storm-conf "localhost" a-port nimbus-timeout))))))))

(deftest test-GetTransportPlugin-throws-RuntimeException
  (let [conf (merge (read-storm-config)
                    {Config/STORM_THRIFT_TRANSPORT_PLUGIN "null.invalid"})]
    (is (util/thrown-cause? RuntimeException (AuthUtils/GetTransportPlugin conf nil nil)))))

(defn mk-impersonating-req-context [impersonating-user user-being-impersonated remote-address]
  (let [impersonating-principal (mk-principal impersonating-user)
        principal-being-impersonated (mk-principal user-being-impersonated)
        subject (Subject. true #{principal-being-impersonated} #{} #{})
        req_context (ReqContext. subject)]
    (.setRemoteAddress req_context remote-address)
    (.setRealPrincipal req_context impersonating-principal)
    req_context))

(deftest impersonation-authorizer-test
  (let [impersonating-user "admin"
        user-being-impersonated (System/getProperty "user.name")
        groups (ShellBasedGroupsMapping.)
        _ (.prepare groups (read-storm-config))
        groups (.getGroups groups user-being-impersonated)
        cluster-conf (merge (read-storm-config)
                       {Config/NIMBUS_IMPERSONATION_ACL {impersonating-user {"hosts" [ (.getHostName (InetAddress/getLocalHost))]
                                                                            "groups" groups}}})
        authorizer (ImpersonationAuthorizer. )
        unauthorized-host (com.google.common.net.InetAddresses/forString "10.10.10.10")
        ]

    (.prepare authorizer cluster-conf)
    ;;non impersonating request, should be permitted.
    (is (= true (.permit authorizer (ReqContext. (mk-subject "anyuser")) "fileUpload" nil)))

    ;;user with no impersonation acl should be reject
    (is (= false (.permit authorizer (mk-impersonating-req-context "user-with-no-acl" user-being-impersonated (InetAddress/getLocalHost)) "someOperation" nil)))

    ;;request from hosts that are not authorized should be rejected, commented because
    (is (= false (.permit authorizer (mk-impersonating-req-context impersonating-user user-being-impersonated unauthorized-host) "someOperation" nil)))

    ;;request to impersonate users from unauthroized groups should be rejected.
    (is (= false (.permit authorizer (mk-impersonating-req-context impersonating-user "unauthroized-user" (InetAddress/getLocalHost)) "someOperation" nil)))

    ;;request from authorized hosts and group should be allowed.
    (is (= true (.permit authorizer (mk-impersonating-req-context impersonating-user user-being-impersonated (InetAddress/getLocalHost)) "someOperation" nil)))))

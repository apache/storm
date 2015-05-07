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
(ns backtype.storm.utils-test
  (:require [backtype.storm.util :as util]
            [backtype.storm.config :as c]
            [clojure.test :refer :all])
  (:import [backtype.storm Config]
           [backtype.storm.utils NimbusClient Utils]
           [org.apache.curator.retry ExponentialBackoffRetry]
           [org.apache.thrift.transport TTransportException]))

(deftest test-new-curator-uses-exponential-backoff
  (let [expected_interval 2400
        expected_retries 10
        expected_ceiling 3000
        conf (merge (util/clojurify-structure (Utils/readDefaultConfig))
          {Config/STORM_ZOOKEEPER_RETRY_INTERVAL expected_interval
           Config/STORM_ZOOKEEPER_RETRY_TIMES expected_retries
           Config/STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING expected_ceiling})
        servers ["bogus_server"]
        arbitrary_port 42
        curator (Utils/newCurator conf servers arbitrary_port nil)
        retry (-> curator .getZookeeperClient .getRetryPolicy)
       ]
    (is (.isAssignableFrom ExponentialBackoffRetry (.getClass retry)))
    (is (= (.getBaseSleepTimeMs retry) expected_interval))
    (is (= (.getN retry) expected_retries))
    (is (= (.getSleepTimeMs retry 10 0) expected_ceiling))
  )
)

(deftest test-getConfiguredClient-throws-RunTimeException-on-bad-config
  (let [storm-conf (merge (c/read-storm-config)
                     {c/STORM-THRIFT-TRANSPORT-PLUGIN
                       "backtype.storm.security.auth.SimpleTransportPlugin"
                      Config/NIMBUS_HOST ""
                      Config/NIMBUS_THRIFT_PORT 65535
                      c/STORM-NIMBUS-RETRY-TIMES 0})]
    (is (util/thrown-cause? RuntimeException
      (NimbusClient/getConfiguredClient storm-conf)))
  )
)

(deftest test-getConfiguredClient-throws-RunTimeException-on-bad-args
  (let [storm-conf (merge
                    (c/read-storm-config)
                    {c/STORM-NIMBUS-RETRY-TIMES 0})]
    (is (util/thrown-cause? TTransportException
      (NimbusClient. storm-conf "" 65535)
    ))
  )
)

(deftest test-isZkAuthenticationConfiguredTopology
    (testing "Returns false on null config"
      (is (not (Utils/isZkAuthenticationConfiguredTopology nil))))
    (testing "Returns false on scheme key missing"
      (is (not (Utils/isZkAuthenticationConfiguredTopology
          {c/STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME nil}))))
    (testing "Returns false on scheme value null"
      (is (not
        (Utils/isZkAuthenticationConfiguredTopology
          {c/STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME nil}))))
    (testing "Returns true when scheme set to string"
      (is
        (Utils/isZkAuthenticationConfiguredTopology
          {c/STORM-ZOOKEEPER-TOPOLOGY-AUTH-SCHEME "foobar"}))))

(deftest test-isZkAuthenticationConfiguredStormServer
  (let [k "java.security.auth.login.config"
        oldprop (System/getProperty k)]
    (try
      (.remove (System/getProperties) k)
      (testing "Returns false on null config"
        (is (not (Utils/isZkAuthenticationConfiguredStormServer nil))))
      (testing "Returns false on scheme key missing"
        (is (not (Utils/isZkAuthenticationConfiguredStormServer
            {c/STORM-ZOOKEEPER-AUTH-SCHEME nil}))))
      (testing "Returns false on scheme value null"
        (is (not
          (Utils/isZkAuthenticationConfiguredStormServer
            {c/STORM-ZOOKEEPER-AUTH-SCHEME nil}))))
      (testing "Returns true when scheme set to string"
        (is
          (Utils/isZkAuthenticationConfiguredStormServer
            {c/STORM-ZOOKEEPER-AUTH-SCHEME "foobar"})))
      (testing "Returns true when java.security.auth.login.config is set"
        (do
          (System/setProperty k "anything")
          (is (Utils/isZkAuthenticationConfiguredStormServer {}))))
      (testing "Returns false when java.security.auth.login.config is set"
        (do
          (System/setProperty k "anything")
          (is (Utils/isZkAuthenticationConfiguredStormServer {}))))
    (finally
      (if (util/not-nil? oldprop)
        (System/setProperty k oldprop)
        (.remove (System/getProperties) k))))))

(deftest test-secs-to-millis-long
  (is (= 0 (util/secs-to-millis-long 0)))
  (is (= 2 (util/secs-to-millis-long 0.002)))
  (is (= 500 (util/secs-to-millis-long 0.5)))
  (is (= 1000 (util/secs-to-millis-long 1)))
  (is (= 1080 (util/secs-to-millis-long 1.08)))
  (is (= 10000 (util/secs-to-millis-long 10)))
  (is (= 10100 (util/secs-to-millis-long 10.1)))
)


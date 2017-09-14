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
(ns org.apache.storm.local-assignment-test
  (:use [clojure test])
  (:use [org.apache.storm testing config])
  (:require [org.apache.storm.converter :as converter])
  (:import [org.apache.storm.utils Utils]
           (org.apache.storm.daemon.common Assignment)
           (org.apache.storm.assignments LocalAssignmentsBackendFactory)))


(defn serialize-assignment
  [assignment]
  (Utils/serialize (converter/thriftify-assignment assignment)))

(defn thriftify-assignment
  [assignment]
  (converter/thriftify-assignment assignment))

(defn deserialize-assignment
  [assignment]
  (converter/clojurify-assignment assignment))

(deftest test-local-assignment
  (with-local-tmp [dir1]
                  (let [[storm1 assignment1] ["storm1" (Assignment.
                                                         "master_code_dir1"
                                                         {"node1" "host1"}
                                                         {[1] ["node1" 9723]}
                                                         {[1] 12345}
                                                         {["node1" 9723] [1 3 2]}
                                                         "o")]
                        [storm2 assignment2] ["storm2" (Assignment.
                                                         "master_code_dir2"
                                                         {"node2" "host2"}
                                                         {[1] ["node2" 9723]}
                                                         {[1] 12345}
                                                         {["node2" 9723] [1 3 2]}
                                                         "o")]
                        conf (read-storm-config)
                        assbd (doto (LocalAssignmentsBackendFactory/getBackend conf)
                                              (.prepare conf))]
                    (is (= nil (.getAssignment assbd "storm1")))
                    (.keepOrUpdateAssignment assbd storm1 (thriftify-assignment assignment1))
                    (.keepOrUpdateAssignment assbd storm2 (thriftify-assignment assignment2))
                    (is (= assignment1 (deserialize-assignment (.getAssignment assbd storm1))))
                    (is (= assignment2 (deserialize-assignment (.getAssignment assbd storm1))))
                    (.clearStateForStorm assbd storm1)
                    (is (= nil (.getAssignment assbd storm1)))
                    (.keepOrUpdateAssignment assbd storm1 (thriftify-assignment assignment1))
                    (.keepOrUpdateAssignment assbd storm1 (thriftify-assignment assignment2))
                    (is (= assignment2 (deserialize-assignment (.getAssignment assbd storm1)))))))

(deftest test-local-id-info
  (with-local-tmp [dir1]
                  (let [[name1 id1] ["name1" "id1"]
                        [name2 id2] ["name2" "id2"]
                        [name3 id3] ["name3" "id3"]
                        conf (read-storm-config)
                        assbd (doto (LocalAssignmentsBackendFactory/getBackend conf)
                                              (.prepare conf))]
                    (is (= nil (.getStormId assbd "name3")))
                    (.keepStormId assbd name1 id1)
                    (.keepStormId assbd name2 id2)
                    (is (= id1 (.getStormId assbd name1)))
                    (is (= id2 (.getStormId assbd name2)))
                    (.deleteStormId assbd name1)
                    (is (= nil (.getStormId assbd name1)))
                    (.clearStateForStorm assbd id1)
                    (is (= nil (.getStormId assbd name2)))
                    (.keepStormId assbd name1 id1)
                    (.keepStormId assbd name1 id3)
                    (is (= id3 (.getStormId assbd name1))))))

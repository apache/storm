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
(ns org.apache.storm.daemon.local-executor
  (:use [org.apache.storm util config log])
  (:import [org.apache.storm.tuple AddressedTuple]
           [org.apache.storm.executor Executor ExecutorData ExecutorTransfer])
  (:import [org.apache.storm.utils DisruptorQueue])
  (:import [org.apache.storm Config Constants]))

(defn local-transfer-executor-tuple []
  (fn [task tuple batch-transfer->worker]
    (let [val (AddressedTuple. task tuple)]
      (.publish ^DisruptorQueue batch-transfer->worker val))))

(defn mk-local-executor-transfer [worker-topology-context batch-queue storm-conf transfer-fn]
  (proxy [ExecutorTransfer] [worker-topology-context batch-queue storm-conf transfer-fn]
    (transfer [task tuple]
      (let [batch-transfer->worker (.getBatchTransferQueue this)]
        ((local-transfer-executor-tuple) task tuple batch-transfer->worker)))))

(defn mk-local-executor [workerData executorId credentials]
  (let [executor (Executor/mkExecutor workerData executorId credentials)
        executor-data (.getExecutorData executor)
        worker-topology-context (.getWorkerTopologyContext executor-data)
        batch-transfer-queue (.getBatchTransferWorkerQueue executor-data)
        storm-conf (.getStormConf executor-data)
        transfer-fn (.getTransferFn executor-data)
        local-executor-transfer (mk-local-executor-transfer worker-topology-context batch-transfer-queue storm-conf transfer-fn)]
    (.setLocalExecutorTransfer executor-data local-executor-transfer)
    (.execute executor)))
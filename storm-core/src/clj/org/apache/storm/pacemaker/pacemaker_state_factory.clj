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

(ns org.apache.storm.pacemaker.pacemaker-state-factory
  (:require [org.apache.storm.pacemaker pacemaker]
            [org.apache.storm.cluster-state [zookeeper-state-factory :as zk-factory]]
            [org.apache.storm
             [config :refer :all]
             [cluster :refer :all]
             [log :refer :all]
             [timer :refer :all]
             [converter :refer :all]
             [util :as util]])
  (:import [org.apache.storm.generated
            HBExecutionException HBNodes HBRecords
            HBServerMessageType HBMessage HBMessageData HBPulse
            ClusterWorkerHeartbeat]
           [org.apache.storm.cluster_state zookeeper_state_factory]
           [org.apache.storm.cluster ClusterState]
           [org.apache.storm.utils Utils Time]
           [org.apache.storm.pacemaker PacemakerClient])
  (:gen-class
    :implements [org.apache.storm.cluster.ClusterStateFactory]))



(defn- maybe-deserialize
  [ser clazz]
  (when ser
    (Utils/deserialize ser clazz)))

(defn clojurify-details [details]
  (if details
    (clojurify-zk-worker-hb (maybe-deserialize details ClusterWorkerHeartbeat))))

(defn get-wk-hb-time-secs-pair [details-set]
  (into []
    (for [details details-set]
      (let [_ (log-debug "details" details)
            wk-hb (if details
                    (clojurify-details details))
            time-secs (:time-secs wk-hb)]
        [time-secs details]))))

;; Method defined to aid mocking for makeClientPool
(defn get-clients [conf servers]
  (into []
    (for [host servers]
      (PacemakerClient. conf host))))

;; So we can mock the client for testing
(defn makeClientPool [conf]
  (let [servers (conf PACEMAKER-SERVERS)]
    (get-clients conf servers)))

(defn makeZKState [conf auth-conf acls context]
  (.mkState (zookeeper_state_factory.) conf auth-conf acls context))

(def max-retries 10)

(defn retry-on-exception
  "Retries specific function on exception based on retries count"
  [retries task-description f & args]
  (let [res (try {:value (apply f args)}
              (catch Exception e
                (if (<= 0 retries)
                  (throw e)
                  {:exception e})))]
    (if (:exception res)
      (do
        (log-error (:exception res) (str "Failed to " task-description ". Will make [" retries "] more attempts."))
        (recur (dec retries) task-description f args))
      (do
        (log-debug (str "Successful " task-description "."))
        (:value res)))))

(defn- delete-worker-hb  [path pacemaker-client]
  (retry-on-exception
    max-retries
    "delete-worker-hb"
    #(let [response
           (.send pacemaker-client
                  (HBMessage. HBServerMessageType/DELETE_PATH
                              (HBMessageData/path path)))]
       (if (= (.get_type response) HBServerMessageType/DELETE_PATH_RESPONSE)
         :ok
         (throw (HBExecutionException. "Invalid Response Type"))))))

(defn- get-worker-hb [path pacemaker-client]
  (let [response (.send pacemaker-client
                        (HBMessage. HBServerMessageType/GET_PULSE
                                    (HBMessageData/path path)))]
    (if (= (.get_type response) HBServerMessageType/GET_PULSE_RESPONSE)
      (try
        (.get_details (.get_pulse (.get_data response)))
        (catch Exception e
          (throw (HBExecutionException. (.toString e)))))
      (throw (HBExecutionException. "Invalid Response Type")))))

(defn- get-worker-hb-children [path pacemaker-client]
  (let [response (.send pacemaker-client
                        (HBMessage. HBServerMessageType/GET_ALL_NODES_FOR_PATH
                                    (HBMessageData/path path)))]
    (if (= (.get_type response) HBServerMessageType/GET_ALL_NODES_FOR_PATH_RESPONSE)
      (try
        (into [] (.get_pulseIds (.get_nodes (.get_data response))))
        (catch Exception e
          (throw (HBExecutionException. (.toString e)))))
      (throw (HBExecutionException. "Invalid Response Type")))))

(defn delete-stale-heartbeats [pacemaker-client-pool]
  (retry-on-exception
    max-retries
    "delete-stale-hearbeats"
    #(doseq [pacemaker-client pacemaker-client-pool]
       (if-not (nil? pacemaker-client)
         (let [storm-ids (get-worker-hb-children WORKERBEATS-SUBTREE pacemaker-client)]
           (doseq [id storm-ids]
             (let[hb-paths (get-worker-hb-children
                             (str WORKERBEATS-SUBTREE Utils/FILE_PATH_SEPARATOR id) pacemaker-client)
                  _ (log-debug "hb-paths" hb-paths)]
               (doseq [path hb-paths]
                 (let [wk-hb (clojurify-details (get-worker-hb (str WORKERBEATS-SUBTREE
                                                                    Utils/FILE_PATH_SEPARATOR id
                                                                    Utils/FILE_PATH_SEPARATOR path)
                                                               pacemaker-client))]
                   (when (and wk-hb (> 600 (- (Time/currentTimeSecs) (:time-secs wk-hb))))
                     (log-debug "deleting wk-hb" wk-hb "for path" path)
                     (delete-worker-hb path pacemaker-client)))))))))))

(defn launch-cleanup-hb-thread [conf pacemaker-client-pool]
  (let [timer (mk-timer :kill-fn (fn [t]
                                   (log-error t "Error when processing event")
                                   (Utils/exitProcess 20 "Error when processing an event")
                                   ))]
    (schedule-recurring timer
                        30
                        300
                        (fn []
                          (delete-stale-heartbeats pacemaker-client-pool)))))

(defn try-reconnect [pacemaker-client]
  (try
    (.reconnect pacemaker-client)
    (catch Exception e
      (log-error "Error reconnecting to client" e))))

(defn -mkState [this conf auth-conf acls context]
  (let [zk-state (makeZKState conf auth-conf acls context)
        pacemaker-client-pool (makeClientPool conf)
        _ (launch-cleanup-hb-thread conf pacemaker-client-pool)
        servers (conf PACEMAKER-SERVERS)
        num-servers (count servers)]

    (reify
      ClusterState
      ;; Let these pass through to the zk-state. We only want to handle heartbeats.
      (register [this callback] (.register zk-state callback))
      (unregister [this callback] (.unregister zk-state callback))
      (set_ephemeral_node [this path data acls] (.set_ephemeral_node zk-state path data acls))
      (create_sequential [this path data acls] (.create_sequential zk-state path data acls))
      (set_data [this path data acls] (.set_data zk-state path data acls))
      (delete_node [this path] (.delete_node zk-state path))
      (get_data [this path watch?] (.get_data zk-state path watch?))
      (get_data_with_version [this path watch?] (.get_data_with_version zk-state path watch?))
      (get_version [this path watch?] (.get_version zk-state path watch?))
      (get_children [this path watch?] (.get_children zk-state path watch?))
      (mkdirs [this path acls] (.mkdirs zk-state path acls))
      (node_exists [this path watch?] (.node_exists zk-state path watch?))

      (set_worker_hb [this path data acls]
        (retry-on-exception
          max-retries
          "set_worker_hb"
          ;; connecting to a single server, might want to
          ;; remove once reimplemented
          #(let [response
                 (.send (nth pacemaker-client-pool 0)
                   (HBMessage. HBServerMessageType/SEND_PULSE
                               (HBMessageData/pulse
                                 (doto (HBPulse.)
                                       (.set_id path)
                                       (.set_details data)))))]
             (if (= (.get_type response) HBServerMessageType/SEND_PULSE_RESPONSE)
               :ok
               (throw (HBExecutionException. "Invalid Response Type"))))))

      (delete_worker_hb [this path]
        ;; connecting to a single server, might want to
        ;; remove once reimplemented
        (delete-worker-hb path (nth pacemaker-client-pool 0)))

      ;; aggregating worker heartbeat details
      (get_worker_hb [this path watch?]
        (retry-on-exception
          max-retries
          "get-worker-hb"
          #(let [details-set (loop [count 0
                                    details #{}]
                               (if (>= count num-servers)
                                 (remove nil? details)
                                 (let [pacemaker-client (nth pacemaker-client-pool count)
                                       result (try
                                                (get-worker-hb path pacemaker-client)
                                                (catch Exception e
                                                  (do
                                                    (log-error e "Error getting worker heartbeat")
                                                    (try-reconnect pacemaker-client))))
                                       _ (log-debug "result" result)]
                                   (recur (inc count)
                                     (conj details result)))))
                 _ (log-debug "details set" details-set)
                 _ (if (empty? details-set) (throw (HBExecutionException.
                                                     "Cannot connect to any pacemaker servers")))
                 wk-hb-time-secs-pair (get-wk-hb-time-secs-pair details-set)
                 sorted-map (into {} (sort-by first wk-hb-time-secs-pair))
                 _ (log-debug "sorted map" sorted-map)]
             (last (vals sorted-map)))))

      ;; aggregating worker heartbeat children across all servers
      (get_worker_hb_children [this path watch?]
        (retry-on-exception
          max-retries
          "get_worker_hb_children"
          #(let [hb-paths (loop [count 0
                                 details []]
                            (if (>= count num-servers)
                              (remove nil? details)
                              (let [pacemaker-client (nth pacemaker-client-pool count)
                                    result (try
                                             (get-worker-hb-children path pacemaker-client)
                                             (catch Exception e
                                               (do
                                                 (log-error e "Error getting worker heartbeat children")
                                                 (try-reconnect pacemaker-client))))]
                                (recur (inc count)
                                  (conj details result)))))
                 _ (if (empty? hb-paths) (throw (HBExecutionException.
                                                  "Cannot connect to any pacemaker servers")))]
             (into[]
               (set (flatten hb-paths))))))

      (close [this]
        (.close zk-state)
        (for [pacemaker-client pacemaker-client-pool]
          (.close pacemaker-client))))))

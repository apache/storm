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
             [converter :as converter]
             [cluster :refer :all]
             [log :refer :all]
             [util :as util]])
  (:import [org.apache.storm.generated
            HBExecutionException HBServerMessageType HBMessage
            HBMessageData HBPulse ClusterWorkerHeartbeat]
           [org.apache.storm.cluster_state zookeeper_state_factory]
           [org.apache.storm.cluster ClusterState ZKStateStorage]
           [org.apache.storm.utils Utils]
           [org.apache.storm.pacemaker PacemakerClient])
  (:gen-class
   :implements [org.apache.storm.cluster.ClusterStateFactory]))

(defn- maybe-deserialize
  [ser clazz]
  (when ser
    (Utils/deserialize ser clazz)))

(defn clojurify-details [details]
  (if details
    (converter/clojurify-zk-worker-hb (maybe-deserialize details ClusterWorkerHeartbeat))))

(defn get-wk-hb-time-secs-pair [details-set]
  (for [details details-set]
    (let [_ (log-debug "details" details)
          wk-hb (if details
                  (clojurify-details details))
          time-secs (:time-secs wk-hb)]
      [time-secs details])))

(defn makeZKState [conf auth-conf acls context]
  (.mkState (zookeeper_state_factory.) conf auth-conf acls context))

(defn -mkStore [this conf auth-conf, acls, context]
  ;; This is only used by the supervisor (this does not support pacemaker)!!!
  (ZKStateStorage. conf, auth-conf, acls, context))

(def max-retries 10)

(defn- delete-worker-hb  [path pacemaker-client]
  (let [response
        (.send pacemaker-client
               (HBMessage. HBServerMessageType/DELETE_PATH
                           (HBMessageData/path path)))]
    (if (= (.get_type response) HBServerMessageType/DELETE_PATH_RESPONSE)
      :ok
      (throw (HBExecutionException. "Invalid Response Type")))))

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

(defn shuffle-servers-list [conf]
  (shuffle (or
            (conf PACEMAKER-SERVERS)
            [(conf PACEMAKER-HOST)])))

(defn shutdown-rotate [servers client-pool]
  ; Shutdown the client and remove from the pool
  (when @client-pool
    (.shutdown (get @client-pool (first @servers)))
    (swap! client-pool dissoc (first @servers))
  ; Rotate server list to try another write client
    (swap! servers (fn [lst] (concat (rest lst) [(first lst)])))))

(defn get-pacemaker-write-client [conf servers client-pool]
  ;; Client should be created in case of an exception or first write call
  ;; Shutdown happens in the retry loop
  (try
    (let [client (get @client-pool (first @servers))]
      (if (nil? client)
        (do
          (swap! client-pool merge {(first @servers) (PacemakerClient. conf (first @servers))})
          (get @client-pool (first @servers)))
        client))
    (catch Exception e
      (throw e))))

;; So we can mock the client for testing
(defn makeClientPool [conf client-pool servers]
  (let [servers @servers
        current-pool @client-pool]
    (swap! client-pool merge
           (into {}
             (map #(if (get current-pool %)
                     nil
                     [% (PacemakerClient. conf %)])
               servers)))
    client-pool))

;; Used for mocking in tests
(defn is-connection-ready [pacemaker-client]
  (.isReady pacemaker-client))

(defn pacemaker-retry-on-exception
  "Retries specific function on exception based on retries count"
  [tries task-description f catchfn]
  (let [res (try {:value (f)}
              (catch Exception e
                (do
                  (if catchfn
                    (catchfn e))
                  (if (= 0 tries)
                    (throw e)
                    {:exception e}))))]
    (if (:exception res)
      (do
        (log-error
          (:exception res)
          (str "Failed to " task-description ". Will make [" tries "] more attempts."))
        (recur (dec tries) task-description f catchfn))
      (do
        (log-debug (str "Successful " task-description "."))
        (:value res)))))

(defn -mkState [this conf auth-conf acls context]
  (let [zk-state (makeZKState conf auth-conf acls context)
        pacemaker-client-pool (atom {})
        ;; We shuffle the list to spread clients among servers
        servers (atom (shuffle-servers-list conf))]

    (reify
      ClusterState
      ;; Let these pass through to the zk-state. We only want to handle heartbeats.
      (register [this callback] (.register zk-state callback))
      (unregister [this callback] (.unregister zk-state callback))
      (set_ephemeral_node [this path data acls] (.set_ephemeral_node zk-state path data acls))
      (create_sequential [this path data acls] (.create_sequential zk-state path data acls))
      (set_data [this path data acls] (.set_data zk-state path data acls))
      (delete_node [this path] (.delete_node zk-state path))
      (delete_node_blobstore [this path nimbus-host-port-info] (.delete_node_blobstore zk-state path nimbus-host-port-info))
      (get_data [this path watch?] (.get_data zk-state path watch?))
      (get_data_with_version [this path watch?] (.get_data_with_version zk-state path watch?))
      (get_version [this path watch?] (.get_version zk-state path watch?))
      (get_children [this path watch?] (.get_children zk-state path watch?))
      (mkdirs [this path acls] (.mkdirs zk-state path acls))
      (node_exists [this path watch?] (.node_exists zk-state path watch?))
      (add_listener [this listener] (.add_listener zk-state listener))
      (sync_path [this path] (.sync_path zk-state path))
      
      (set_worker_hb [this path data acls]
        (pacemaker-retry-on-exception
          max-retries
          "set_worker_hb"
          #(let [response
                 (.send (get-pacemaker-write-client conf servers pacemaker-client-pool)
                        (HBMessage. HBServerMessageType/SEND_PULSE
                                    (HBMessageData/pulse
                                      (doto (HBPulse.)
                                            (.set_id path)
                                            (.set_details data)))))]
             (if (= (.get_type response) HBServerMessageType/SEND_PULSE_RESPONSE)
               :ok
               (throw (HBExecutionException. "Invalid Response Type"))))
          (fn set_worker_hb_error [err]
            (shutdown-rotate servers pacemaker-client-pool))))

      (delete_worker_hb [this path]
        (pacemaker-retry-on-exception
          max-retries
          "delete-worker-hb"
          #(let [pacemaker-client-pool (makeClientPool conf pacemaker-client-pool servers)
                 results (map (fn [[host client]]
                                (try
                                  (if (is-connection-ready client)
                                    (delete-worker-hb path client)
                                    :error)
                                  (catch Exception e
                                    :error)))
                              @pacemaker-client-pool)]
             (when (every? (fn [result] (= :error result)) results)
               (throw (HBExecutionException. "Cannot connect to any pacemaker servers"))))
          nil))

      ;; aggregating worker heartbeat details
      (get_worker_hb [this path watch?]
        (pacemaker-retry-on-exception
          max-retries
          "get-worker-hb"
          #(let [pacemaker-client-pool (makeClientPool conf pacemaker-client-pool servers)
                 results (map (fn [[host client]]
                                (try
                                  (if (is-connection-ready client)
                                    (get-worker-hb path client)
                                    (do
                                      (log-error (HBExecutionException.) "Connection not ready for host " host client)
                                      :error))
                                  (catch Exception e
                                    (do
                                      (log-error e "Error getting worker heartbeat for host " host client)
                                      :error))))
                              @pacemaker-client-pool)]
             (if (every? (fn [result] (= :error result)) results)
               (throw (HBExecutionException. "Cannot connect to any pacemaker servers"))
               (->> results
                    ; Filter the results and clojurify
                    (filter (fn [result] (not (or (nil? result) (= :error result)))))
                    (get-wk-hb-time-secs-pair)
                    (sort-by first)
                    (into {})
                    (vals)
                    (last))))
          nil))

      ;; aggregating worker heartbeat children across all servers
      (get_worker_hb_children [this path watch?]
        (pacemaker-retry-on-exception
          max-retries
          "get_worker_hb_children"
          #(let [pacemaker-client-pool (makeClientPool conf pacemaker-client-pool servers)
                 results (map (fn [[host client]]
                                (try
                                  (if (is-connection-ready client)
                                    (get-worker-hb-children path client)
                                    (do
                                      (log-error (HBExecutionException.) "Connection not ready for host " host client)
                                      :error))
                                  (catch Exception e
                                    (do
                                      (log-error e str "Error getting worker heartbeat children for host " host client)
                                      :error))))
                              @pacemaker-client-pool)]
             ;; If all connections are throwing exceptions or not ready we throw exception up the stack
             (if (every? (fn [result] (= :error result)) results)
               (throw (HBExecutionException. "Cannot connect to any pacemaker servers"))
               (into [] (->> results
                             ; Filter the results and clojurify
                             (filter (fn [result] (not (or (nil? result) (= :error result)))))
                             (flatten)
                             (set)))))
          nil))
      
      (close [this]
        (.close zk-state)
        (doseq [[host pacemaker-client] @pacemaker-client-pool]
          (.close pacemaker-client))))))

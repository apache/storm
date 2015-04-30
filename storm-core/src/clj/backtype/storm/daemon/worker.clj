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
(ns backtype.storm.daemon.worker
  (:use [backtype.storm.daemon common]
        [backtype.storm local-state])
  (:require [backtype.storm.daemon [executor :as executor]]
            [backtype.storm [disruptor :as disruptor] [cluster :as cluster]]
            [clojure.set :as set]
            [backtype.storm.messaging.loader :as msg-loader]
            [backtype.storm.log :refer [log-message log-warn log-error]]
            [backtype.storm.timer :as t]
            [backtype.storm.util :as util :refer [defnk]]
            [backtype.storm.config :as c])
  (:import [java.util.concurrent Executors]
           [java.util ArrayList HashMap]
           [backtype.storm.utils Utils TransferDrainer ThriftTopologyUtils]
           [backtype.storm.messaging TransportFactory]
           [backtype.storm.messaging TaskMessage IContext IConnection ConnectionWithStatus ConnectionWithStatus$Status]
           [backtype.storm.daemon Shutdownable]
           [backtype.storm.serialization KryoTupleSerializer]
           [backtype.storm.generated StormTopology]
           [backtype.storm.tuple Fields]
           [backtype.storm.task WorkerTopologyContext]
           [backtype.storm Constants]
           [backtype.storm.security.auth AuthUtils]
           [javax.security.auth Subject]
           [java.security PrivilegedExceptionAction])
  (:gen-class))

(defmulti mk-suicide-fn c/cluster-mode)

(defn read-worker-executors [storm-conf storm-cluster-state storm-id assignment-id port assignment-versions]
  (log-message "Reading Assignments.")
  (let [assignment (:executor->node+port (.assignment-info storm-cluster-state storm-id nil))]
    (doall
     (concat
      [Constants/SYSTEM_EXECUTOR_ID]
      (mapcat (fn [[executor loc]]
                (if (= loc [assignment-id port])
                  [executor]
                  ))
              assignment)))))

(defnk do-executor-heartbeats [worker :executors nil]
  ;; stats is how we know what executors are assigned to this worker
  (let [stats (if-not executors
                  (into {} (map (fn [e] {e nil}) (:executors worker)))
                  (->> executors
                    (map (fn [e] {(executor/get-executor-id e) (executor/render-stats e)}))
                    (apply merge)))
        zk-hb {:storm-id       (:storm-id worker)
               :executor-stats stats
               :uptime         ((:uptime worker))
               :time-secs      (util/current-time-secs)
               }]
    ;; do the zookeeper heartbeat
    (.worker-heartbeat! (:storm-cluster-state worker) (:storm-id worker) (:assignment-id worker) (:port worker) zk-hb)
    ))

(defn do-heartbeat [worker]
  (let [conf (:conf worker)
        state (c/worker-state conf (:worker-id worker))]
    ;; do the local-file-system heartbeat.
    (ls-worker-heartbeat! state (util/current-time-secs) (:storm-id worker) (:executors worker) (:port worker))
    (.cleanup state 60) ; this is just in case supervisor is down so that disk doesn't fill up.
                         ; it shouldn't take supervisor 120 seconds between listing dir and reading it

    ))

(defn worker-outbound-tasks
  "Returns seq of task-ids that receive messages from this worker"
  [worker]
  (let [context (worker-context worker)
        components (mapcat
                     (fn [task-id]
                       (->> (.getComponentId context (int task-id))
                            (.getTargets context)
                            vals
                            (map keys)
                            (apply concat)))
                     (:task-ids worker))]
    (-> worker
        :task->component
        util/reverse-map
        (select-keys components)
        vals
        flatten
        set )))

(defn mk-transfer-local-fn [worker]
  (let [short-executor-receive-queue-map (:short-executor-receive-queue-map worker)
        task->short-executor (:task->short-executor worker)
        task-getter (comp #(get task->short-executor %) util/fast-first)]
    (fn [tuple-batch]
      (let [grouped (util/fast-group-by task-getter tuple-batch)]
        (util/fast-map-iter [[short-executor pairs] grouped]
          (let [q (short-executor-receive-queue-map short-executor)]
            (if q
              (disruptor/publish q pairs)
              (log-warn "Received invalid messages for unknown tasks. Dropping... ")
              )))))))

(defn- assert-can-serialize [^KryoTupleSerializer serializer tuple-batch]
  "Check that all of the tuples can be serialized by serializing them."
  (util/fast-list-iter [[task tuple :as pair] tuple-batch]
    (.serialize serializer tuple)))

(defn mk-transfer-fn [worker]
  (let [local-tasks (-> worker :task-ids set)
        local-transfer (:transfer-local-fn worker)
        ^DisruptorQueue transfer-queue (:transfer-queue worker)
        task->node+port (:cached-task->node+port worker)
        try-serialize-local ((:storm-conf worker) c/TOPOLOGY-TESTING-ALWAYS-TRY-SERIALIZE)
        transfer-fn
          (fn [^KryoTupleSerializer serializer tuple-batch]
            (let [local (ArrayList.)
                  remoteMap (HashMap.)]
              (util/fast-list-iter [[task tuple :as pair] tuple-batch]
                (if (local-tasks task)
                  (.add local pair)

                  ;;Using java objects directly to avoid performance issues in java code
                  (let [node+port (get @task->node+port task)]
                    (when (not (.get remoteMap node+port))
                      (.put remoteMap node+port (ArrayList.)))
                    (let [remote (.get remoteMap node+port)]
                      (.add remote (TaskMessage. task (.serialize serializer tuple)))
                     ))))
                (local-transfer local)
                (disruptor/publish transfer-queue remoteMap)
              ))]
    (if try-serialize-local
      (do
        (log-warn "WILL TRY TO SERIALIZE ALL TUPLES (Turn off " c/TOPOLOGY-TESTING-ALWAYS-TRY-SERIALIZE " for production)")
        (fn [^KryoTupleSerializer serializer tuple-batch]
          (assert-can-serialize serializer tuple-batch)
          (transfer-fn serializer tuple-batch)))
      transfer-fn)))

(defn- mk-receive-queue-map [storm-conf executors]
  (->> executors
       ;; TODO: this depends on the type of executor
       (map (fn [e] [e (disruptor/disruptor-queue (str "receive-queue" e)
                                                  (storm-conf c/TOPOLOGY-EXECUTOR-RECEIVE-BUFFER-SIZE)
                                                  :wait-strategy (storm-conf c/TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))]))
       (into {})
       ))

(defn- stream->fields [^StormTopology topology component]
  (->> (ThriftTopologyUtils/getComponentCommon topology component)
       .get_streams
       (map (fn [[s info]] [s (Fields. (.get_output_fields info))]))
       (into {})
       (HashMap.)))

(defn component->stream->fields [^StormTopology topology]
  (->> (ThriftTopologyUtils/getComponentIds topology)
       (map (fn [c] [c (stream->fields topology c)]))
       (into {})
       (HashMap.)))

(defn- mk-default-resources [worker]
  (let [conf (:conf worker)
        thread-pool-size (int (conf c/TOPOLOGY-WORKER-SHARED-THREAD-POOL-SIZE))]
    {WorkerTopologyContext/SHARED_EXECUTOR (Executors/newFixedThreadPool thread-pool-size)}
    ))

(defn- mk-user-resources [worker]
  ;;TODO: need to invoke a hook provided by the topology, giving it a chance to create user resources.
  ;; this would be part of the initialization hook
  ;; need to separate workertopologycontext into WorkerContext and WorkerUserContext.
  ;; actually just do it via interfaces. just need to make sure to hide setResource from tasks
  {})

(defn mk-halting-timer [timer-name]
  (t/mk-timer :kill-fn (fn [t]
                       (log-error t "Error when processing event")
                       (util/exit-process! 20 "Error when processing an event")
                       )
            :timer-name timer-name))

(defn worker-data [conf mq-context storm-id assignment-id port worker-id storm-conf cluster-state storm-cluster-state]
  (let [assignment-versions (atom {})
        executors (set (read-worker-executors storm-conf storm-cluster-state storm-id assignment-id port assignment-versions))
        transfer-queue (disruptor/disruptor-queue "worker-transfer-queue" (storm-conf c/TOPOLOGY-TRANSFER-BUFFER-SIZE)
                                                  :wait-strategy (storm-conf c/TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))
        executor-receive-queue-map (mk-receive-queue-map storm-conf executors)

        receive-queue-map (->> executor-receive-queue-map
                               (mapcat (fn [[e queue]] (for [t (executor-id->tasks e)] [t queue])))
                               (into {}))

        topology (c/read-supervisor-topology conf storm-id)
        mq-context  (if mq-context
                      mq-context
                      (TransportFactory/makeContext storm-conf))]

    (util/recursive-map
      :conf conf
      :mq-context mq-context
      :receiver (.bind ^IContext mq-context storm-id port)
      :storm-id storm-id
      :assignment-id assignment-id
      :port port
      :worker-id worker-id
      :cluster-state cluster-state
      :storm-cluster-state storm-cluster-state
      ;; when worker bootup, worker will start to setup initial connections to
      ;; other workers. When all connection is ready, we will enable this flag
      ;; and spout and bolt will be activated.
      :worker-active-flag (atom false)
      :storm-active-atom (atom false)
      :executors executors
      :task-ids (->> receive-queue-map keys (map int) sort)
      :storm-conf storm-conf
      :topology topology
      :system-topology (system-topology! storm-conf topology)
      :heartbeat-timer (mk-halting-timer "heartbeat-timer")
      :refresh-connections-timer (mk-halting-timer "refresh-connections-timer")
      :refresh-credentials-timer (mk-halting-timer "refresh-credentials-timer")
      :refresh-active-timer (mk-halting-timer "refresh-active-timer")
      :executor-heartbeat-timer (mk-halting-timer "executor-heartbeat-timer")
      :user-timer (mk-halting-timer "user-timer")
      :task->component (HashMap. (storm-task-info topology storm-conf)) ; for optimized access when used in tasks later on
      :component->stream->fields (component->stream->fields (:system-topology <>))
      :component->sorted-tasks (->> (:task->component <>) util/reverse-map (util/map-val sort))
      :endpoint-socket-lock (util/mk-rw-lock)
      :cached-node+port->socket (atom {})
      :cached-task->node+port (atom {})
      :transfer-queue transfer-queue
      :executor-receive-queue-map executor-receive-queue-map
      :short-executor-receive-queue-map (util/map-key first executor-receive-queue-map)
      :task->short-executor (->> executors
                                 (mapcat (fn [e] (for [t (executor-id->tasks e)] [t (first e)])))
                                 (into {})
                                 (HashMap.))
      :suicide-fn (mk-suicide-fn conf)
      :uptime (util/uptime-computer)
      :default-shared-resources (mk-default-resources <>)
      :user-shared-resources (mk-user-resources <>)
      :transfer-local-fn (mk-transfer-local-fn <>)
      :receiver-thread-count (get storm-conf c/WORKER-RECEIVER-THREAD-COUNT)
      :transfer-fn (mk-transfer-fn <>)
      :assignment-versions assignment-versions
      )))

(defn- endpoint->string [[node port]]
  (str port "/" node))

(defn string->endpoint [^String s]
  (let [[port-str node] (.split s "/" 2)]
    [node (Integer/valueOf port-str)]
    ))

(defn mk-refresh-connections [worker]
  (let [outbound-tasks (worker-outbound-tasks worker)
        conf (:conf worker)
        storm-cluster-state (:storm-cluster-state worker)
        storm-id (:storm-id worker)]
    (fn this
      ([]
        (this (fn [& ignored] (t/schedule (:refresh-connections-timer worker) 0 this))))
      ([callback]
         (let [version (.assignment-version storm-cluster-state storm-id callback)
               assignment (if (= version (:version (get @(:assignment-versions worker) storm-id)))
                            (:data (get @(:assignment-versions worker) storm-id))
                            (let [new-assignment (.assignment-info-with-version storm-cluster-state storm-id callback)]
                              (swap! (:assignment-versions worker) assoc storm-id new-assignment)
                              (:data new-assignment)))
              my-assignment (-> assignment
                                :executor->node+port
                                to-task->node+port
                                (select-keys outbound-tasks)
                                (#(util/map-val endpoint->string %)))
              ;; we dont need a connection for the local tasks anymore
              needed-assignment (->> my-assignment
                                      (util/filter-key (complement (-> worker :task-ids set))))
              needed-connections (-> needed-assignment vals set)
              needed-tasks (-> needed-assignment keys)

              current-connections (set (keys @(:cached-node+port->socket worker)))
              new-connections (set/difference needed-connections current-connections)
              remove-connections (set/difference current-connections needed-connections)]
              (swap! (:cached-node+port->socket worker)
                     #(HashMap. (merge (into {} %1) %2))
                     (into {}
                       (util/dofor [endpoint-str new-connections
                               :let [[node port] (string->endpoint endpoint-str)]]
                         [endpoint-str
                          (.connect
                           ^IContext (:mq-context worker)
                           storm-id
                           ((:node->host assignment) node)
                           port)
                          ]
                         )))
              (util/write-locked (:endpoint-socket-lock worker)
                (reset! (:cached-task->node+port worker)
                        (HashMap. my-assignment)))
              (doseq [endpoint remove-connections]
                (.close (get @(:cached-node+port->socket worker) endpoint)))
              (apply swap!
                     (:cached-node+port->socket worker)
                     #(HashMap. (apply dissoc (into {} %1) %&))
                     remove-connections)

           )))))

(defn refresh-storm-active
  ([worker]
    (refresh-storm-active worker (fn [& ignored] (t/schedule (:refresh-active-timer worker) 0 (partial refresh-storm-active worker)))))
  ([worker callback]
    (let [base (.storm-base (:storm-cluster-state worker) (:storm-id worker) callback)]
     (reset!
      (:storm-active-atom worker)
       (and (= :active (-> base :status :type)) @(:worker-active-flag worker))
      ))
     ))

;; TODO: consider having a max batch size besides what disruptor does automagically to prevent latency issues
(defn mk-transfer-tuples-handler [worker]
  (let [^DisruptorQueue transfer-queue (:transfer-queue worker)
        drainer (TransferDrainer.)
        node+port->socket (:cached-node+port->socket worker)
        task->node+port (:cached-task->node+port worker)
        endpoint-socket-lock (:endpoint-socket-lock worker)
        ]
    (disruptor/clojure-handler
      (fn [packets _ batch-end?]
        (.add drainer packets)

        (when batch-end?
          (util/read-locked endpoint-socket-lock
            (let [node+port->socket @node+port->socket]
              (.send drainer node+port->socket)))
          (.clear drainer))))))

;; Check whether this messaging connection is ready to send data
(defn is-connection-ready [^IConnection connection]
  (if (instance?  ConnectionWithStatus connection)
    (let [^ConnectionWithStatus connection connection
          status (.status connection)]
      (= status ConnectionWithStatus$Status/Ready))
    true))

;; all connections are ready
(defn all-connections-ready [worker]
    (let [connections (vals @(:cached-node+port->socket worker))]
      (every? is-connection-ready connections)))

;; we will wait all connections to be ready and then activate the spout/bolt
;; when the worker bootup
(defn activate-worker-when-all-connections-ready
  [worker]
  (let [timer (:refresh-active-timer worker)
        delay-secs 0
        recur-secs 1]
    (t/schedule timer
      delay-secs
      (fn this []
        (if (all-connections-ready worker)
          (do
            (log-message "All connections are ready for worker " (:assignment-id worker) ":" (:port worker)
              " with id "(:worker-id worker))
            (reset! (:worker-active-flag worker) true))
          (t/schedule timer recur-secs this :check-active false)
            )))))

(defn launch-receive-thread [worker]
  (log-message "Launching receive-thread for " (:assignment-id worker) ":" (:port worker))
  (msg-loader/launch-receive-thread!
    (:mq-context worker)
    (:receiver worker)
    (:storm-id worker)
    (:receiver-thread-count worker)
    (:port worker)
    (:transfer-local-fn worker)
    (-> worker :storm-conf (get c/TOPOLOGY-RECEIVER-BUFFER-SIZE))
    :kill-fn (fn [t] (util/exit-process! 11))))

(defn- close-resources [worker]
  (let [dr (:default-shared-resources worker)]
    (log-message "Shutting down default resources")
    (.shutdownNow (get dr WorkerTopologyContext/SHARED_EXECUTOR))
    (log-message "Shut down default resources")))

(defn- override-login-config-with-system-property [conf]
  (if-let [login_conf_file (System/getProperty "java.security.auth.login.config")]
    (assoc conf "java.security.auth.login.config" login_conf_file)
    conf))

;; TODO: should worker even take the storm-id as input? this should be
;; deducable from cluster state (by searching through assignments)
;; what about if there's inconsistency in assignments? -> but nimbus
;; should guarantee this consistency
(defserverfn mk-worker [conf shared-mq-context storm-id assignment-id port worker-id]
  (log-message "Launching worker for " storm-id " on " assignment-id ":" port " with id " worker-id
               " and conf " conf)
  (if-not (c/local-mode? conf)
    (util/redirect-stdio-to-slf4j!))
  ;; because in local mode, its not a separate
  ;; process. supervisor will register it in this case
  (when (= :distributed (c/cluster-mode conf))
    (util/touch (c/worker-pid-path conf worker-id (util/process-pid))))
  (let [storm-conf (c/read-supervisor-storm-conf conf storm-id)
        storm-conf (override-login-config-with-system-property storm-conf)
        acls (Utils/getWorkerACL storm-conf)
        cluster-state (cluster/mk-distributed-cluster-state conf :auth-conf storm-conf :acls acls)
        storm-cluster-state (cluster/mk-storm-cluster-state cluster-state :acls acls)
        initial-credentials (.credentials storm-cluster-state storm-id nil)
        auto-creds (AuthUtils/GetAutoCredentials storm-conf)
        subject (AuthUtils/populateSubject nil auto-creds initial-credentials)]
      (Subject/doAs subject (reify PrivilegedExceptionAction
        (run [this]
          (let [worker (worker-data conf shared-mq-context storm-id assignment-id port worker-id storm-conf cluster-state storm-cluster-state)
        heartbeat-fn #(do-heartbeat worker)

        ;; do this here so that the worker process dies if this fails
        ;; it's important that worker heartbeat to supervisor ASAP when launching so that the supervisor knows it's running (and can move on)
        _ (heartbeat-fn)

        executors (atom nil)
        ;; launch heartbeat threads immediately so that slow-loading tasks don't cause the worker to timeout
        ;; to the supervisor
        _ (t/schedule-recurring (:heartbeat-timer worker) 0 (conf c/WORKER-HEARTBEAT-FREQUENCY-SECS) heartbeat-fn)
        _ (t/schedule-recurring (:executor-heartbeat-timer worker) 0 (conf c/TASK-HEARTBEAT-FREQUENCY-SECS) #(do-executor-heartbeats worker :executors @executors))

        receive-thread-shutdown (launch-receive-thread worker)

        refresh-connections (mk-refresh-connections worker)

        _ (refresh-connections nil)

        _ (activate-worker-when-all-connections-ready worker)

        _ (refresh-storm-active worker nil)


        _ (reset! executors (util/dofor [e (:executors worker)] (executor/mk-executor worker e initial-credentials)))

        transfer-tuples (mk-transfer-tuples-handler worker)

        transfer-thread (disruptor/consume-loop* (:transfer-queue worker) transfer-tuples)
        shutdown* (fn []
                    (log-message "Shutting down worker " storm-id " " assignment-id " " port)
                    (doseq [[_ socket] @(:cached-node+port->socket worker)]
                      ;; this will do best effort flushing since the linger period
                      ;; was set on creation
                      (.close socket))
                    (log-message "Shutting down receive thread")
                    (receive-thread-shutdown)
                    (log-message "Shut down receive thread")
                    (log-message "Terminating messaging context")
                    (log-message "Shutting down executors")
                    (doseq [executor @executors] (.shutdown executor))
                    (log-message "Shut down executors")

                    ;;this is fine because the only time this is shared is when it's a local context,
                    ;;in which case it's a noop
                    (.term ^IContext (:mq-context worker))
                    (log-message "Shutting down transfer thread")
                    (disruptor/halt-with-interrupt! (:transfer-queue worker))

                    (.interrupt transfer-thread)
                    (.join transfer-thread)
                    (log-message "Shut down transfer thread")
                    (t/cancel-timer (:heartbeat-timer worker))
                    (t/cancel-timer (:refresh-connections-timer worker))
                    (t/cancel-timer (:refresh-credentials-timer worker))
                    (t/cancel-timer (:refresh-active-timer worker))
                    (t/cancel-timer (:executor-heartbeat-timer worker))
                    (t/cancel-timer (:user-timer worker))

                    (close-resources worker)

                    ;; TODO: here need to invoke the "shutdown" method of WorkerHook

                    (.remove-worker-heartbeat! (:storm-cluster-state worker) storm-id assignment-id port)
                    (log-message "Disconnecting from storm cluster state context")
                    (.disconnect (:storm-cluster-state worker))
                    (.close (:cluster-state worker))
                    (log-message "Shut down worker " storm-id " " assignment-id " " port))
        ret (reify
             Shutdownable
             (shutdown
              [this]
              (shutdown*))
             DaemonCommon
             (waiting? [this]
               (and
                 (t/timer-waiting? (:heartbeat-timer worker))
                 (t/timer-waiting? (:refresh-connections-timer worker))
                 (t/timer-waiting? (:refresh-credentials-timer worker))
                 (t/timer-waiting? (:refresh-active-timer worker))
                 (t/timer-waiting? (:executor-heartbeat-timer worker))
                 (t/timer-waiting? (:user-timer worker))
                 ))
             )
        credentials (atom initial-credentials)
        check-credentials-changed (fn []
                                    (let [new-creds (.credentials (:storm-cluster-state worker) storm-id nil)]
                                      (when-not (= new-creds @credentials) ;;This does not have to be atomic, worst case we update when one is not needed
                                        (AuthUtils/updateSubject subject auto-creds new-creds)
                                        (util/dofor [e @executors] (.credentials-changed e new-creds))
                                        (reset! credentials new-creds))))
      ]
    (.credentials (:storm-cluster-state worker) storm-id (fn [args] (check-credentials-changed)))
    (t/schedule-recurring (:refresh-credentials-timer worker) 0 (conf c/TASK-CREDENTIALS-POLL-SECS) check-credentials-changed)
    (t/schedule-recurring (:refresh-connections-timer worker) 0 (conf c/TASK-REFRESH-POLL-SECS) refresh-connections)
    (t/schedule-recurring (:refresh-active-timer worker) 0 (conf c/TASK-REFRESH-POLL-SECS) (partial refresh-storm-active worker))

    (log-message "Worker has topology config " (:storm-conf worker))
    (log-message "Worker " worker-id " for storm " storm-id " on " assignment-id ":" port " has finished loading")
    ret
    ))))))

(defmethod mk-suicide-fn
  :local [conf]
  (fn [] (util/exit-process! 1 "Worker died")))

(defmethod mk-suicide-fn
  :distributed [conf]
  (fn [] (util/exit-process! 1 "Worker died")))

(defn -main [storm-id assignment-id port-str worker-id]
  (let [conf (c/read-storm-config)]
    (util/setup-default-uncaught-exception-handler)
    (validate-distributed-mode! conf)
    (let [worker (mk-worker conf nil storm-id assignment-id (Integer/parseInt port-str) worker-id)]
      (util/add-shutdown-hook-with-force-kill-in-1-sec #(.shutdown worker)))))

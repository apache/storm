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
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:require [backtype.storm.daemon [executor :as executor]])
  (:import [java.util.concurrent Executors])
  (:import [java.util ArrayList HashMap])
  (:import [backtype.storm.utils TransferDrainer])
  (:import [backtype.storm.messaging TransportFactory])
  (:import [backtype.storm.messaging TaskMessage IContext IConnection])
  (:import [backtype.storm.security.auth AuthUtils])
  (:import [javax.security.auth Subject])
  (:import [java.security PrivilegedExceptionAction])
  (:gen-class))

(bootstrap)

(defmulti mk-suicide-fn cluster-mode)

(defn read-worker-executors [assignment-info assignment-id port]
  (log-message "Reading Assignments.")
  (let [assignment (:executor->node+port assignment-info)]
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
                  (into {} (map (fn [e] {e nil}) @(:executors worker)))
                  (->> executors
                    (map (fn [e] {(executor/get-executor-id e) (executor/render-stats e)}))
                    (apply merge)))
        zk-hb {:storm-id (:storm-id worker)
               :executor-stats stats
               :uptime ((:uptime worker))
               :time-secs (current-time-secs)
               }]
    ;; do the zookeeper heartbeat
    (.worker-heartbeat! (:storm-cluster-state worker) (:storm-id worker) (:assignment-id worker) (:port worker) zk-hb)    
    ))

(defn do-heartbeat [worker]
  (let [conf (:conf worker)
        hb (WorkerHeartbeat.
             (current-time-secs)
             (:storm-id worker)
             @(:executors worker)
             (:port worker))
        state (worker-state conf (:worker-id worker))]
    (log-debug "Doing heartbeat " (pr-str hb))
    ;; do the local-file-system heartbeat.
    (.put state
        LS-WORKER-HEARTBEAT
        hb
        false
        )
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
                     @(:task-ids worker))]
    (-> worker
        :task->component
        reverse-map
        (select-keys components)
        vals
        flatten
        set )))

(defn mk-transfer-local-fn [worker]
  (let [short-executor-receive-queue-map (:short-executor-receive-queue-map worker)
        task->short-executor (:task->short-executor worker)
        task-getter (comp #(get @task->short-executor %) fast-first)]
    (fn [tuple-batch]
      (let [grouped (fast-group-by task-getter tuple-batch)]
        (fast-map-iter [[short-executor pairs] grouped]
          (let [q (@short-executor-receive-queue-map short-executor)]
            (if q
              (disruptor/publish q pairs)
              (log-warn "Received invalid messages for unknown tasks " short-executor ". Dropping... ")
              )))))))

(defn- assert-can-serialize [^KryoTupleSerializer serializer tuple-batch]
  "Check that all of the tuples can be serialized by serializing them."
  (fast-list-iter [[task tuple :as pair] tuple-batch]
    (.serialize serializer tuple)))

(defn mk-transfer-fn [worker]
  (let [local-tasks (:task-ids worker)
        local-transfer (:transfer-local-fn worker)
        ^DisruptorQueue transfer-queue (:transfer-queue worker)
        task->node+port (:cached-task->node+port worker)
        try-serialize-local ((:conf worker) TOPOLOGY-TESTING-ALWAYS-TRY-SERIALIZE)
        transfer-fn
          (fn [^KryoTupleSerializer serializer tuple-batch]
            (let [local (ArrayList.)
                  remoteMap (HashMap.)]
              (fast-list-iter [[task tuple :as pair] tuple-batch]
                (if (@local-tasks task)
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
        (log-warn "WILL TRY TO SERIALIZE ALL TUPLES (Turn off " TOPOLOGY-TESTING-ALWAYS-TRY-SERIALIZE " for production)")
        (fn [^KryoTupleSerializer serializer tuple-batch]
          (assert-can-serialize serializer tuple-batch)
          (transfer-fn serializer tuple-batch)))
      transfer-fn)))

(defn- mk-receive-queue-map [storm-conf executors]
  (->> executors
       ;; TODO: this depends on the type of executor
       (map (fn [e] [e (disruptor/disruptor-queue (str "receive-queue" e)
                                                  (storm-conf TOPOLOGY-EXECUTOR-RECEIVE-BUFFER-SIZE)
                                                  :wait-strategy (storm-conf TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))]))
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
        thread-pool-size (int (conf TOPOLOGY-WORKER-SHARED-THREAD-POOL-SIZE))]
    {WorkerTopologyContext/SHARED_EXECUTOR (Executors/newFixedThreadPool thread-pool-size)}
    ))

(defn- mk-user-resources [worker]
  ;;TODO: need to invoke a hook provided by the topology, giving it a chance to create user resources.
  ;; this would be part of the initialization hook
  ;; need to separate workertopologycontext into WorkerContext and WorkerUserContext.
  ;; actually just do it via interfaces. just need to make sure to hide setResource from tasks
  {})

(defn mk-halting-timer [timer-name]
  (mk-timer :kill-fn (fn [t]
                       (log-error t "Error when processing event")
                       (exit-process! 20 "Error when processing an event")
                       )
            :timer-name timer-name))

(defn worker-data [conf mq-context storm-id assignment-id port worker-id storm-conf cluster-state storm-cluster-state]
  (let [assignment-versions (atom {})
        transfer-queue (disruptor/disruptor-queue "worker-transfer-queue" (storm-conf TOPOLOGY-TRANSFER-BUFFER-SIZE)
                                                  :wait-strategy (storm-conf TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))

        topology (read-supervisor-topology conf storm-id)
        mq-context  (if mq-context
                      mq-context
                      (TransportFactory/makeContext storm-conf))]

    (recursive-map
      :conf conf
      :mq-context mq-context
      :receiver (.bind ^IContext mq-context storm-id port)
      :storm-id storm-id
      :assignment-id assignment-id
      :port port
      :worker-id worker-id
      :cluster-state cluster-state
      :storm-cluster-state storm-cluster-state
      :storm-active-atom (atom false)
      :executors (atom nil)
      :task-ids (atom nil)
      :outbound-tasks (atom nil)
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
      :component->sorted-tasks (->> (:task->component <>) reverse-map (map-val sort))
      :endpoint-socket-lock (mk-rw-lock)
      :cached-node+port->socket (atom (HashMap.))
      :cached-task->node+port (atom {})
      :transfer-queue transfer-queue
      :executor-receive-queue-map (atom {})
      :short-executor-receive-queue-map (atom {})
      :task->short-executor (atom {})
      :suicide-fn (mk-suicide-fn conf)
      :uptime (uptime-computer)
      :default-shared-resources (mk-default-resources <>)
      :user-shared-resources (mk-user-resources <>)
      :transfer-local-fn (mk-transfer-local-fn <>)
      :receiver-thread-count (get storm-conf WORKER-RECEIVER-THREAD-COUNT)
      :transfer-fn (mk-transfer-fn <>)
      :assignment-versions assignment-versions
      )))

(defn- endpoint->string [[node port]]
  (str port "/" node))

(defn string->endpoint [^String s]
  (let [[port-str node] (.split s "/" 2)]
    [node (Integer/valueOf port-str)]
    ))

(defn mk-sync-executors [worker executors credentials]
  (let [conf (:conf worker)
        storm-cluster-state (:storm-cluster-state worker)
        storm-id (:storm-id worker)
        assignment-versions (:assignment-versions worker)
        assignment-id (:assignment-id worker)
        port (:port worker)]
    (fn []
      (let [old-executor-ids @(:executors worker)
            assignment-info (:data (@assignment-versions storm-id))
            new-executor-ids (set (read-worker-executors assignment-info assignment-id port))]
        (if (not= new-executor-ids old-executor-ids)
          (let [_ (log-message "Syncing executors " old-executor-ids " -> " new-executor-ids)
                executors-to-launch (set/difference new-executor-ids old-executor-ids)
                executors-to-kill (set/difference old-executor-ids new-executor-ids)
                executor-receive-queue-map (merge
                                             (mk-receive-queue-map conf executors-to-launch)
                                             (apply dissoc @(:executor-receive-queue-map worker) executors-to-kill))
                receive-queue-map (->> executor-receive-queue-map
                                       (mapcat (fn [[e queue]] (for [t (executor-id->tasks e)] [t queue])))
                                       (into {}))
                task-ids (->> receive-queue-map keys (map int) sort)
                update-executors-fn (fn [executors]
                                      (doall
                                        (concat
                                          (mapcat (fn [e]
                                                    (let [id (executor/get-executor-id e)]
                                                      (if (new-executor-ids id)
                                                        [e]
                                                        (do
                                                          (log-message "Shutting down executor " id)
                                                          (.shutdown e)))))
                                                  executors)
                                          (dofor [e executors-to-launch]
                                            (executor/mk-executor worker e @credentials)))))]
            (reset! (:executors worker) new-executor-ids)
            (reset! (:executor-receive-queue-map worker) executor-receive-queue-map)
            (reset! (:short-executor-receive-queue-map worker) (map-key first executor-receive-queue-map))
            (reset! (:task-ids worker) (set task-ids))
            (reset! (:task->short-executor worker) (->> new-executor-ids
                                                        (mapcat (fn [e] (for [t (executor-id->tasks e)] [t (first e)])))
                                                        (into {})
                                                        (HashMap.)))
            (reset! (:outbound-tasks worker) (worker-outbound-tasks worker))
            #(swap! executors update-executors-fn)))))))

(defn mk-refresh-connections [worker executors credentials]
  (let [outbound-tasks (:outbound-tasks worker)
        conf (:conf worker)
        storm-cluster-state (:storm-cluster-state worker)
        storm-id (:storm-id worker)
        assignment-versions (:assignment-versions worker)
        sync-executors (mk-sync-executors worker executors credentials)]
    (fn this
      ([]
        (this (fn [& ignored]
                (log-message "refresh-connections callback fired")
                (schedule (:refresh-connections-timer worker) 0 this))))
      ([callback]
         (let [version (.assignment-version storm-cluster-state storm-id callback)
               old-version (:version (get @assignment-versions storm-id))
               version-changed? (not= version old-version)
               assignment (if (not version-changed?)
                            (:data (get @assignment-versions storm-id))
                            (let [new-assignment (.assignment-info-with-version storm-cluster-state storm-id callback)]
                              (swap! assignment-versions assoc storm-id new-assignment)
                              (:data new-assignment)))
               update-executors (if version-changed? (sync-executors) (fn []))

               my-assignment (-> assignment
                                 :executor->node+port
                                 to-task->node+port
                                 (select-keys @outbound-tasks)
                                 (#(map-val endpoint->string %)))
               ;; we dont need a connection for the local tasks anymore
               needed-assignment (->> my-assignment
                                      (filter-key (complement @(:task-ids worker))))
               needed-connections (-> needed-assignment vals set)
               needed-tasks (-> needed-assignment keys)

               current-connections (set (keys @(:cached-node+port->socket worker)))
               new-connections (set/difference needed-connections current-connections)
               remove-connections (set/difference current-connections needed-connections)]
           (log-message "refresh-connections " current-connections " -> " needed-connections)
           (swap! (:cached-node+port->socket worker)
             #(HashMap. (merge (into {} %1) %2))
             (into {}
                   (dofor [endpoint-str new-connections
                           :let [[node port] (string->endpoint endpoint-str)]]
                          [endpoint-str
                           (.connect
                             ^IContext (:mq-context worker)
                             storm-id
                             ((:node->host assignment) node)
                             port)
                           ]
                          )))
           (write-locked (:endpoint-socket-lock worker)
                    (reset! (:cached-task->node+port worker)
                            (HashMap. my-assignment)))
           (write-locked (:endpoint-socket-lock worker)
             (doseq [endpoint remove-connections]
               (.close (get @(:cached-node+port->socket worker) endpoint)))
             (apply swap!
                    (:cached-node+port->socket worker)
                    #(HashMap. (apply dissoc (into {} %1) %&))
                    remove-connections))

           (let [missing-tasks (->> needed-tasks
                                    (filter (complement my-assignment)))]
             (when-not (empty? missing-tasks)
               (log-warn "Missing assignment for following tasks: " (pr-str missing-tasks))))

           ;; this needs to be done last as executor initialization may require connections
           (update-executors))))))

(defn refresh-storm-active
  ([worker]
    (refresh-storm-active worker (fn [& ignored] (schedule (:refresh-active-timer worker) 0 (partial refresh-storm-active worker)))))
  ([worker callback]
    (let [base (.storm-base (:storm-cluster-state worker) (:storm-id worker) callback)]
     (reset!
      (:storm-active-atom worker)
      (= :active (-> base :status :type))
      ))
     ))

;; TODO: consider having a max batch size besides what disruptor does automagically to prevent latency issues
(defn mk-transfer-tuples-handler [worker]
  (let [drainer (TransferDrainer.)
        node+port->socket (:cached-node+port->socket worker)
        endpoint-socket-lock (:endpoint-socket-lock worker)]
    (disruptor/clojure-handler
      (fn [packets _ batch-end?]
        (log-message "transfering outbound " packets)
        (.add drainer packets)
        
        (when batch-end?
          (read-locked endpoint-socket-lock
            (let [node+port->socket @node+port->socket]
              (.send drainer node+port->socket)))
          (.clear drainer))))))

(defn launch-receive-thread [worker]
  (log-message "Launching receive-thread for " (:assignment-id worker) ":" (:port worker))
  (msg-loader/launch-receive-thread!
    (:mq-context worker)
    (:receiver worker)
    (:storm-id worker)
    (:receiver-thread-count worker)
    (:port worker)
    (:transfer-local-fn worker)
    (-> worker :storm-conf (get TOPOLOGY-RECEIVER-BUFFER-SIZE))
    :kill-fn (fn [t] (exit-process! 11))))

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
  (if-not (local-mode? conf)
    (redirect-stdio-to-slf4j!))
  ;; because in local mode, its not a separate
  ;; process. supervisor will register it in this case
  (when (= :distributed (cluster-mode conf))
    (touch (worker-pid-path conf worker-id (process-pid))))
  (let [storm-conf (read-supervisor-storm-conf conf storm-id)
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
 
        credentials (atom initial-credentials)
        executors (atom nil)
        ;; launch heartbeat threads immediately so that slow-loading tasks don't cause the worker to timeout
        ;; to the supervisor
        _ (schedule-recurring (:heartbeat-timer worker) 0 (conf WORKER-HEARTBEAT-FREQUENCY-SECS) heartbeat-fn)
        _ (schedule-recurring (:executor-heartbeat-timer worker) 0 (conf TASK-HEARTBEAT-FREQUENCY-SECS) #(do-executor-heartbeats worker :executors @executors))

        refresh-connections (mk-refresh-connections worker executors credentials)

        _ (refresh-connections nil)
        _ (refresh-storm-active worker nil)
 

        receive-thread-shutdown (launch-receive-thread worker)
        
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
                    (cancel-timer (:heartbeat-timer worker))
                    (cancel-timer (:refresh-connections-timer worker))
                    (cancel-timer (:refresh-credentials-timer worker))
                    (cancel-timer (:refresh-active-timer worker))
                    (cancel-timer (:executor-heartbeat-timer worker))
                    (cancel-timer (:user-timer worker))
                    
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
                 (timer-waiting? (:heartbeat-timer worker))
                 (timer-waiting? (:refresh-connections-timer worker))
                 (timer-waiting? (:refresh-credentials-timer worker))
                 (timer-waiting? (:refresh-active-timer worker))
                 (timer-waiting? (:executor-heartbeat-timer worker))
                 (timer-waiting? (:user-timer worker))
                 ))
             )
        check-credentials-changed (fn []
                                    (let [new-creds (.credentials (:storm-cluster-state worker) storm-id nil)]
                                      (when-not (= new-creds @credentials) ;;This does not have to be atomic, worst case we update when one is not needed
                                        (AuthUtils/updateSubject subject auto-creds new-creds)
                                        (dofor [e @executors] (.credentials-changed e new-creds))
                                        (reset! credentials new-creds))))
      ]
    (.credentials (:storm-cluster-state worker) storm-id (fn [args] (check-credentials-changed)))
    (schedule-recurring (:refresh-credentials-timer worker) 0 (conf TASK-CREDENTIALS-POLL-SECS) check-credentials-changed)
    (schedule-recurring (:refresh-connections-timer worker) 0 (conf TASK-REFRESH-POLL-SECS) refresh-connections)
    (schedule-recurring (:refresh-active-timer worker) 0 (conf TASK-REFRESH-POLL-SECS) (partial refresh-storm-active worker))

    (log-message "Worker has topology config " (:storm-conf worker))
    (log-message "Worker " worker-id " for storm " storm-id " on " assignment-id ":" port " has finished loading")
    ret
    ))))))

(defmethod mk-suicide-fn
  :local [conf]
  (fn [] (exit-process! 1 "Worker died")))

(defmethod mk-suicide-fn
  :distributed [conf]
  (fn [] (exit-process! 1 "Worker died")))

(defn -main [storm-id assignment-id port-str worker-id]  
  (let [conf (read-storm-config)]
    (validate-distributed-mode! conf)
    (let [worker (mk-worker conf nil storm-id assignment-id (Integer/parseInt port-str) worker-id)]
      (add-shutdown-hook-with-force-kill-in-1-sec #(.shutdown worker)))))

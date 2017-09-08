;
; Licensed to the Apache Software Foundation (ASF) under one
; or more contributor license agreements.  See the NOTICE file
; distributed with this work for additional information
; regarding copyright ownership.  The ASF licenses this file
; to you under the Apache License, Version 2.0 (the
; "License"); you may not use this file except in compliance
; with the License.  You may obtain a copy of the License at
;
; http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.
;

(ns org.apache.storm.ui.core
  (:use compojure.core)
  (:use [clojure.java.shell :only [sh]])
  (:use ring.middleware.reload
        ring.middleware.multipart-params
        ring.middleware.multipart-params.temp-file)
  (:use [ring.middleware.json :only [wrap-json-params]])
  (:use [hiccup core page-helpers])
  (:use [org.apache.storm config daemon-config util log])
  (:use [org.apache.storm.ui helpers])
  (:import [org.apache.storm.utils Time]
           [org.apache.storm.generated NimbusSummary]
           [org.apache.storm.stats StatsUtil]
           [org.apache.storm.ui UIHelpers IConfigurator FilterConfiguration]
           [org.apache.storm.metric StormMetricsRegistry])
  (:import [org.apache.storm.utils TopologySpoutLag])
  (:use [clojure.string :only [blank? lower-case trim split]])
  (:import [org.apache.storm.generated ExecutorSpecificStats
            ExecutorStats ExecutorSummary ExecutorInfo TopologyInfo SpoutStats BoltStats
            ErrorInfo ClusterSummary SupervisorSummary TopologySummary
            Nimbus$Client StormTopology GlobalStreamId RebalanceOptions
            KillOptions GetInfoOptions NumErrorsChoice DebugOptions TopologyPageInfo
            TopologyStats CommonAggregateStats ComponentAggregateStats
            ComponentType BoltAggregateStats SpoutAggregateStats
            ExecutorAggregateStats SpecificAggregateStats ComponentPageInfo
            LogConfig LogLevel LogLevelAction SupervisorPageInfo WorkerSummary OwnerResourceSummary])
  (:import [org.apache.storm.security.auth AuthUtils ReqContext])
  (:import [org.apache.storm.generated AuthorizationException ProfileRequest ProfileAction NodeInfo])
  (:import [org.apache.storm.security.auth AuthUtils])
  (:import [org.apache.storm.utils VersionInfo ConfigUtils Utils WebAppUtils])
  (:import [org.apache.storm Config])
  (:import [java.io File])
  (:import [java.net URLEncoder URLDecoder])
  (:import [org.json.simple JSONValue])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [ring.util.response :as resp]
            [org.apache.storm.internal [thrift :as thrift]])
  (:import [org.apache.commons.lang StringEscapeUtils])
  (:import [org.apache.logging.log4j Level]
           (org.apache.storm.daemon StormCommon Acker))
  (:import [org.eclipse.jetty.server Server])
  (:gen-class))

(def ^:dynamic *STORM-CONF* (clojurify-structure (ConfigUtils/readStormConfig)))
(def ^:dynamic *UI-ACL-HANDLER* (StormCommon/mkAuthorizationHandler (*STORM-CONF* NIMBUS-AUTHORIZER) *STORM-CONF*))
(def ^:dynamic *UI-IMPERSONATION-HANDLER* (StormCommon/mkAuthorizationHandler (*STORM-CONF* NIMBUS-IMPERSONATION-AUTHORIZER) *STORM-CONF*))
(def http-creds-handler (AuthUtils/GetUiHttpCredentialsPlugin *STORM-CONF*))
(def STORM-VERSION (VersionInfo/getVersion))

(def ui:num-cluster-configuration-http-requests (StormMetricsRegistry/registerMeter "ui:num-cluster-configuration-http-requests")) 
(def ui:num-cluster-summary-http-requests (StormMetricsRegistry/registerMeter "ui:num-cluster-summary-http-requests")) 
(def ui:num-nimbus-summary-http-requests (StormMetricsRegistry/registerMeter "ui:num-nimbus-summary-http-requests")) 
(def ui:num-supervisor-http-requests (StormMetricsRegistry/registerMeter "ui:num-supervisor-http-requests")) 
(def ui:num-supervisor-summary-http-requests (StormMetricsRegistry/registerMeter "ui:num-supervisor-summary-http-requests")) 
(def ui:num-all-topologies-summary-http-requests (StormMetricsRegistry/registerMeter "ui:num-all-topologies-summary-http-requests")) 
(def ui:num-topology-page-http-requests (StormMetricsRegistry/registerMeter "ui:num-topology-page-http-requests"))
(def ui:num-topology-metric-http-requests (StormMetricsRegistry/registerMeter "ui:num-topology-metric-http-requests"))
(def ui:num-build-visualization-http-requests (StormMetricsRegistry/registerMeter "ui:num-build-visualization-http-requests"))
(def ui:num-mk-visualization-data-http-requests (StormMetricsRegistry/registerMeter "ui:num-mk-visualization-data-http-requests")) 
(def ui:num-component-page-http-requests (StormMetricsRegistry/registerMeter "ui:num-component-page-http-requests")) 
(def ui:num-log-config-http-requests (StormMetricsRegistry/registerMeter "ui:num-log-config-http-requests")) 
(def ui:num-activate-topology-http-requests (StormMetricsRegistry/registerMeter "ui:num-activate-topology-http-requests")) 
(def ui:num-deactivate-topology-http-requests (StormMetricsRegistry/registerMeter "ui:num-deactivate-topology-http-requests")) 
(def ui:num-debug-topology-http-requests (StormMetricsRegistry/registerMeter "ui:num-debug-topology-http-requests")) 
(def ui:num-component-op-response-http-requests (StormMetricsRegistry/registerMeter "ui:num-component-op-response-http-requests"))
(def ui:num-topology-op-response-http-requests (StormMetricsRegistry/registerMeter "ui:num-topology-op-response-http-requests")) 
(def ui:num-main-page-http-requests (StormMetricsRegistry/registerMeter "ui:num-main-page-http-requests")) 
(def ui:num-topology-lag-http-requests (StormMetricsRegistry/registerMeter "ui:num-topology-lag-http-requests"))
(def ui:num-get-owner-resource-summaries-http-requests (StormMetricsRegistry/registerMeter "ui:num-get-owner-resource-summaries-http-request"))

(defn assert-authorized-user
  ([op]
    (assert-authorized-user op nil))
  ([op topology-conf]
    (let [context (ReqContext/context)]
      (if (.isImpersonating context)
        (if *UI-IMPERSONATION-HANDLER*
            (if-not (.permit *UI-IMPERSONATION-HANDLER* context op topology-conf)
              (let [principal (.principal context)
                    real-principal (.realPrincipal context)
                    user (if principal (.getName principal) "unknown")
                    real-user (if real-principal (.getName real-principal) "unknown")
                    remote-address (.remoteAddress context)]
                (throw (AuthorizationException.
                         (str "user '" real-user "' is not authorized to impersonate user '" user "' from host '" remote-address "'. Please
                         see SECURITY.MD to learn how to configure impersonation ACL.")))))
          (log-warn " principal " (.realPrincipal context) " is trying to impersonate " (.principal context) " but "
            NIMBUS-IMPERSONATION-AUTHORIZER " has no authorizer configured. This is a potential security hole.
            Please see SECURITY.MD to learn how to configure an impersonation authorizer.")))

      (if *UI-ACL-HANDLER*
       (if-not (.permit *UI-ACL-HANDLER* context op topology-conf)
         (let [principal (.principal context)
               user (if principal (.getName principal) "unknown")]
           (throw (AuthorizationException.
                   (str "UI request '" op "' for '" user "' user is not authorized")))))))))

(defn executor-summary-type
  [topology ^ExecutorSummary s]
  (StatsUtil/componentType topology (.get_component_id s)))

(defn spout-summary?
  [topology s]
  (= "spout" (executor-summary-type topology s)))

(defn bolt-summary?
  [topology s]
  (= "bolt" (executor-summary-type topology s)))

(defn group-by-comp
  [summs]
  (let [ret (group-by #(.get_component_id ^ExecutorSummary %) summs)]
    (into (sorted-map) ret )))

(defn logviewer-link [host fname secure?]
  (if (and secure? (*STORM-CONF* LOGVIEWER-HTTPS-PORT))
    (UIHelpers/urlFormat "https://%s:%s/api/v1/log?file=%s"
      (to-array
        [host
        (*STORM-CONF* LOGVIEWER-HTTPS-PORT)
        fname]))
    (UIHelpers/urlFormat "http://%s:%s/api/v1/log?file=%s"
      (to-array
        [host
        (*STORM-CONF* LOGVIEWER-PORT)
        fname]))))

(defn event-log-link
  [topology-id component-id host port secure?]
  (logviewer-link host (WebAppUtils/eventLogsFilename topology-id (str port)) secure?))

(defn worker-log-link [host port topology-id secure?]
  (if (or (empty? host) (let [port_str (str port "")] (or (empty? port_str) (= "0" port_str))))
    ""
    (let [fname (WebAppUtils/logsFilename topology-id (str port))]
      (logviewer-link host fname secure?))))

(defn nimbus-log-link [host]
  (UIHelpers/urlFormat "http://%s:%s/api/v1/daemonlog?file=nimbus.log" (to-array [host (*STORM-CONF* LOGVIEWER-PORT)])))

(defn supervisor-log-link [host]
  (UIHelpers/urlFormat "http://%s:%s/api/v1/daemonlog?file=supervisor.log" (to-array [host (*STORM-CONF* LOGVIEWER-PORT)])))

(defn get-error-data
  [error]
  (if error
    (StatsUtil/errorSubset (.get_error ^ErrorInfo error))
    ""))

(defn get-error-port
  [error]
  (if error
    (.get_port ^ErrorInfo error)
    ""))

(defn get-error-host
  [error]
  (if error
    (.get_host ^ErrorInfo error)
    ""))

(defn get-error-time
  [error]
  (if error
    (.get_error_time_secs ^ErrorInfo error)))

(defn worker-dump-link [host port topology-id]
  (UIHelpers/urlFormat "http://%s:%s/api/v1/dumps/%s/%s"
    (to-array [(URLEncoder/encode host)
              (*STORM-CONF* LOGVIEWER-PORT)
              (URLEncoder/encode topology-id)
              (str (URLEncoder/encode host) ":" (URLEncoder/encode port))])))

(defn stats-times
  [stats-map]
  (sort-by #(Integer/parseInt %)
           (-> stats-map
               clojurify-structure
               (dissoc ":all-time")
               keys)))

(defn window-hint
  [window]
  (if (= window ":all-time")
    "All time"
    (UIHelpers/prettyUptimeSec window)))

(defn sanitize-stream-name
  [name]
  (let [sym-regex #"(?![A-Za-z_\-:\.])."]
    (str
     (if (re-find #"^[A-Za-z]" name)
       (clojure.string/replace name sym-regex "_")
       (clojure.string/replace (str \s name) sym-regex "_"))
     (hash name))))

(defn sanitize-transferred
  [transferred]
  (into {}
        (for [[time, stream-map] transferred]
          [time, (into {}
                       (for [[stream, trans] stream-map]
                         [(sanitize-stream-name stream), trans]))])))

(defn visualization-data
  [spout-bolt spout-comp-summs bolt-comp-summs window storm-id]
  (let [components (for [[id spec] spout-bolt
                         :when (or (contains? bolt-comp-summs id)
                                   (contains? spout-comp-summs id))]
            [id
             (let [inputs (.get_inputs (.get_common spec))
                   bolt-summs (.get bolt-comp-summs id)
                   spout-summs (.get spout-comp-summs id)
                   bolt-cap (if bolt-summs
                              (StatsUtil/computeBoltCapacity bolt-summs)
                              0)]
               {:type (if bolt-summs "bolt" "spout")
                :capacity bolt-cap
                :latency (if bolt-summs
                           (get-in
                             (clojurify-structure (StatsUtil/boltStreamsStats bolt-summs true))
                             ["process-latencies" window])
                           (get-in
                             (clojurify-structure (StatsUtil/spoutStreamsStats spout-summs true))
                             ["complete-latencies" window]))
                :transferred (or
                               (get-in
                                 (clojurify-structure (StatsUtil/spoutStreamsStats spout-summs true))
                                 ["transferred" window])
                               (get-in
                                 (clojurify-structure (StatsUtil/boltStreamsStats bolt-summs true))
                                 ["transferred" window]))
                :stats (let [mapfn (fn [dat]
                                     (map (fn [^ExecutorSummary summ]
                                            {:host (.get_host summ)
                                             :port (.get_port summ)
                                             :uptime_secs (.get_uptime_secs summ)
                                             :transferred (if-let [stats (.get_stats summ)]
                                                            (sanitize-transferred (.get_transferred stats)))})
                                          dat))]
                         (if bolt-summs
                           (mapfn bolt-summs)
                           (mapfn spout-summs)))
                :link (UIHelpers/urlFormat "/component.html?id=%s&topology_id=%s" (to-array [id storm-id]))
                :inputs (for [[global-stream-id group] inputs]
                          {:component (.get_componentId global-stream-id)
                           :stream (.get_streamId global-stream-id)
                           :sani-stream (sanitize-stream-name (.get_streamId global-stream-id))
                           :grouping (clojure.core/name (thrift/grouping-type group))})})])]
    (into {} (doall components))))

(defn mk-include-sys-fn
  [include-sys?]
  (if include-sys?
    (fn [_] true)
    (fn [stream] (and (string? stream) (not (Utils/isSystemId stream))))))

(defn stream-boxes [datmap]
  (let [filter-fn (mk-include-sys-fn true)
        streams
        (vec (doall (distinct
                     (apply concat
                            (for [[k v] datmap]
                              (for [m (get v :inputs)]
                                {:stream (get m :stream)
                                 :sani-stream (get m :sani-stream)
                                 :checked (not
                                            (Utils/isSystemId
                                              (get m :stream)))}))))))]
    (map (fn [row]
           {:row row}) (partition 4 4 nil streams))))

(defn- get-topology-info
  ([^Nimbus$Client nimbus id]
    (.getTopologyInfo nimbus id))
  ([^Nimbus$Client nimbus id options]
    (.getTopologyInfoWithOpts nimbus id options)))

(defn mk-visualization-data
  [id window include-sys?]
  (thrift/with-configured-nimbus-connection
    nimbus
    (let [window (if window window ":all-time")
          topology (.getTopology ^Nimbus$Client nimbus id)
          spouts (.get_spouts topology)
          bolts (.get_bolts topology)
          summ (->> (doto
                      (GetInfoOptions.)
                      (.set_num_err_choice NumErrorsChoice/NONE))
                    (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
          execs (.get_executors summ)
          spout-summs (filter (partial spout-summary? topology) execs)
          bolt-summs (filter (partial bolt-summary? topology) execs)
          spout-comp-summs (group-by-comp spout-summs)
          bolt-comp-summs (group-by-comp bolt-summs)
          ;TODO: when translating this function, you should replace the filter-key with a proper for loop + if condition HERE
          bolt-comp-summs (filter-key (mk-include-sys-fn include-sys?)
                                      bolt-comp-summs)]
      (visualization-data
       (merge (hashmap-to-persistent spouts)
              (hashmap-to-persistent bolts))
       spout-comp-summs bolt-comp-summs window id))))

(defn- from-json
  [^String str]
  (if str
    (clojurify-structure
      (JSONValue/parse str))
    nil))

(defn validate-tplg-submit-params [params]
  (let [tplg-jar-file (params :topologyJar)
        tplg-config (if (not-nil? (params :topologyConfig)) (from-json (params :topologyConfig)))]
    (cond
     (nil? tplg-jar-file) {:valid false :error "missing topology jar file"}
     (nil? tplg-config) {:valid false :error "missing topology config"}
     (nil? (tplg-config "topologyMainClass")) {:valid false :error "topologyMainClass missing in topologyConfig"}
     :else {:valid true})))

(defn run-tplg-submit-cmd [tplg-jar-file tplg-config user]
  (let [tplg-main-class (if (not-nil? tplg-config) (trim (tplg-config "topologyMainClass")))
        tplg-main-class-args (if (not-nil? tplg-config) (tplg-config "topologyMainClassArgs"))
        storm-home (System/getProperty "storm.home")
        storm-conf-dir (str storm-home WebAppUtils/FILE_PATH_SEPARATOR "conf")
        storm-log-dir (if (not-nil? (*STORM-CONF* "storm.log.dir")) (*STORM-CONF* "storm.log.dir")
                                                                    (str storm-home WebAppUtils/FILE_PATH_SEPARATOR "logs"))
        storm-libs (str storm-home WebAppUtils/FILE_PATH_SEPARATOR "lib" WebAppUtils/FILE_PATH_SEPARATOR "*")
        java-cmd (str (System/getProperty "java.home") WebAppUtils/FILE_PATH_SEPARATOR "bin" WebAppUtils/FILE_PATH_SEPARATOR "java")
        storm-cmd (str storm-home WebAppUtils/FILE_PATH_SEPARATOR "bin" WebAppUtils/FILE_PATH_SEPARATOR "storm")
        tplg-cmd-response (apply sh
                            (flatten
                              [storm-cmd "jar" tplg-jar-file tplg-main-class
                                (if (not-nil? tplg-main-class-args) tplg-main-class-args [])
                                (if (not= user "unknown") (str "-c storm.doAsUser=" user) [])]))]
    (log-message "tplg-cmd-response " tplg-cmd-response)
    (cond
     (= (tplg-cmd-response :exit) 0) {"status" "success"}
     (and (not= (tplg-cmd-response :exit) 0)
          (not-nil? (re-find #"already exists on cluster" (tplg-cmd-response :err)))) {"status" "failed" "error" "Topology with the same name exists in cluster"}
          (not= (tplg-cmd-response :exit) 0) {"status" "failed" "error" (clojure.string/trim-newline (tplg-cmd-response :err))}
          :else {"status" "success" "response" "topology deployed"}
          )))

(defn cluster-configuration []
  (thrift/with-configured-nimbus-connection nimbus
    (.getNimbusConf ^Nimbus$Client nimbus)))

(defn topology-history-info
  ([user]
    (thrift/with-configured-nimbus-connection nimbus
      (topology-history-info (.getTopologyHistory ^Nimbus$Client nimbus user) user)))
  ([history user]
    {"topo-history"
     (into [] (.get_topo_ids history))}))

(defn cluster-summary
  ([user]
     (thrift/with-configured-nimbus-connection nimbus
        (cluster-summary (.getClusterInfo ^Nimbus$Client nimbus) user)))
  ([^ClusterSummary summ user]
     (let [sups (.get_supervisors summ)
           used-slots (reduce + (map #(.get_num_used_workers ^SupervisorSummary %) sups))
           total-slots (reduce + (map #(.get_num_workers ^SupervisorSummary %) sups))
           free-slots (- total-slots used-slots)
           topologies (.get_topologies_size summ)
           total-tasks (->> (.get_topologies summ)
                            (map #(.get_num_tasks ^TopologySummary %))
                            (reduce +))
           total-executors (->> (.get_topologies summ)
                                (map #(.get_num_executors ^TopologySummary %))
                             (reduce +))
           resourceSummary (if (> (.size sups) 0)
                             (reduce #(map + %1 %2)
                               (for [^SupervisorSummary s sups
                                     :let [sup-total-mem (get (.get_total_resources s) Config/SUPERVISOR_MEMORY_CAPACITY_MB)
                                           sup-total-cpu (get (.get_total_resources s) Config/SUPERVISOR_CPU_CAPACITY)
                                           sup-avail-mem (max (- sup-total-mem (.get_used_mem s)) 0.0)
                                           sup-avail-cpu (max (- sup-total-cpu (.get_used_cpu s)) 0.0)]]
                                 [sup-total-mem sup-total-cpu sup-avail-mem sup-avail-cpu]))
                             [0.0 0.0 0.0 0.0])
           total-mem (nth resourceSummary 0)
           total-cpu (nth resourceSummary 1)
           avail-mem (nth resourceSummary 2)
           avail-cpu (nth resourceSummary 3)]
       {"user" user
        "stormVersion" STORM-VERSION
        "supervisors" (count sups)
        "topologies" topologies
        "slotsTotal" total-slots
        "slotsUsed"  used-slots
        "slotsFree" free-slots
        "executorsTotal" total-executors
        "tasksTotal" total-tasks
        "schedulerDisplayResource" (*STORM-CONF* SCHEDULER-DISPLAY-RESOURCE)
        "totalMem" total-mem
        "totalCpu" total-cpu
        "availMem" avail-mem
        "availCpu" avail-cpu
        "memAssignedPercentUtil" (if (and (not (nil? total-mem)) (> total-mem 0.0)) (format "%.1f" (* (/ (- total-mem avail-mem) total-mem) 100.0)) 0.0)
        "cpuAssignedPercentUtil" (if (and (not (nil? total-cpu)) (> total-cpu 0.0)) (format "%.1f" (* (/ (- total-cpu avail-cpu) total-cpu) 100.0)) 0.0)})))

(defn convert-to-nimbus-summary[nimbus-seed]
  (let [[host port] (.split nimbus-seed ":")]
    {
      "host" host
      "port" port
      "nimbusLogLink" (nimbus-log-link host)
      "status" "Offline"
      "version" "Not applicable"
      "nimbusUpTime" "Not applicable"
      "nimbusUptimeSeconds" "Not applicable"}
    ))

(defn nimbus-summary
  ([]
    (thrift/with-configured-nimbus-connection nimbus
      (nimbus-summary
        (.get_nimbuses (.getClusterInfo ^Nimbus$Client nimbus)))))
  ([nimbuses]
    (let [nimbus-seeds (set (map #(str %1 ":" (*STORM-CONF* NIMBUS-THRIFT-PORT)) (set (*STORM-CONF* NIMBUS-SEEDS))))
          alive-nimbuses (set (map #(str (.get_host %1) ":" (.get_port %1)) nimbuses))
          offline-nimbuses (clojure.set/difference nimbus-seeds alive-nimbuses)
          offline-nimbuses-summary (map #(convert-to-nimbus-summary %1) offline-nimbuses)]
      {"nimbuses"
       (concat offline-nimbuses-summary
       (for [^NimbusSummary n nimbuses
             :let [uptime (.get_uptime_secs n)]]
         {
          "host" (.get_host n)
          "port" (.get_port n)
          "nimbusLogLink" (nimbus-log-link (.get_host n))
          "status" (if (.is_isLeader n) "Leader" "Not a Leader")
          "version" (.get_version n)
          "nimbusUpTime" (UIHelpers/prettyUptimeSec uptime)
          "nimbusUpTimeSeconds" uptime}))})))

(defn worker-summary-to-json
  [secure? ^WorkerSummary worker-summary]
  (let [host (.get_host worker-summary)
        port (.get_port worker-summary)
        topology-id (.get_topology_id worker-summary)
        uptime-secs (.get_uptime_secs worker-summary)]
    {"supervisorId" (.get_supervisor_id worker-summary)
     "host" host
     "port" port
     "topologyId" topology-id
     "topologyName" (.get_topology_name worker-summary)
     "executorsTotal" (.get_num_executors worker-summary)
     "assignedMemOnHeap" (.get_assigned_memonheap worker-summary)
     "assignedMemOffHeap" (.get_assigned_memoffheap worker-summary)
     "assignedCpu" (.get_assigned_cpu worker-summary)
     "componentNumTasks" (.get_component_to_num_tasks worker-summary)
     "uptime" (UIHelpers/prettyUptimeSec uptime-secs)
     "uptimeSeconds" uptime-secs
     "workerLogLink" (worker-log-link host port topology-id secure?)}))

(defn supervisor-summary-to-json 
  [summary]
  (let [slotsTotal (.get_num_workers summary)
        slotsUsed (.get_num_used_workers summary)
        slotsFree (max (- slotsTotal slotsUsed) 0)
        totalMem (get (.get_total_resources summary) Config/SUPERVISOR_MEMORY_CAPACITY_MB)
        totalCpu (get (.get_total_resources summary) Config/SUPERVISOR_CPU_CAPACITY)
        usedMem (.get_used_mem summary)
        usedCpu (.get_used_cpu summary)
        availMem (max (- totalMem usedMem) 0)
        availCpu (max (- totalCpu usedCpu) 0)]
  {"id" (.get_supervisor_id summary)
   "host" (.get_host summary)
   "uptime" (UIHelpers/prettyUptimeSec (.get_uptime_secs summary))
   "uptimeSeconds" (.get_uptime_secs summary)
   "slotsTotal" slotsTotal
   "slotsUsed" slotsUsed
   "slotsFree" slotsFree
   "totalMem" totalMem
   "totalCpu" totalCpu
   "usedMem" usedMem
   "usedCpu" usedCpu
   "logLink" (supervisor-log-link (.get_host summary))
   "availMem" availMem
   "availCpu" availCpu
   "version" (.get_version summary)}))

(defn supervisor-page-info
  ([supervisor-id host include-sys? secure?]
     (thrift/with-configured-nimbus-connection 
        nimbus (supervisor-page-info (.getSupervisorPageInfo ^Nimbus$Client nimbus
                                                      supervisor-id
                                                      host
                                                      include-sys?) secure?)))
  ([^SupervisorPageInfo supervisor-page-info secure?]
    ;; ask nimbus to return supervisor workers + any details user is allowed
    ;; access on a per-topology basis (i.e. components)
    (let [supervisors-json (map supervisor-summary-to-json (.get_supervisor_summaries supervisor-page-info))]
      {"supervisors" supervisors-json
       "schedulerDisplayResource" (*STORM-CONF* SCHEDULER-DISPLAY-RESOURCE)
       "workers" (into [] (for [^WorkerSummary worker-summary (.get_worker_summaries supervisor-page-info)]
                            (worker-summary-to-json secure? worker-summary)))})))

(defn supervisor-summary
  ([]
   (thrift/with-configured-nimbus-connection nimbus
                (supervisor-summary
                  (.get_supervisors (.getClusterInfo ^Nimbus$Client nimbus)))))
  ([summs]
   {"supervisors" (for [^SupervisorSummary s summs]
                    (supervisor-summary-to-json s))
    "schedulerDisplayResource" (*STORM-CONF* SCHEDULER-DISPLAY-RESOURCE)}))

(defnk get-topologies-map [summs :conditional (fn [t] true) :keys nil]
  (for [^TopologySummary t summs :when (conditional t)]
    (let [data {"id" (.get_id t)
                "encodedId" (URLEncoder/encode (.get_id t))
                "owner" (.get_owner t)
                "name" (.get_name t)
                "status" (.get_status t)
                "uptime" (UIHelpers/prettyUptimeSec (.get_uptime_secs t))
                "uptimeSeconds" (.get_uptime_secs t)
                "tasksTotal" (.get_num_tasks t)
                "workersTotal" (.get_num_workers t)
                "executorsTotal" (.get_num_executors t)
                "replicationCount" (.get_replication_count t)
                "schedulerInfo" (.get_sched_status t)
                "requestedMemOnHeap" (.get_requested_memonheap t)
                "requestedMemOffHeap" (.get_requested_memoffheap t)
                "requestedTotalMem" (+ (.get_requested_memonheap t) (.get_requested_memoffheap t))
                "requestedCpu" (.get_requested_cpu t)
                "assignedMemOnHeap" (.get_assigned_memonheap t)
                "assignedMemOffHeap" (.get_assigned_memoffheap t)
                "assignedTotalMem" (+ (.get_assigned_memonheap t) (.get_assigned_memoffheap t))
                "assignedCpu" (.get_assigned_cpu t)
                "topologyVersion" (.get_topology_version t)
                "stormVersion" (.get_storm_version t)}]
      (if (not-nil? keys) (select-keys data keys) data))))

(defn all-topologies-summary
  ([]
   (thrift/with-configured-nimbus-connection
     nimbus
     (all-topologies-summary
       (.get_topologies (.getClusterInfo ^Nimbus$Client nimbus)))))
  ([summs]
   {"topologies" (get-topologies-map summs)
    "schedulerDisplayResource" (*STORM-CONF* SCHEDULER-DISPLAY-RESOURCE)}))

(defn topology-stats [window stats]
  (let [times (stats-times (:emitted stats))
        display-map (into {} (for [t times] [t window-hint]))
        display-map (assoc display-map ":all-time" (fn [_] "All time"))]
    (for [w (concat times [":all-time"])
          :let [disp ((display-map w) w)]]
      {"windowPretty" disp
       "window" w
       "emitted" (get-in stats [:emitted w])
       "transferred" (get-in stats [:transferred w])
       "completeLatency" (StatsUtil/floatStr (get-in stats [:complete-latencies w]))
       "acked" (get-in stats [:acked w])
       "failed" (get-in stats [:failed w])})))

(defn build-visualization [id window include-sys?]
  (thrift/with-configured-nimbus-connection nimbus
    (let [window (if window window ":all-time")
          topology-info (->> (doto
                               (GetInfoOptions.)
                               (.set_num_err_choice NumErrorsChoice/ONE))
                             (.getTopologyInfoWithOpts ^Nimbus$Client nimbus
                                                       id))
          storm-topology (.getTopology ^Nimbus$Client nimbus id)
          spout-executor-summaries (filter (partial spout-summary? storm-topology) (.get_executors topology-info))
          bolt-executor-summaries (filter (partial bolt-summary? storm-topology) (.get_executors topology-info))
          spout-comp-id->executor-summaries (group-by-comp spout-executor-summaries)
          bolt-comp-id->executor-summaries (group-by-comp bolt-executor-summaries)
          ;TODO: when translating this function, you should replace the filter-key with a proper for loop + if condition HERE
          bolt-comp-id->executor-summaries (filter-key (mk-include-sys-fn include-sys?) bolt-comp-id->executor-summaries)
          id->spout-spec (.get_spouts storm-topology)
          id->bolt (.get_bolts storm-topology)
          visualizer-data (visualization-data (merge (hashmap-to-persistent id->spout-spec)
                                                     (hashmap-to-persistent id->bolt))
                                              spout-comp-id->executor-summaries
                                              bolt-comp-id->executor-summaries
                                              window
                                              id)]
       {"visualizationTable" (stream-boxes visualizer-data)})))

(defn- get-error-json
  [topo-id error-info secure?]
  (let [host (get-error-host error-info)
        port (get-error-port error-info)]
    {"lastError" (get-error-data error-info)
     "errorTime" (get-error-time error-info)
     "errorHost" host
     "errorPort" port
     "errorLapsedSecs" (if-let [t (get-error-time error-info)] (Time/deltaSecs t))
     "errorWorkerLogLink" (worker-log-link host port topo-id secure?)}))

(defn- common-agg-stats-json
  "Returns a JSON representation of a common aggregated statistics."
  [^CommonAggregateStats common-stats]
  {"executors" (.get_num_executors common-stats)
   "tasks" (.get_num_tasks common-stats)
   "emitted" (.get_emitted common-stats)
   "transferred" (.get_transferred common-stats)
   "acked" (.get_acked common-stats)
   "failed" (.get_failed common-stats)
   "requestedMemOnHeap" (.get (.get_resources_map common-stats) Config/TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)
   "requestedMemOffHeap" (.get (.get_resources_map common-stats) Config/TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)
   "requestedCpu" (.get (.get_resources_map common-stats) Config/TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)})

(defmulti comp-agg-stats-json
  "Returns a JSON representation of aggregated statistics."
  (fn [_ _ [id ^ComponentAggregateStats s]] (.get_type s)))

(defmethod comp-agg-stats-json ComponentType/SPOUT
  [topo-id secure? [id ^ComponentAggregateStats s]]
  (let [^SpoutAggregateStats ss (.. s get_specific_stats get_spout)
        cs (.get_common_stats s)]
    (merge
      (common-agg-stats-json cs)
      (get-error-json topo-id (.get_last_error s) secure?)
      {"spoutId" id
       "encodedSpoutId" (URLEncoder/encode id)
       "completeLatency" (StatsUtil/floatStr (.get_complete_latency_ms ss))})))

(defmethod comp-agg-stats-json ComponentType/BOLT
  [topo-id secure? [id ^ComponentAggregateStats s]]
  (let [^BoltAggregateStats ss (.. s get_specific_stats get_bolt)
        cs (.get_common_stats s)]
    (merge
      (common-agg-stats-json cs)
      (get-error-json topo-id (.get_last_error s) secure?)
      {"boltId" id
       "encodedBoltId" (URLEncoder/encode id)
       "capacity" (StatsUtil/floatStr (.get_capacity ss))
       "executeLatency" (StatsUtil/floatStr (.get_execute_latency_ms ss))
       "executed" (.get_executed ss)
       "processLatency" (StatsUtil/floatStr (.get_process_latency_ms ss))})))

(defn- unpack-topology-page-info
  "Unpacks the serialized object to data structures"
  [^TopologyPageInfo topo-info window secure?]
  (let [id (.get_id topo-info)
        ^TopologyStats topo-stats (.get_topology_stats topo-info)
        stat->window->number
          {:emitted (.get_window_to_emitted topo-stats)
           :transferred (.get_window_to_transferred topo-stats)
           :complete-latencies (.get_window_to_complete_latencies_ms topo-stats)
           :acked (.get_window_to_acked topo-stats)
           :failed (.get_window_to_failed topo-stats)}
        topo-stats (topology-stats window stat->window->number)
        [debugEnabled
         samplingPct] (if-let [debug-opts (.get_debug_options topo-info)]
                        [(.is_enable debug-opts)
                         (.get_samplingpct debug-opts)])
        uptime (.get_uptime_secs topo-info)]
    {"id" id
     "encodedId" (URLEncoder/encode id)
     "owner" (.get_owner topo-info)
     "name" (.get_name topo-info)
     "status" (.get_status topo-info)
     "uptime" (UIHelpers/prettyUptimeSec uptime)
     "uptimeSeconds" uptime
     "tasksTotal" (.get_num_tasks topo-info)
     "workersTotal" (.get_num_workers topo-info)
     "executorsTotal" (.get_num_executors topo-info)
     "schedulerInfo" (.get_sched_status topo-info)
     "requestedMemOnHeap" (.get_requested_memonheap topo-info)
     "requestedMemOffHeap" (.get_requested_memoffheap topo-info)
     "requestedCpu" (.get_requested_cpu topo-info)
     "requestedTotalMem" (+ (.get_requested_memonheap topo-info) (.get_requested_memoffheap topo-info))
     "assignedMemOnHeap" (.get_assigned_memonheap topo-info)
     "assignedMemOffHeap" (.get_assigned_memoffheap topo-info)
     "assignedTotalMem" (+ (.get_assigned_memonheap topo-info) (.get_assigned_memoffheap topo-info))
     "assignedCpu" (.get_assigned_cpu topo-info)
     "requestedRegularOnHeapMem" (.get_requested_regular_on_heap_memory topo-info)
     "requestedSharedOnHeapMem" (.get_requested_shared_on_heap_memory topo-info)
     "requestedRegularOffHeapMem" (.get_requested_regular_off_heap_memory topo-info)
     "requestedSharedOffHeapMem" (.get_requested_shared_off_heap_memory topo-info)
     "assignedRegularOnHeapMem" (.get_assigned_regular_on_heap_memory topo-info)
     "assignedSharedOnHeapMem" (.get_assigned_shared_on_heap_memory topo-info)
     "assignedRegularOffHeapMem" (.get_assigned_regular_off_heap_memory topo-info)
     "assignedSharedOffHeapMem" (.get_assigned_shared_off_heap_memory topo-info)
     "topologyStats" topo-stats
     "workers"  (map (partial worker-summary-to-json secure?)
                     (.get_workers topo-info))
     "spouts" (map (partial comp-agg-stats-json id secure?)
                   (.get_id_to_spout_agg_stats topo-info))
     "bolts" (map (partial comp-agg-stats-json id secure?)
                  (.get_id_to_bolt_agg_stats topo-info))
     "configuration" (.get_topology_conf topo-info)
     "debug" (or debugEnabled false)
     "samplingPct" (or samplingPct 10)
     "replicationCount" (.get_replication_count topo-info)
     "topologyVersion" (.get_topology_version topo-info)
     "stormVersion" (.get_storm_version topo-info)}))

(defn exec-host-port
  [executors]
  (for [^ExecutorSummary e executors]
    {"host" (.get_host e)
     "port" (.get_port e)}))

(defn worker-host-port
  "Get the set of all worker host/ports"
  [id]
  (thrift/with-configured-nimbus-connection nimbus
    (distinct (exec-host-port (.get_executors (get-topology-info nimbus id))))))

(defn topology-page [id window include-sys? user secure?]
  (thrift/with-configured-nimbus-connection nimbus
    (let [window (if window window ":all-time")
          window-hint (window-hint window)
          topo-page-info (.getTopologyPageInfo ^Nimbus$Client nimbus
                                               id
                                               window
                                               include-sys?)
          topology-conf (from-json (.get_topology_conf topo-page-info))
          msg-timeout (topology-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)]
      (merge
       (unpack-topology-page-info topo-page-info window secure?)
       {"user" user
        "window" window
        "windowHint" window-hint
        "msgTimeout" msg-timeout
        "configuration" topology-conf
        "visualizationTable" []
        "schedulerDisplayResource" (*STORM-CONF* SCHEDULER-DISPLAY-RESOURCE)}))))

(defn- sum
  [vals]
  (reduce + vals))

(defn- average
  [vals]
  (/ (sum vals) (count vals)))

(defn- merge-with-conj [& mlist]
  (let [flatten-keys (set (filter identity (flatten (map keys mlist))))
        dict-keys-with-empty-list (zipmap flatten-keys (repeat '()))]
    (apply merge-with conj dict-keys-with-empty-list mlist)))

(defn- conj-specific-stats-by-field [window field-fn stats]
  (apply merge-with-conj (map #(into {} (.get (field-fn %) window)) stats)))

(defn- reduce-conj-specific-stats-by-field [agg-val-fn stats-map]
  (let [fn-key-to-str (fn [key]
                        (condp instance? key
                          String {"stream_id" key}
                          GlobalStreamId {"component_id" (.get_componentId key) "stream_id" (.get_streamId key)}
                          {"stream_id" (str key)}))]
    (reduce-kv #(conj %1 (merge (fn-key-to-str %2) {"value" (agg-val-fn %3)})) '() stats-map)))

(defn- merge-stats-specific-field-by-stream
  [window field-fn agg-val-fn stats]
  (reduce-conj-specific-stats-by-field agg-val-fn
    (conj-specific-stats-by-field window field-fn stats)))

(defn- merge-executor-common-stats
  [window executor-stats]
  {"emitted"
   (merge-stats-specific-field-by-stream window #(.get_emitted %) sum executor-stats)
   "transferred"
   (merge-stats-specific-field-by-stream window #(.get_transferred %) sum executor-stats)
   })

(defmulti merge-executor-specific-stats
  (fn [_ specific-stats]
    (if (.is_set_spout (first specific-stats)) :spout :bolt)))

(defmethod merge-executor-specific-stats :spout
  [window specific-stats]
  (let [stats (map #(.get_spout %) specific-stats)]
    {"acked"
     (merge-stats-specific-field-by-stream window #(.get_acked %) sum stats)
     "failed"
     (merge-stats-specific-field-by-stream window #(.get_failed %) sum stats)
     "complete_ms_avg"
     (merge-stats-specific-field-by-stream window #(.get_complete_ms_avg %) #(StatsUtil/floatStr (average %)) stats)
     }
    ))

(defmethod merge-executor-specific-stats :bolt
  [window specific-stats]
  (let [stats (map #(.get_bolt %) specific-stats)]
    {"acked"
     (merge-stats-specific-field-by-stream window #(.get_acked %) sum stats)
     "failed"
     (merge-stats-specific-field-by-stream window #(.get_failed %) sum stats)
     "process_ms_avg"
     (merge-stats-specific-field-by-stream window #(.get_process_ms_avg %) #(StatsUtil/floatStr (average %)) stats)
     "executed"
     (merge-stats-specific-field-by-stream window #(.get_executed %) sum stats)
     "executed_ms_avg"
     (merge-stats-specific-field-by-stream window #(.get_execute_ms_avg %) #(StatsUtil/floatStr (average %)) stats)
     }
    ))

(defn merge-executor-stats [window component-id eslist]
  (let [stats (map #(.get_stats %) eslist)
        specific-stats (map #(.get_specific %) stats)]
    (merge {"id" component-id}
           (merge-executor-common-stats window stats)
           (merge-executor-specific-stats window specific-stats))))

(defn topology-metrics-page [id window include-sys?]
  (thrift/with-configured-nimbus-connection nimbus
    (let [window (if window window ":all-time")
          window-hint (window-hint window)
          topology (.getTopology ^Nimbus$Client nimbus id)
          summ (->> (doto
                      (GetInfoOptions.)
                      (.set_num_err_choice NumErrorsChoice/NONE))
                    (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
          execs (.get_executors summ)
          spout-summs (filter (partial spout-summary? topology) execs)
          bolt-summs (filter (partial bolt-summary? topology) execs)
          spout-comp-summs (group-by-comp spout-summs)
          bolt-comp-summs (group-by-comp bolt-summs)
          bolt-comp-summs (filter-key (mk-include-sys-fn include-sys?)
                                      bolt-comp-summs)
          merged-spout-stats (map (fn [[k v]] (merge-executor-stats window k v)) spout-comp-summs)
          merged-bolt-stats (map (fn [[k v]] (merge-executor-stats window k v)) bolt-comp-summs)]
      (merge {"window" window "window-hint" window-hint "spouts" merged-spout-stats "bolts" merged-bolt-stats}))))

(defn topology-lag [id topology-conf]
  (thrift/with-configured-nimbus-connection nimbus
    (let [topology (.getUserTopology ^Nimbus$Client nimbus
                                               id)]
      (TopologySpoutLag/lag topology topology-conf))))

(defn component-errors
  [errors-list topology-id secure?]
  (let [errors (->> errors-list
                    (sort-by #(.get_error_time_secs ^ErrorInfo %))
                    reverse)]
    {"componentErrors"
     (for [^ErrorInfo e errors]
       {"errorTime" (get-error-time e)
        "errorHost" (.get_host e)
        "errorPort"  (.get_port e)
        "errorWorkerLogLink"  (worker-log-link (.get_host e)
                                               (.get_port e)
                                               topology-id
                                               secure?)
        "errorLapsedSecs" (if-let [t (get-error-time e)] (Time/deltaSecs t))
        "error" (.get_error e)})}))

(defmulti unpack-comp-agg-stat
  (fn [[_ ^ComponentAggregateStats s]] (.get_type s)))

(defmethod unpack-comp-agg-stat ComponentType/BOLT
  [[window ^ComponentAggregateStats s]]
  (let [^CommonAggregateStats comm-s (.get_common_stats s)
        ^SpecificAggregateStats spec-s (.get_specific_stats s)
        ^BoltAggregateStats bolt-s (.get_bolt spec-s)]
    {"window" window
     "windowPretty" (window-hint window)
     "emitted" (.get_emitted comm-s)
     "transferred" (.get_transferred comm-s)
     "acked" (.get_acked comm-s)
     "failed" (.get_failed comm-s)
     "executeLatency" (StatsUtil/floatStr (.get_execute_latency_ms bolt-s))
     "processLatency"  (StatsUtil/floatStr (.get_process_latency_ms bolt-s))
     "executed" (.get_executed bolt-s)
     "capacity" (StatsUtil/floatStr (.get_capacity bolt-s))}))

(defmethod unpack-comp-agg-stat ComponentType/SPOUT
  [[window ^ComponentAggregateStats s]]
  (let [^CommonAggregateStats comm-s (.get_common_stats s)
        ^SpecificAggregateStats spec-s (.get_specific_stats s)
        ^SpoutAggregateStats spout-s (.get_spout spec-s)]
    {"window" window
     "windowPretty" (window-hint window)
     "emitted" (.get_emitted comm-s)
     "transferred" (.get_transferred comm-s)
     "acked" (.get_acked comm-s)
     "failed" (.get_failed comm-s)
     "completeLatency" (StatsUtil/floatStr (.get_complete_latency_ms spout-s))}))

(defn- unpack-bolt-input-stat
  [[^GlobalStreamId s ^ComponentAggregateStats stats]]
  (let [^SpecificAggregateStats sas (.get_specific_stats stats)
        ^BoltAggregateStats bas (.get_bolt sas)
        ^CommonAggregateStats cas (.get_common_stats stats)
        comp-id (.get_componentId s)]
    {"component" comp-id
     "encodedComponentId" (URLEncoder/encode comp-id)
     "stream" (.get_streamId s)
     "executeLatency" (StatsUtil/floatStr (.get_execute_latency_ms bas))
     "processLatency" (StatsUtil/floatStr (.get_process_latency_ms bas))
     "executed" (if-let [e (.get_executed bas)] e 0)
     "acked" (if-let [a (.get_acked cas)] a 0)
     "failed" (if-let [f (.get_failed cas)] f 0)}))

(defmulti unpack-comp-output-stat
  (fn [[_ ^ComponentAggregateStats s]] (.get_type s)))

(defmethod unpack-comp-output-stat ComponentType/BOLT
  [[stream-id ^ComponentAggregateStats stats]]
  (let [^CommonAggregateStats cas (.get_common_stats stats)]
    {"stream" stream-id
     "emitted" (if-let [e (.get_emitted cas)] e 0)
     "transferred" (if-let [t (.get_transferred cas)] t 0)}))

(defmethod unpack-comp-output-stat ComponentType/SPOUT
  [[stream-id ^ComponentAggregateStats stats]]
  (let [^CommonAggregateStats cas (.get_common_stats stats)
        ^SpecificAggregateStats spec-s (.get_specific_stats stats)
        ^SpoutAggregateStats spout-s (.get_spout spec-s)]
    {"stream" stream-id
     "emitted" (if-let [e (.get_emitted cas)] e 0)
     "transferred" (if-let [t (.get_transferred cas)] t 0)
     "completeLatency" (StatsUtil/floatStr (.get_complete_latency_ms spout-s))
     "acked" (if-let [a (.get_acked cas)] a 0)
     "failed" (if-let [f (.get_failed cas)] f 0)}))

(defmulti unpack-comp-exec-stat
  (fn [_ _ ^ComponentAggregateStats cas] (.get_type (.get_stats ^ExecutorAggregateStats cas))))

(defmethod unpack-comp-exec-stat ComponentType/BOLT
  [topology-id secure? ^ExecutorAggregateStats eas]
  (let [^ExecutorSummary summ (.get_exec_summary eas)
        ^ExecutorInfo info (.get_executor_info summ)
        ^ComponentAggregateStats stats (.get_stats eas)
        ^SpecificAggregateStats ss (.get_specific_stats stats)
        ^BoltAggregateStats bas (.get_bolt ss)
        ^CommonAggregateStats cas (.get_common_stats stats)
        host (.get_host summ)
        port (.get_port summ)
        exec-id (UIHelpers/prettyExecutorInfo info)
        uptime (.get_uptime_secs summ)]
    {"id" exec-id
     "encodedId" (URLEncoder/encode exec-id)
     "uptime" (UIHelpers/prettyUptimeSec uptime)
     "uptimeSeconds" uptime
     "host" host
     "port" port
     "emitted" (if-let [e (.get_emitted cas)] e 0)
     "transferred" (if-let [t (.get_transferred cas)] t 0)
     "capacity" (StatsUtil/floatStr (Utils/nullToZero (.get_capacity bas)))
     "executeLatency" (StatsUtil/floatStr (.get_execute_latency_ms bas))
     "executed" (if-let [e (.get_executed bas)] e 0)
     "processLatency" (StatsUtil/floatStr (.get_process_latency_ms bas))
     "acked" (if-let [a (.get_acked cas)] a 0)
     "failed" (if-let [f (.get_failed cas)] f 0)
     "workerLogLink" (worker-log-link host port topology-id secure?)}))

(defmethod unpack-comp-exec-stat ComponentType/SPOUT
  [topology-id secure? ^ExecutorAggregateStats eas]
  (let [^ExecutorSummary summ (.get_exec_summary eas)
        ^ExecutorInfo info (.get_executor_info summ)
        ^ComponentAggregateStats stats (.get_stats eas)
        ^SpecificAggregateStats ss (.get_specific_stats stats)
        ^SpoutAggregateStats sas (.get_spout ss)
        ^CommonAggregateStats cas (.get_common_stats stats)
        host (.get_host summ)
        port (.get_port summ)
        exec-id (UIHelpers/prettyExecutorInfo info)
        uptime (.get_uptime_secs summ)]
    {"id" exec-id
     "encodedId" (URLEncoder/encode exec-id)
     "uptime" (UIHelpers/prettyUptimeSec uptime)
     "uptimeSeconds" uptime
     "host" host
     "port" port
     "emitted" (if-let [em (.get_emitted cas)] em 0)
     "transferred" (if-let [t (.get_transferred cas)] t 0)
     "completeLatency" (StatsUtil/floatStr (.get_complete_latency_ms sas))
     "acked" (if-let [ack (.get_acked cas)] ack 0)
     "failed" (if-let [f (.get_failed cas)] f 0)
     "workerLogLink" (worker-log-link host port topology-id secure?)}))

(defmulti unpack-component-page-info
  "Unpacks component-specific info to clojure data structures"
  (fn [^ComponentPageInfo info & _]
    (.get_component_type info)))

(defmethod unpack-component-page-info ComponentType/BOLT
  [^ComponentPageInfo info topology-id window include-sys? secure?]
  (merge
    {"boltStats" (map unpack-comp-agg-stat (.get_window_to_stats info))
     "inputStats" (map unpack-bolt-input-stat (.get_gsid_to_input_stats info))
     "outputStats" (map unpack-comp-output-stat (.get_sid_to_output_stats info))
     "executorStats" (map (partial unpack-comp-exec-stat topology-id secure?)
                          (.get_exec_stats info))}
    (-> info .get_errors (component-errors topology-id secure?))))

(defmethod unpack-component-page-info ComponentType/SPOUT
  [^ComponentPageInfo info topology-id window include-sys? secure?]
  (merge
    {"spoutSummary" (map unpack-comp-agg-stat (.get_window_to_stats info))
     "outputStats" (map unpack-comp-output-stat (.get_sid_to_output_stats info))
     "executorStats" (map (partial unpack-comp-exec-stat topology-id secure?)
                          (.get_exec_stats info))}
    (-> info .get_errors (component-errors topology-id secure?))))

(defn clojurify-profile-request
  [^ProfileRequest request]
  (when request
    {:host (.get_node (.get_nodeInfo request))
     :port (first (.get_port (.get_nodeInfo request)))
     :action     (.get_action request)
     :timestamp  (.get_time_stamp request)}))

(defn get-active-profile-actions
  [nimbus topology-id component]
  (let [profile-actions  (.getComponentPendingProfileActions nimbus
                                               topology-id
                                               component
                                 ProfileAction/JPROFILE_STOP)
        latest-profile-actions (map clojurify-profile-request profile-actions)
        active-actions (map (fn [profile-action]
                              {"host" (:host profile-action)
                               "port" (str (:port profile-action))
                               "dumplink" (worker-dump-link (:host profile-action) (str (:port profile-action)) topology-id)
                               "timestamp" (str (- (:timestamp profile-action) (System/currentTimeMillis)))})
                            latest-profile-actions)]
    (log-message "Latest-active actions are: " (pr-str active-actions))
    active-actions))

(defn component-page
  [topology-id component window include-sys? user secure?]
  (thrift/with-configured-nimbus-connection nimbus
    (let [window (or window ":all-time")
          window-hint (window-hint window)
          comp-page-info (.getComponentPageInfo ^Nimbus$Client nimbus
                                                topology-id
                                                component
                                                window
                                                include-sys?)
          topology-conf (from-json (.getTopologyConf ^Nimbus$Client nimbus
                                                     topology-id))
          msg-timeout (topology-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)
          [debugEnabled
           samplingPct] (if-let [debug-opts (.get_debug_options comp-page-info)]
                          [(.is_enable debug-opts)
                           (.get_samplingpct debug-opts)])]
      (assoc
       (unpack-component-page-info comp-page-info
                                   topology-id
                                   window
                                   include-sys?
                                   secure?)
       "user" user
       "id" component
       "encodedId" (URLEncoder/encode component)
       "name" (.get_topology_name comp-page-info)
       "executors" (.get_num_executors comp-page-info)
       "tasks" (.get_num_tasks comp-page-info)
       "requestedMemOnHeap" (.get (.get_resources_map comp-page-info) Config/TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)
       "requestedMemOffHeap" (.get (.get_resources_map comp-page-info) Config/TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)
       "requestedCpu" (.get (.get_resources_map comp-page-info) Config/TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)
       "schedulerDisplayResource" (*STORM-CONF* SCHEDULER-DISPLAY-RESOURCE)
       "topologyId" topology-id
       "topologyStatus" (.get_topology_status comp-page-info)
       "encodedTopologyId" (URLEncoder/encode topology-id)
       "window" window
       "componentType" (-> comp-page-info .get_component_type str lower-case)
       "windowHint" window-hint
       "debug" (or debugEnabled false)
       "samplingPct" (or samplingPct 10)
       "eventLogLink" (event-log-link topology-id
                                      component
                                      (.get_eventlog_host comp-page-info)
                                      (.get_eventlog_port comp-page-info)
                                      secure?)
       "profilingAndDebuggingCapable" (not (Utils/isOnWindows))
       "profileActionEnabled" (*STORM-CONF* WORKER-PROFILER-ENABLED)
       "profilerActive" (if (*STORM-CONF* WORKER-PROFILER-ENABLED)
                          (get-active-profile-actions nimbus topology-id component)
                          [])))))

(defn unpack-owner-resource-summary [summary]
  (let [memory-guarantee (if (.is_set_memory_guarantee summary)
                           (.get_memory_guarantee summary)
                           "N/A")
        cpu-guarantee (if (.is_set_cpu_guarantee summary)
                        (.get_cpu_guarantee summary)
                        "N/A")
        isolated-node-guarantee (if (.is_set_isolated_node_guarantee summary)
                                  (.get_isolated_node_guarantee summary)
                                  "N/A")
        memory-guarantee-remaining (if (.is_set_memory_guarantee_remaining summary)
                                     (.get_memory_guarantee_remaining summary)
                                     "N/A")
        cpu-guarantee-remaining (if (.is_set_cpu_guarantee_remaining summary)
                                  (.get_cpu_guarantee_remaining summary)
                                  "N/A")]
    {"owner" (.get_owner summary)
     "totalTopologies" (.get_total_topologies summary)
     "totalExecutors" (.get_total_executors summary)
     "totalWorkers" (.get_total_workers summary)
     "totalTasks" (.get_total_tasks summary)
     "totalMemoryUsage" (.get_memory_usage summary)
     "totalCpuUsage" (.get_cpu_usage summary)
     "memoryGuarantee" memory-guarantee
     "cpuGuarantee" cpu-guarantee
     "isolatedNodes" isolated-node-guarantee
     "memoryGuaranteeRemaining" memory-guarantee-remaining
     "cpuGuaranteeRemaining" cpu-guarantee-remaining
     "totalReqOnHeapMem" (.get_requested_on_heap_memory summary)
     "totalReqOffHeapMem" (.get_requested_off_heap_memory summary)
     "totalReqMem" (.get_requested_total_memory summary)
     "totalReqCpu" (.get_requested_cpu summary)
     "totalAssignedOnHeapMem" (.get_assigned_on_heap_memory summary)
     "totalAssignedOffHeapMem" (.get_assigned_off_heap_memory summary)}))

(defn owner-resource-summaries []
  (thrift/with-configured-nimbus-connection nimbus
    (let [summaries (.getOwnerResourceSummaries nimbus nil)]
      {"schedulerDisplayResource" (*STORM-CONF* SCHEDULER-DISPLAY-RESOURCE)
       "owners"
         (for [summary summaries]
           (unpack-owner-resource-summary summary))})))

(defn owner-resource-summary [owner]
  (thrift/with-configured-nimbus-connection nimbus
    (let [summaries (.getOwnerResourceSummaries nimbus owner)]
      (merge {"schedulerDisplayResource" (*STORM-CONF* SCHEDULER-DISPLAY-RESOURCE)}
             (if (empty? summaries)
               ;; send a default value, we couldn't find topos by that owner
               (unpack-owner-resource-summary (OwnerResourceSummary. owner))
               (let [topologies (.get_topologies (.getClusterInfo ^Nimbus$Client nimbus))
                     data (get-topologies-map topologies :conditional (fn [t] (= (.get_owner t) owner)))]
                 (merge {"topologies" data}
                        (unpack-owner-resource-summary (first summaries)))))))))

(defn- level-to-dict [level]
  (if level
    (let [timeout (.get_reset_log_level_timeout_secs level)
          timeout-epoch (.get_reset_log_level_timeout_epoch level)
          target-level (.get_target_log_level level)
          reset-level (.get_reset_log_level level)]
          {"target_level" (.toString (Level/toLevel target-level))
           "reset_level" (.toString (Level/toLevel reset-level))
           "timeout" timeout
           "timeout_epoch" timeout-epoch})))

(defn log-config [topology-id]
  (thrift/with-configured-nimbus-connection
    nimbus
    (let [log-config (.getLogConfig ^Nimbus$Client nimbus topology-id)
          named-logger-levels (into {}
                                (for [[key val] (.get_named_logger_level log-config)]
                                  [(str key) (level-to-dict val)]))]
      {"namedLoggerLevels" named-logger-levels})))

(defn topology-config [topology-id]
  (thrift/with-configured-nimbus-connection nimbus
    (from-json (.getTopologyConf ^Nimbus$Client nimbus topology-id))))

(defn topology-op-response [topology-id op]
  {"topologyOperation" op,
   "topologyId" topology-id,
   "status" "success"
   })

(defn component-op-response [topology-id component-id op]
  {"topologyOperation" op,
   "topologyId" topology-id,
   "componentId" component-id,
   "status" "success"
   })

(defn check-include-sys?
  [sys?]
  (if (or (nil? sys?) (= "false" sys?)) false true))

(def http-creds-handler (AuthUtils/GetUiHttpCredentialsPlugin *STORM-CONF*))

(defn populate-context!
  "Populate the Storm RequestContext from an servlet-request. This should be called in each handler"
  [servlet-request]
    (when http-creds-handler
      (.populateContext http-creds-handler (ReqContext/context) servlet-request)))

(defn get-user-name
  [servlet-request]
  (.getUserName http-creds-handler servlet-request))

(defn json-profiling-disabled
  "Return a JSON response communicating that profiling is disabled and
  therefore unavailable."
  [callback]
  (json-response {"status" "disabled",
                  "message" "Profiling is not enabled on this server"}
                 callback
                 :status 501))

(defroutes main-routes
  (GET "/api/v1/cluster/configuration" [& m]
    (.mark ui:num-cluster-configuration-http-requests)
    (json-response (cluster-configuration)
                   (:callback m) :need-serialize false))
  (GET "/api/v1/cluster/summary" [:as {:keys [cookies servlet-request]} & m]
    (.mark ui:num-cluster-summary-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getClusterInfo")
    (let [user (get-user-name servlet-request)]
      (json-response (assoc (cluster-summary user)
                          "bugtracker-url" (*STORM-CONF* UI-PROJECT-BUGTRACKER-URL)
                          "central-log-url" (*STORM-CONF* UI-CENTRAL-LOGGING-URL)) (:callback m))))
  (GET "/api/v1/nimbus/summary" [:as {:keys [cookies servlet-request]} & m]
    (.mark ui:num-nimbus-summary-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getClusterInfo")
    (json-response (nimbus-summary) (:callback m)))
  (GET "/api/v1/owner-resources" [:as {:keys [cookies servlet-request scheme]} id & m]
    (.mark ui:num-get-owner-resource-summaries-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getOwnerResourceSummaries")
    (json-response (owner-resource-summaries) (:callback m)))
  (GET "/api/v1/owner-resources/:id" [:as {:keys [cookies servlet-request scheme]} id & m]
    (.mark ui:num-get-owner-resource-summaries-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getOwnerResourceSummaries")
    (json-response (owner-resource-summary id) (:callback m)))
  (GET "/api/v1/history/summary" [:as {:keys [cookies servlet-request]} & m]
    (let [user (.getUserName http-creds-handler servlet-request)]
      (json-response (topology-history-info user) (:callback m))))
  (GET "/api/v1/supervisor/summary" [:as {:keys [cookies servlet-request]} & m]
    (.mark ui:num-supervisor-summary-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getClusterInfo")
    (json-response (assoc (supervisor-summary)
                     "logviewerPort" (*STORM-CONF* LOGVIEWER-PORT)) (:callback m)))
  (GET "/api/v1/supervisor" [:as {:keys [cookies servlet-request scheme]} & m]
    (.mark ui:num-supervisor-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getSupervisorPageInfo")
    ;; supervisor takes either an id or a host query parameter, and technically both
    ;; that said, if both the id and host are provided, the id wins
    (let [id (:id m)
          host (:host m)]
      (json-response (assoc (supervisor-page-info id host (check-include-sys? (:sys m)) (= scheme :https))
                            "logviewerPort" (*STORM-CONF* LOGVIEWER-PORT)) (:callback m))))
  (GET "/api/v1/topology/summary" [:as {:keys [cookies servlet-request]} & m]
    (.mark ui:num-all-topologies-summary-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getClusterInfo")
    (json-response (all-topologies-summary) (:callback m)))
  (GET  "/api/v1/topology-workers/:id" [:as {:keys [cookies servlet-request]} id & m]
    (let [id (URLDecoder/decode id)]
      (json-response {"hostPortList" (worker-host-port id)
                      "logviewerPort" (*STORM-CONF* LOGVIEWER-PORT)} (:callback m))))
  (GET "/api/v1/topology/:id" [:as {:keys [cookies servlet-request scheme]} id & m]
    (.mark ui:num-topology-page-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopology" (topology-config id))
    (let [user (get-user-name servlet-request)]
      (json-response (topology-page id (:window m) (check-include-sys? (:sys m)) user (= scheme :https)) (:callback m))))
  (GET "/api/v1/topology/:id/metrics" [:as {:keys [cookies servlet-request]} id & m]
    (.mark ui:num-topology-metric-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopology" (topology-config id))
    (json-response (topology-metrics-page id (:window m) (check-include-sys? (:sys m))) (:callback m)))
  (GET "/api/v1/topology/:id/lag" [:as {:keys [cookies servlet-request scheme]} id & m]
    (.mark ui:num-topology-lag-http-requests)
    (populate-context! servlet-request)
    (let [topology-conf (topology-config id)]
      (assert-authorized-user "getUserTopology" topology-conf)
      (json-response (topology-lag id topology-conf) nil)))
  (GET "/api/v1/topology/:id/visualization-init" [:as {:keys [cookies servlet-request]} id & m]
    (.mark ui:num-build-visualization-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopology" (topology-config id))
    (json-response (build-visualization id (:window m) (check-include-sys? (:sys m))) (:callback m)))
  (GET "/api/v1/topology/:id/visualization" [:as {:keys [cookies servlet-request]} id & m]
    (.mark ui:num-mk-visualization-data-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopology" (topology-config id))
    (json-response (mk-visualization-data id (:window m) (check-include-sys? (:sys m))) (:callback m)))
  (GET "/api/v1/topology/:id/component/:component" [:as {:keys [cookies servlet-request scheme]} id component & m]
    (.mark ui:num-component-page-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopology" (topology-config id))
    (let [user (get-user-name servlet-request)]
      (json-response
          (component-page id component (:window m) (check-include-sys? (:sys m)) user (= scheme :https))
          (:callback m))))
  (GET "/api/v1/topology/:id/logconfig" [:as {:keys [cookies servlet-request]} id & m]
    (.mark ui:num-log-config-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "getTopology" (topology-config id))
    (json-response (log-config id) (:callback m)))
  (POST "/api/v1/topology/:id/activate" [:as {:keys [cookies servlet-request]} id & m]
    (.mark ui:num-activate-topology-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "activate" (topology-config id))
    (thrift/with-configured-nimbus-connection nimbus
       (let [tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
            name (.get_name tplg)]
        (.activate nimbus name)
        (log-message "Activating topology '" name "'")))
    (json-response (topology-op-response id "activate") (m "callback")))
  (POST "/api/v1/topology/:id/deactivate" [:as {:keys [cookies servlet-request]} id & m]
    (.mark ui:num-deactivate-topology-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "deactivate" (topology-config id))
    (thrift/with-configured-nimbus-connection nimbus
        (let [tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
            name (.get_name tplg)]
        (.deactivate nimbus name)
        (log-message "Deactivating topology '" name "'")))
    (json-response (topology-op-response id "deactivate") (m "callback")))
  (POST "/api/v1/topology/:id/debug/:action/:spct" [:as {:keys [cookies servlet-request]} id action spct & m]
    (.mark ui:num-debug-topology-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "debug" (topology-config id))
    (thrift/with-configured-nimbus-connection nimbus
        (let [tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                   (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
            name (.get_name tplg)
            enable? (= "enable" action)]
        (.debug nimbus name "" enable? (Integer/parseInt spct))
        (log-message "Debug topology [" name "] action [" action "] sampling pct [" spct "]")))
    (json-response (topology-op-response id (str "debug/" action)) (m "callback")))
  (POST "/api/v1/topology/:id/component/:component/debug/:action/:spct" [:as {:keys [cookies servlet-request]} id component action spct & m]
    (.mark ui:num-component-op-response-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "debug" (topology-config id))
    (thrift/with-configured-nimbus-connection nimbus
      (let [tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                   (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
            name (.get_name tplg)
            enable? (= "enable" action)]
        (.debug nimbus name component enable? (Integer/parseInt spct))
        (log-message "Debug topology [" name "] component [" component "] action [" action "] sampling pct [" spct "]")))
    (json-response (component-op-response id component (str "/debug/" action)) (m "callback")))
  (POST "/api/v1/topology/:id/rebalance/:wait-time" [:as {:keys [cookies servlet-request]} id wait-time & m]
    (.mark ui:num-topology-op-response-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "rebalance" (topology-config id))
    (thrift/with-configured-nimbus-connection nimbus
      (let [tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
            name (.get_name tplg)
            rebalance-options (m "rebalanceOptions")
            options (RebalanceOptions.)]
        (.set_wait_secs options (Integer/parseInt wait-time))
        (if (and (not-nil? rebalance-options) (contains? rebalance-options "numWorkers"))
          (.set_num_workers options (Integer/parseInt (.toString (rebalance-options "numWorkers")))))
        (if (and (not-nil? rebalance-options) (contains? rebalance-options "executors"))
          (doseq [keyval (rebalance-options "executors")]
            (.put_to_num_executors options (key keyval) (Integer/parseInt (.toString (val keyval))))))
        (.rebalance nimbus name options)
        (log-message "Rebalancing topology '" name "' with wait time: " wait-time " secs")))
    (json-response (topology-op-response id "rebalance") (m "callback")))
  (POST "/api/v1/topology/:id/kill/:wait-time" [:as {:keys [cookies servlet-request]} id wait-time & m]
    (.mark ui:num-topology-op-response-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "killTopology" (topology-config id))
    (thrift/with-configured-nimbus-connection nimbus
      (let [tplg (->> (doto
                        (GetInfoOptions.)
                        (.set_num_err_choice NumErrorsChoice/NONE))
                      (.getTopologyInfoWithOpts ^Nimbus$Client nimbus id))
            name (.get_name tplg)
            options (KillOptions.)]
        (.set_wait_secs options (Integer/parseInt wait-time))
        (.killTopologyWithOpts nimbus name options)
        (log-message "Killing topology '" name "' with wait time: " wait-time " secs")))
    (json-response (topology-op-response id "kill") (m "callback")))
  (POST "/api/v1/topology/:id/logconfig" [:as {:keys [cookies servlet-request]} id namedLoggerLevels & m]
    (.mark ui:num-topology-op-response-http-requests)
    (populate-context! servlet-request)
    (assert-authorized-user "setLogConfig" (topology-config id))
    (thrift/with-configured-nimbus-connection
      nimbus
      (let [new-log-config (LogConfig.)]
        (doseq [[key level] namedLoggerLevels]
            (let [logger-name (str key)
                  target-level (.get level "target_level")
                  timeout (or (.get level "timeout") 0)
                  named-logger-level (LogLevel.)]
              ;; if target-level is nil, do not set it, user wants to clear
              (log-message "The target level for " logger-name " is " target-level)
              (if (nil? target-level)
                (do
                  (.set_action named-logger-level LogLevelAction/REMOVE)
                  (.unset_target_log_level named-logger-level))
                (do
                  (.set_action named-logger-level LogLevelAction/UPDATE)
                  ;; the toLevel here ensures the string we get is valid
                  (.set_target_log_level named-logger-level (.name (Level/toLevel target-level)))
                  (.set_reset_log_level_timeout_secs named-logger-level timeout)))
              (log-message "Adding this " logger-name " " named-logger-level " to " new-log-config)
              (.put_to_named_logger_level new-log-config logger-name named-logger-level)))
        (log-message "Setting topology " id " log config " new-log-config)
        (.setLogConfig nimbus id new-log-config)
        (json-response (log-config id) (m "callback")))))

  (GET "/api/v1/topology/:id/profiling/start/:host-port/:timeout"
       [:as {:keys [servlet-request]} id host-port timeout & m]
       (if (get *STORM-CONF* WORKER-PROFILER-ENABLED)
         (do
           (populate-context! servlet-request)
           (thrift/with-configured-nimbus-connection nimbus
             (assert-authorized-user "setWorkerProfiler" (topology-config id))
             (let [[host, port] (split host-port #":")
                   nodeinfo (NodeInfo. host (set [(Long. port)]))
                   timestamp (+ (System/currentTimeMillis) (* 60000 (Long. timeout)))
                   request (ProfileRequest. nodeinfo
                                            ProfileAction/JPROFILE_STOP)]
               (.set_time_stamp request timestamp)
               (.setWorkerProfiler nimbus id request)
               (json-response {"status" "ok"
                               "id" host-port
                               "timeout" timeout
                               "dumplink" (worker-dump-link
                                           host
                                           port
                                           id)}
                              (m "callback")))))
         (json-profiling-disabled (m "callback"))))

  (GET "/api/v1/topology/:id/profiling/stop/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (if (get *STORM-CONF* WORKER-PROFILER-ENABLED)
         (do
           (populate-context! servlet-request)
           (thrift/with-configured-nimbus-connection nimbus
             (assert-authorized-user "setWorkerProfiler" (topology-config id))
             (let [[host, port] (split host-port #":")
                   nodeinfo (NodeInfo. host (set [(Long. port)]))
                   timestamp 0
                   request (ProfileRequest. nodeinfo
                                            ProfileAction/JPROFILE_STOP)]
               (.set_time_stamp request timestamp)
               (.setWorkerProfiler nimbus id request)
               (json-response {"status" "ok"
                               "id" host-port}
                              (m "callback")))))
         (json-profiling-disabled (m "callback"))))

  (GET "/api/v1/topology/:id/profiling/dumpprofile/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (if (get *STORM-CONF* WORKER-PROFILER-ENABLED)
         (do
           (populate-context! servlet-request)
           (thrift/with-configured-nimbus-connection nimbus
             (assert-authorized-user "setWorkerProfiler" (topology-config id))
             (let [[host, port] (split host-port #":")
                   nodeinfo (NodeInfo. host (set [(Long. port)]))
                   timestamp (System/currentTimeMillis)
                   request (ProfileRequest. nodeinfo
                                            ProfileAction/JPROFILE_DUMP)]
               (.set_time_stamp request timestamp)
               (.setWorkerProfiler nimbus id request)
               (json-response {"status" "ok"
                               "id" host-port}
                              (m "callback")))))
         (json-profiling-disabled (m "callback"))))

  (GET "/api/v1/topology/:id/profiling/dumpjstack/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (populate-context! servlet-request)
       (thrift/with-configured-nimbus-connection nimbus
         (assert-authorized-user "setWorkerProfiler" (topology-config id))
         (let [[host, port] (split host-port #":")
               nodeinfo (NodeInfo. host (set [(Long. port)]))
               timestamp (System/currentTimeMillis)
               request (ProfileRequest. nodeinfo
                                        ProfileAction/JSTACK_DUMP)]
           (.set_time_stamp request timestamp)
           (.setWorkerProfiler nimbus id request)
           (json-response {"status" "ok"
                           "id" host-port}
                          (m "callback")))))

  (GET "/api/v1/topology/:id/profiling/restartworker/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (populate-context! servlet-request)
       (thrift/with-configured-nimbus-connection nimbus
         (assert-authorized-user "setWorkerProfiler" (topology-config id))
         (let [[host, port] (split host-port #":")
               nodeinfo (NodeInfo. host (set [(Long. port)]))
               timestamp (System/currentTimeMillis)
               request (ProfileRequest. nodeinfo
                                        ProfileAction/JVM_RESTART)]
           (.set_time_stamp request timestamp)
           (.setWorkerProfiler nimbus id request)
           (json-response {"status" "ok"
                           "id" host-port}
                          (m "callback")))))

  (GET "/api/v1/topology/:id/profiling/dumpheap/:host-port"
       [:as {:keys [servlet-request]} id host-port & m]
       (populate-context! servlet-request)
       (thrift/with-configured-nimbus-connection nimbus
         (assert-authorized-user "setWorkerProfiler" (topology-config id))
         (let [[host, port] (split host-port #":")
               nodeinfo (NodeInfo. host (set [(Long. port)]))
               timestamp (System/currentTimeMillis)
               request (ProfileRequest. nodeinfo
                                        ProfileAction/JMAP_DUMP)]
           (.set_time_stamp request timestamp)
           (.setWorkerProfiler nimbus id request)
           (json-response {"status" "ok"
                           "id" host-port}
                          (m "callback")))))

  (GET "/" [:as {cookies :cookies}]
    (.mark ui:num-main-page-http-requests)
    (resp/redirect "/index.html"))
  (route/resources "/")
  (route/not-found "Page not found"))

(defn catch-errors
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception ex
        (json-response (UIHelpers/exceptionToJson ex) ((:query-params request) "callback") :status 500)))))

(def app
  (handler/site (-> main-routes
                    (wrap-json-params)
                    (wrap-multipart-params)
                    (wrap-reload '[org.apache.storm.ui.core])
                    requests-middleware
                    catch-errors)))

(defn start-server!
  []
  (try
    (let [conf *STORM-CONF*
          header-buffer-size (int (.get conf UI-HEADER-BUFFER-BYTES))
          filters-confs [(FilterConfiguration. (conf UI-FILTER) (conf UI-FILTER-PARAMS))]
          https-port (int (or (conf UI-HTTPS-PORT) 0))
          https-ks-path (conf UI-HTTPS-KEYSTORE-PATH)
          https-ks-password (conf UI-HTTPS-KEYSTORE-PASSWORD)
          https-ks-type (conf UI-HTTPS-KEYSTORE-TYPE)
          https-key-password (conf UI-HTTPS-KEY-PASSWORD)
          https-ts-path (conf UI-HTTPS-TRUSTSTORE-PATH)
          https-ts-password (conf UI-HTTPS-TRUSTSTORE-PASSWORD)
          https-ts-type (conf UI-HTTPS-TRUSTSTORE-TYPE)
          https-want-client-auth (conf UI-HTTPS-WANT-CLIENT-AUTH)
          https-need-client-auth (conf UI-HTTPS-NEED-CLIENT-AUTH)]
      (StormMetricsRegistry/startMetricsReporters conf)
      (UIHelpers/stormRunJetty  (int (conf UI-PORT))
                                (conf UI-HOST)
                                https-port
                                (reify IConfigurator
                                  (execute [this server]
                                    (UIHelpers/configSsl server
                                      https-port
                                      https-ks-path
                                      https-ks-password
                                      https-ks-type
                                      https-key-password
                                      https-ts-path
                                      https-ts-password
                                      https-ts-type
                                      https-need-client-auth
                                      https-want-client-auth)
                                    (doseq [connector (.getConnectors server)]
                                      (.setRequestHeaderSize connector header-buffer-size))
                                    (UIHelpers/configFilter server (ring.util.servlet/servlet app) filters-confs)))))
   (catch Exception ex
     (log-error ex))))

(defn -main
  []
  (log-message "Starting ui server for storm version '" STORM-VERSION "'")
  (start-server!))

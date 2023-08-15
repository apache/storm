---
title: Cluster Metrics
layout: documentation
documentation: true
---

# Cluster Metrics

There are lots of metrics to help you monitor a running cluster.  Many of these metrics are still a work in progress and so is the metrics system itself so any of them may change, even between minor version releases.  We will try to keep them as stable as possible, but they should all be considered somewhat unstable. Some of the metrics may also be for experimental features, or features that are not complete yet, so please read the description of the metric before using it for monitoring or alerting.

Also be aware that depending on the metrics system you use, the names are likely to be translated into a different format that is compatible with the system.  Typically this means that the ':' separating character will be replaced with a '.' character.

Most metrics should have the units that they are reported in as a part of the description. For Timers often this is configured by the reporter that is uploading them to your system.  Pay attention because even if the metric name has a time unit in it, it may be false.

Also most metrics, except for gauges and counters, are a collection of numbers, and not a single value.  Often these result in multiple metrics being uploaded to a reporting system, such as percentiles for a histogram, or rates for a meter.  It is dependent on the configured metrics reporter how this happens, or how the name here corresponds to the metric in your reporting system.

## Cluster Metrics (From Nimbus)

These are metrics that come from the active nimbus instance and report the state of the cluster as a whole, as seen by nimbus.

| Metric Name | Type | Description |
|-------------|------|-------------|
| cluster:num-nimbus-leaders | gauge | Number of nimbuses marked as a leader. This should really only ever be 1 in a healthy cluster, or 0 for a short period of time while a failover happens. |
| cluster:num-nimbuses | gauge | Number of nimbuses, leader or standby. |
| cluster:num-supervisors | gauge | Number of supervisors. |
| cluster:num-topologies | gauge | Number of topologies. |
| cluster:num-total-used-workers | gauge | Number of used workers/slots. |
| cluster:num-total-workers | gauge | Number of workers/slots. |
| cluster:total-fragmented-cpu-non-negative | gauge | Total fragmented CPU (% of core).  This is CPU that the system thinks it cannot use because other resources on the node are used up. |
| cluster:total-fragmented-memory-non-negative | gauge | Total fragmented memory (MB).  This is memory that the system thinks it cannot use because other resources on the node are used up.  |
| topologies:assigned-cpu | histogram | CPU scheduled per topology (% of a core) |
| topologies:assigned-mem-off-heap | histogram | Off heap memory scheduled per topology (MB) |
| topologies:assigned-mem-on-heap | histogram | On heap memory scheduled per topology (MB) |
| topologies:num-executors | histogram | Number of executors per topology. |
| topologies:num-tasks | histogram | Number of tasks per topology. |
| topologies:num-workers | histogram | Number of workers per topology. |
| topologies:replication-count | histogram | Replication count per topology. |
| topologies:requested-cpu | histogram | CPU requested per topology  (% of a core). |
| topologies:requested-mem-off-heap | histogram | Off heap memory requested per topology (MB). |
| topologies:requested-mem-on-heap | histogram | On heap memory requested per topology (MB). |
| topologies:uptime-secs | histogram | Uptime per topology (seconds). |
| nimbus:available-cpu-non-negative | gauge | Available cpu on the cluster (% of a core). |
| nimbus:total-cpu | gauge | total CPU on the cluster (% of a core) |
| nimbus:total-memory | gauge | total memory on the cluster MB |
| supervisors:fragmented-cpu | histogram | fragmented cpu per supervisor (% of a core) |
| supervisors:fragmented-mem | histogram | fragmented memory per supervisor (MB) |
| supervisors:num-used-workers | histogram | workers used per supervisor |
| supervisors:num-workers | histogram | number of workers per supervisor |
| supervisors:uptime-secs | histogram | uptime of supervisors |
| supervisors:used-cpu | histogram | cpu used per supervisor (% of a core) |
| supervisors:used-mem | histogram | memory used per supervisor MB |

## Nimbus Metrics

These are metrics that are specific to a nimbus instance.  In many instances only the active nimbus will be reporting these metrics, but they could come from standby nimbus instances as well.

| Metric Name | Type | Description |
|-------------|------|-------------|
| nimbus:files-upload-duration-ms | timer | Time it takes to upload a file from start to finish (Not Blobs, but this may change) |
| nimbus:longest-scheduling-time-ms | gauge | Longest time ever taken so far to schedule. This includes the current scheduling run, which is intended to detect if scheduling is stuck for some reason. |
| nimbus:mkAssignments-Errors | meter | tracks exceptions from mkAssignments |
| nimbus:num-activate-calls | meter | calls to the activate thrift method. |
| nimbus:num-added-executors-per-scheduling | histogram | number of executors added after a scheduling run. |
| nimbus:num-added-slots-per-scheduling | histogram |  number of slots added after a scheduling run. |
| nimbus:num-beginFileUpload-calls | meter | calls to the beginFileUpload thrift method. |
| nimbus:num-blacklisted-supervisor | gauge | Number of supervisors currently marked as blacklisted because they appear to be somewhat unstable. |
| nimbus:num-deactivate-calls | meter | calls to deactivate thrift method. |
| nimbus:num-debug-calls | meter | calls to debug thrift method.|
| nimbus:num-downloadChunk-calls | meter | calls to downloadChunk thrift method. |
| nimbus:num-finishFileUpload-calls | meter | calls to finishFileUpload thrift method.|
| nimbus:num-gained-leadership | meter | number of times this nimbus gained leadership. |
| nimbus:num-getClusterInfo-calls | meter | calls to getClusterInfo thrift method. |
| nimbus:num-getComponentPageInfo-calls | meter | calls to getComponentPageInfo thrift method. |
| nimbus:num-getComponentPendingProfileActions-calls | meter | calls to getComponentPendingProfileActions thrift method. |
| nimbus:num-getLeader-calls | meter | calls to getLeader thrift method. |
| nimbus:num-getLogConfig-calls | meter | calls to getLogConfig thrift method. |
| nimbus:num-getNimbusConf-calls | meter | calls to getNimbusConf thrift method. |
| nimbus:num-getOwnerResourceSummaries-calls | meter | calls to getOwnerResourceSummaries thrift method. |
| nimbus:num-getSupervisorPageInfo-calls | meter | calls to getSupervisorPageInfo thrift method. |
| nimbus:num-getTopology-calls | meter | calls to getTopology thrift method. |
| nimbus:num-getTopologyConf-calls | meter | calls to getTopologyConf thrift method. |
| nimbus:num-getTopologyInfo-calls | meter | calls to getTopologyInfo thrift method. |
| nimbus:num-getTopologyInfoWithOpts-calls | meter | calls to getTopologyInfoWithOpts thrift method includes calls to getTopologyInfo. |
| nimbus:num-getTopologyPageInfo-calls | meter | calls to getTopologyPageInfo thrift method. |
| nimbus:num-getUserTopology-calls | meter | calls to getUserTopology thrift method. |
| nimbus:num-isTopologyNameAllowed-calls | meter | calls to isTopologyNameAllowed thrift method. |
| nimbus:num-killTopology-calls | meter | calls to killTopology thrift method. |
| nimbus:num-killTopologyWithOpts-calls | meter | calls to killTopologyWithOpts thrift method includes calls to killTopology. |
| nimbus:num-launched | meter | number of times a nimbus was launched |
| nimbus:num-lost-leadership | meter | number of times this nimbus lost leadership |
| nimbus:num-negative-resource-events | meter | Any time a resource goes negative (either CPU or Memory).  This metric is not ideal as it is measured in a data structure that is used for internal calculations that may go negative and not actually represent over scheduling of a resource. |
| nimbus:num-net-executors-increase-per-scheduling | histogram | added executors minus removed executors after a scheduling run |
| nimbus:num-net-slots-increase-per-scheduling | histogram | added slots minus removed slots after a scheduling run |
| nimbus:num-rebalance-calls | meter | calls to rebalance thrift method. |
| nimbus:num-removed-executors-per-scheduling | histogram | number of executors removed after a scheduling run |
| nimbus:num-scheduling-timeouts | meter | number of timeouts during scheduling |
| nimbus:num-removed-slots-per-scheduling | histogram | number of slots removed after a scheduling run |
| nimbus:num-setLogConfig-calls | meter | calls to setLogConfig thrift method. |
| nimbus:num-setWorkerProfiler-calls | meter | calls to setWorkerProfiler thrift method. |
| nimbus:num-shutdown-calls | meter | times nimbus is shut down (this may not actually be reported as nimbus is in the middle of shutting down) |
| nimbus:num-submitTopology-calls | meter | calls to submitTopology thrift method. |
| nimbus:num-submitTopologyWithOpts-calls | meter | calls to submitTopologyWithOpts thrift method includes calls to submitTopology. |
| nimbus:num-uploadChunk-calls | meter | calls to uploadChunk thrift method. |
| nimbus:num-uploadNewCredentials-calls | meter | calls to uploadNewCredentials thrift method. |
| nimbus:process-worker-metric-calls | meter | calls to processWorkerMetrics thrift method. |
| nimbus:scheduler-internal-errors | meter | tracks internal scheduling errors |
| nimbus:topology-scheduling-duration-ms | timer | time it takes to do a scheduling run. |
| nimbus:total-available-memory-non-negative | gauge | available memory on the cluster MB |
| nimbuses:uptime-secs | histogram | uptime of nimbuses |
| MetricsCleaner:purgeTimestamp | gauge | last time metrics were purged (Unfinished Feature) |
| RocksDB:metric-failures | meter | generally any failure that happens in the rocksdb metrics store. (Unfinished Feature) |


## DRPC Metrics

Metrics related to DRPC servers.

| Metric Name | Type | Description |
|-------------|------|-------------|
| drpc:HTTP-request-response-duration | timer | how long it takes to execute an http drpc request |
| drpc:num-execute-calls | meter | calls to execute a DRPC request |
| drpc:num-execute-http-requests | meter | http requests to the DRPC server |
| drpc:num-failRequest-calls | meter | calls to failRequest |
| drpc:num-fetchRequest-calls | meter | calls to fetchRequest |
| drpc:num-result-calls | meter | calls to returnResult |
| drpc:num-server-timedout-requests | meter | times a DRPC request timed out without a response |
| drpc:num-shutdown-calls | meter | number of times shutdown is called on the drpc server |

## Logviewer Metrics

Metrics related to the logviewer process. This process currently also handles cleaning up worker logs when they get too large or too old.

| Metric Name | Type | Description |
|-------------|------|-------------|
| logviewer:cleanup-routine-duration-ms | timer | how long it takes to run the log cleanup routine |
| logviewer:deep-search-request-duration-ms | timer | how long it takes for /deepSearch/{topoId} |
| logviewer:disk-space-freed-in-bytes | histogram | number of bytes cleaned up each time through the cleanup routine. |
| logviewer:download-file-size-rounded-MB | histogram | size in MB of files being downloaded |
| logviewer:num-daemonlog-page-http-requests | meter | calls to /daemonlog |
| logviewer:num-deep-search-no-result | meter | number of deep search requests that did not return any results |
| logviewer:num-deep-search-requests-with-archived | meter | calls to /deepSearch/{topoId} with ?search-archived=true |
| logviewer:num-deep-search-requests-without-archived | meter | calls to /deepSearch/{topoId} with ?search-archived=false |
| logviewer:num-download-daemon-log-exceptions | meter | num errors in calls to /daemondownload |
| logviewer:num-download-dump-exceptions | meter | num errors in calls to /dumps/{topo-id}/{host-port}/{filename} |
| logviewer:num-download-log-daemon-file-http-requests | meter | calls to /daemondownload |
| logviewer:num-download-log-exceptions | meter | num errors in calls to /download |
| logviewer:num-download-log-file-http-requests | meter | calls to /download |
| logviewer:num-file-download-exceptions | meter | errors while trying to download files. |
| logviewer:num-file-download-exceptions | meter | number of exceptions trying to download a log file |
| logviewer:num-file-open-exceptions | meter | errors trying to open a file (when deleting logs) |
| logviewer:num-file-open-exceptions | meter | number of exceptions trying to open a log file for serving |
| logviewer:num-file-read-exceptions | meter | number of exceptions trying to read from a log file for serving |
| logviewer:num-file-removal-exceptions | meter | number of exceptions trying to cleanup files. |
| logviewer:num-files-cleaned-up | histogram | number of files cleaned up each time through the cleanup routine. |
| logviewer:num-files-scanned-per-deep-search | histogram | number of files scanned per deep search |
| logviewer:num-list-dump-files-exceptions | meter | num errors in calls to /dumps/{topo-id}/{host-port} |
| logviewer:num-list-logs-http-request | meter | calls to /listLogs |
| logviewer:num-log-page-http-requests | meter | calls to /log |
| logviewer:num-other-cleanup-exceptions | meter | number of exception in the cleanup loop, not directly deleting files. |
| logviewer:num-page-read | meter | number of pages (parts of a log file) that are served up |
| logviewer:num-read-daemon-log-exceptions | meter | num errors in calls to /daemonlog |
| logviewer:num-read-log-exceptions | meter | num errors in calls to /log |
| logviewer:num-search-exceptions | meter | num errors in calls to /search |
| logviewer:num-search-log-exceptions | meter | num errors in calls to /listLogs |
| logviewer:num-search-logs-requests | meter | calls to /search |
| logviewer:num-search-request-no-result | meter | number of regular search results that were empty |
| logviewer:num-set-permission-exceptions | meter | num errors running set permissions to open up files for reading. |
| logviewer:num-shutdown-calls | meter | number of times shutdown was called on the logviewer |
| logviewer:search-requests-duration-ms | timer | how long it takes for /search |
| logviewer:worker-log-dir-size | gauge | size in bytes of the worker logs directory. |

## Supervisor Metrics

Metrics associated with the supervisor, which launches the workers for a topology.  The supervisor also has a state machine for each slot.  Some of the metrics are associated with that state machine and can be confusing if you do not understand the state machine.

| Metric Name | Type | Description |
|-------------|------|-------------|
| supervisor:blob-cache-update-duration | timer | how long it takes to update all of the blobs in the cache (frequently just check if they have changed, but may also include downloading them.) |
| supervisor:blob-fetching-rate-MB/s | histogram | Download rate of a blob in MB/sec.  Blobs are downloaded rarely so it is very bursty. |
| supervisor:blob-localization-duration | timer | Approximately how long it takes to get the blob we want after it is requested. |
| supervisor:current-reserved-memory-mb | gauge | total amount of memory reserved for workers on the supervisor (MB) |
| supervisor:current-used-memory-mb | gauge | memory currently used as measured by the supervisor (this typically requires cgroups) (MB) |
| supervisor:health-check-timeouts | meter | tracks timeouts executing health check scripts |
| supervisor:local-resource-file-not-found-when-releasing-slot | meter | number of times file-not-found exception happens when reading local blobs upon releasing slots |
| supervisor:num-blob-update-version-changed | meter | number of times a version of a blob changes. |
| supervisor:num-cleanup-exceptions | meter | exceptions thrown during container cleanup. |
| supervisor:num-force-kill-exceptions | meter | exceptions thrown during force kill. |
| supervisor:num-kill-exceptions | meter | exceptions thrown during kill. |
| supervisor:num-kill-worker-errors | meter | errors killing workers. |
| supervisor:num-launched | meter | number of times the supervisor is launched. |
| supervisor:num-shell-exceptions | meter | number of exceptions calling shell commands. |
| supervisor:num-slots-used-gauge | gauge | number of slots used on the supervisor. |
| supervisor:num-worker-start-timed-out | meter | number of times worker start timed out. |
| supervisor:num-worker-transitions-into-empty | meter | number of transitions into empty state. |
| supervisor:num-worker-transitions-into-kill | meter | number of transitions into kill state. |
| supervisor:num-worker-transitions-into-kill-and-relaunch | meter | number of transitions into kill-and-relaunch state |
| supervisor:num-worker-transitions-into-kill-blob-update | meter | number of transitions into kill-blob-update state |
| supervisor:num-worker-transitions-into-running | meter | number of transitions into running state |
| supervisor:num-worker-transitions-into-waiting-for-blob-localization | meter | number of transitions into waiting-for-blob-localization state |
| supervisor:num-worker-transitions-into-waiting-for-blob-update | meter | number of transitions into waiting-for-blob-update state |
| supervisor:num-worker-transitions-into-waiting-for-worker-start | meter | number of transitions into waiting-for-worker-start state |
| supervisor:num-workers-force-kill | meter | number of times a worker was force killed.  This may mean that the worker did not exit cleanly/quickly. |
| supervisor:num-workers-killed-assignment-changed | meter | workers killed because the assignment changed. |
| supervisor:num-workers-killed-blob-changed | meter | workers killed because the blob changed and they needed to be relaunched. |
| supervisor:num-workers-killed-hb-null | meter | workers killed because there was no hb at all from the worker. This would typically only happen when a worker is launched for the first time. |
| supervisor:num-workers-killed-hb-timeout | meter | workers killed because the hb from the worker was too old.  This often happens because of GC issues in the worker that prevents it from sending a heartbeat, but could also mean the worker process exited, and the supervisor is not the parent of the process to know that it exited. |
| supervisor:num-workers-killed-memory-violation | meter | workers killed because the worker was using too much memory.  If the supervisor can monitor memory usage of the worker (typically through cgroups) and the worker goes over the limit it may be shot. |
| supervisor:num-workers-killed-process-exit | meter | workers killed because the process exited and the supervisor was the parent process |
| supervisor:num-workers-launched | meter | number of workers launched |
| supervisor:single-blob-localization-duration | timer | how long it takes for a blob to be updated (downloaded, unzipped, inform slots, and make the move) |
| supervisor:time-worker-spent-in-state-empty-ms | timer | time spent in empty state as it transitions out. Not necessarily in ms. |
| supervisor:time-worker-spent-in-state-kill-and-relaunch-ms | timer | time spent in kill-and-relaunch state as it transitions out. Not necessarily in ms. |
| supervisor:time-worker-spent-in-state-kill-blob-update-ms | timer | time spent in kill-blob-update state as it transitions out. Not necessarily in ms. |
| supervisor:time-worker-spent-in-state-kill-ms | timer | time spent in kill state as it transitions out. Not necessarily in ms. |
| supervisor:time-worker-spent-in-state-running-ms | timer | time spent in running state as it transitions out. Not necessarily in ms. |
| supervisor:time-worker-spent-in-state-waiting-for-blob-localization-ms | timer | time spent in waiting-for-blob-localization state as it transitions out. Not necessarily in ms. |
| supervisor:time-worker-spent-in-state-waiting-for-blob-update-ms | timer | time spent in waiting-for-blob-update state as it transitions out. Not necessarily in ms. |
| supervisor:time-worker-spent-in-state-waiting-for-worker-start-ms | timer | time spent in waiting-for-worker-start state as it transitions out. Not necessarily in ms. |
| supervisor:update-blob-exceptions | meter | number of exceptions updating blobs. |
| supervisor:worker-launch-duration | timer | Time taken for a worker to launch. |
| supervisor:worker-per-call-clean-up-duration-ns | meter | how long it takes to cleanup a worker (ns). |
| supervisor:worker-shutdown-duration-ns | meter | how long it takes to shutdown a worker (ns). |
| supervisor:workerTokenAuthorizer-get-password-failures | meter | Failures getting password for user in WorkerTokenAuthorizer |


## UI Metrics

Metrics associated with a single UI daemon.

| Metric Name | Type | Description |
|-------------|------|-------------|
| ui:num-activate-topology-http-requests | meter | calls to /topology/{id}/activate |
| ui:num-all-topologies-summary-http-requests | meter | calls to /topology/summary |
| ui:num-build-visualization-http-requests | meter | calls to /topology/{id}/visualization |
| ui:num-cluster-configuration-http-requests | meter | calls to /cluster/configuration |
| ui:num-cluster-summary-http-requests | meter | calls to /cluster/summary |
| ui:num-component-op-response-http-requests | meter | calls to /topology/{id}/component/{component}/debug/{action}/{spct} |
| ui:num-component-page-http-requests | meter | calls to /topology/{id}/component/{component} |
| ui:num-deactivate-topology-http-requests | meter | calls to topology/{id}/deactivate |
| ui:num-debug-topology-http-requests | meter | calls to /topology/{id}/debug/{action}/{spct} |
| ui:num-get-owner-resource-summaries-http-request | meter | calls to /owner-resources or /owner-resources/{id} |
| ui:num-log-config-http-requests | meter | calls to /topology/{id}/logconfig |
| ui:num-main-page-http-requests | meter | number of requests to /index.html |
| ui:num-mk-visualization-data-http-requests | meter | calls to /topology/{id}/visualization-init |
| ui:num-nimbus-summary-http-requests | meter | calls to /nimbus/summary |
| ui:num-supervisor-http-requests | meter | calls to /supervisor |
| ui:num-supervisor-summary-http-requests | meter | calls to /supervisor/summary |
| ui:num-topology-lag-http-requests | meter | calls to /topology/{id}/lag |
| ui:num-topology-metric-http-requests | meter | calls to /topology/{id}/metrics |
| ui:num-topology-op-response-http-requests | meter | calls to /topology/{id}/logconfig or /topology/{id}/rebalance/{wait-time} or /topology/{id}/kill/{wait-time} |
| ui:num-topology-page-http-requests | meter | calls to /topology/{id} |
| num-web-requests | meter | nominally the total number of web requests being made. |

## Pacemaker Metrics (Deprecated)

The pacemaker process is deprecated and only still exists for backwards compatibility.

| Metric Name | Type | Description |
|-------------|------|-------------|
| pacemaker:get-pulse=count | meter | number of times getPulse was called.  yes the = is in the name, but typically this is mapped to a '-' by the metrics reporters. |
| pacemaker:heartbeat-size | histogram | size in bytes of heartbeats |
| pacemaker:send-pulse-count | meter | number of times sendPulse was called |
| pacemaker:size-total-keys | gauge | total number of keys in this pacemaker instance |
| pacemaker:total-receive-size | meter | total size in bytes of heartbeats received |
| pacemaker:total-sent-size | meter | total size in bytes of heartbeats read |


## Metric Reporters

For metrics to be reported, configure reporters using `storm.daemon.metrics.reporter.plugins`. The following metric reporters are supported:
  * Console Reporter (`org.apache.storm.daemon.metrics.reporters.ConsolePreparableReporter`):
    Reports metrics to `System.out`.
  * CSV Reporter (`org.apache.storm.daemon.metrics.reporters.CsvPreparableReporter`):
    Reports metrics to a CSV file.
  * JMX Reporter (`org.apache.storm.daemon.metrics.reporters.JmxPreparableReporter`):
    Exposes metrics via JMX.

Custom reporter can be created by implementing `org.apache.storm.daemon.metrics.reporters.PreparableReporter` interface.

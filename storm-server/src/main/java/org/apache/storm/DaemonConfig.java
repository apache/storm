/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm;

import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.nimbus.ITopologyActionNotifierPlugin;
import org.apache.storm.scheduler.resource.strategies.eviction.IEvictionStrategy;
import org.apache.storm.scheduler.resource.strategies.priority.ISchedulingPriorityStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.validation.Validated;

import java.util.ArrayList;
import java.util.Map;

import static org.apache.storm.validation.ConfigValidationAnnotations.*;

/**
 * Storm configs are specified as a plain old map. This class provides constants for
 * all the configurations possible on a Storm cluster. Each constant is paired with an annotation
 * that defines the validity criterion of the corresponding field. Default
 * values for these configs can be found in defaults.yaml.
 *
 * This class extends {@link org.apache.storm.Config} for supporting Storm Daemons.
 */
public class DaemonConfig implements Validated {

    /**
     * We check with this interval that whether the Netty channel is writable and try to write pending messages
     */
    @isInteger
    public static final String STORM_NETTY_FLUSH_CHECK_INTERVAL_MS = "storm.messaging.netty.flush.check.interval.ms";

    /**
     * A list of daemon metrics  reporter plugin class names.
     * These plugins must implement {@link org.apache.storm.daemon.metrics.reporters.PreparableReporter} interface.
     */
    @isStringList
    public static final String STORM_DAEMON_METRICS_REPORTER_PLUGINS = "storm.daemon.metrics.reporter.plugins";

    /**
     * A specify Locale for daemon metrics reporter plugin.
     * Use the specified IETF BCP 47 language tag string for a Locale.
     */
    @isString
    public static final String STORM_DAEMON_METRICS_REPORTER_PLUGIN_LOCALE = "storm.daemon.metrics.reporter.plugin.locale";

    /**
     * A specify domain for daemon metrics reporter plugin to limit reporting to specific domain.
     */
    @isString
    public static final String STORM_DAEMON_METRICS_REPORTER_PLUGIN_DOMAIN = "storm.daemon.metrics.reporter.plugin.domain";

    /**
     * A specify rate-unit in TimeUnit to specify reporting frequency for daemon metrics reporter plugin.
     */
    @isString
    public static final String STORM_DAEMON_METRICS_REPORTER_PLUGIN_RATE_UNIT = "storm.daemon.metrics.reporter.plugin.rate.unit";

    /**
     * A specify duration-unit in TimeUnit to specify reporting window for daemon metrics reporter plugin.
     */
    @isString
    public static final String STORM_DAEMON_METRICS_REPORTER_PLUGIN_DURATION_UNIT = "storm.daemon.metrics.reporter.plugin.duration.unit";


    /**
     * A specify csv reporter directory for CvsPreparableReporter daemon metrics reporter.
     */
    @isString
    public static final String STORM_DAEMON_METRICS_REPORTER_CSV_LOG_DIR = "storm.daemon.metrics.reporter.csv.log.dir";

    /**
     * A directory that holds configuration files for log4j2.
     * It can be either a relative or an absolute directory.
     * If relative, it is relative to the storm's home directory.
     */
    @isString
    public static final String STORM_LOG4J2_CONF_DIR = "storm.log4j2.conf.dir";

    /**
     * A global task scheduler used to assign topologies's tasks to supervisors' workers.
     *
     * If this is not set, a default system scheduler will be used.
     */
    @isString
    public static final String STORM_SCHEDULER = "storm.scheduler";

    /**
     * Whether we want to display all the resource capacity and scheduled usage on the UI page.
     * You MUST have this variable set if you are using any kind of resource-related scheduler.
     *
     * If this is not set, we will not display resource capacity and usage on the UI.
     */
    @isBoolean
    public static final String SCHEDULER_DISPLAY_RESOURCE = "scheduler.display.resource";

    /**
     * Initialization parameters for the group mapping service plugin.
     * Provides a way for a @link{STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN}
     * implementation to access optional settings.
     */
    @isType(type=Map.class)
    public static final String STORM_GROUP_MAPPING_SERVICE_PARAMS = "storm.group.mapping.service.params";

    /**
     * The directory where storm's health scripts go.
     */
    @isString
    public static final String STORM_HEALTH_CHECK_DIR = "storm.health.check.dir";

    /**
     * The time to allow any given healthcheck script to run before it
     * is marked failed due to timeout
     */
    @isNumber
    public static final String STORM_HEALTH_CHECK_TIMEOUT_MS = "storm.health.check.timeout.ms";

    /**
     * This is the user that the Nimbus daemon process is running as. May be used when security
     * is enabled to authorize actions in the cluster.
     */
    @isString
    public static final String NIMBUS_DAEMON_USER = "nimbus.daemon.user";

    /**
     * This parameter is used by the storm-deploy project to configure the
     * jvm options for the nimbus daemon.
     */
    @isStringOrStringList
    public static final String NIMBUS_CHILDOPTS = "nimbus.childopts";


    /**
     * How long without heartbeating a task can go before nimbus will consider the
     * task dead and reassign it to another location.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_TASK_TIMEOUT_SECS = "nimbus.task.timeout.secs";


    /**
     * How often nimbus should wake up to check heartbeats and do reassignments. Note
     * that if a machine ever goes down Nimbus will immediately wake up and take action.
     * This parameter is for checking for failures when there's no explicit event like that
     * occurring.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_MONITOR_FREQ_SECS = "nimbus.monitor.freq.secs";

    /**
     * How often nimbus should wake the cleanup thread to clean the inbox.
     * @see #NIMBUS_INBOX_JAR_EXPIRATION_SECS
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_CLEANUP_INBOX_FREQ_SECS = "nimbus.cleanup.inbox.freq.secs";

    /**
     * The length of time a jar file lives in the inbox before being deleted by the cleanup thread.
     *
     * Probably keep this value greater than or equal to NIMBUS_CLEANUP_INBOX_JAR_EXPIRATION_SECS.
     * Note that the time it takes to delete an inbox jar file is going to be somewhat more than
     * NIMBUS_CLEANUP_INBOX_JAR_EXPIRATION_SECS (depending on how often NIMBUS_CLEANUP_FREQ_SECS
     * is set to).
     * @see #NIMBUS_CLEANUP_INBOX_FREQ_SECS
     */
    @isInteger
    public static final String NIMBUS_INBOX_JAR_EXPIRATION_SECS = "nimbus.inbox.jar.expiration.secs";

    /**
     * How long before a supervisor can go without heartbeating before nimbus considers it dead
     * and stops assigning new work to it.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_SUPERVISOR_TIMEOUT_SECS = "nimbus.supervisor.timeout.secs";

    /**
     * A special timeout used when a task is initially launched. During launch, this is the timeout
     * used until the first heartbeat, overriding nimbus.task.timeout.secs.
     *
     * <p>A separate timeout exists for launch because there can be quite a bit of overhead
     * to launching new JVM's and configuring them.</p>
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_TASK_LAUNCH_SECS = "nimbus.task.launch.secs";

    /**
     * During upload/download with the master, how long an upload or download connection is idle
     * before nimbus considers it dead and drops the connection.
     */
    @isInteger
    public static final String NIMBUS_FILE_COPY_EXPIRATION_SECS = "nimbus.file.copy.expiration.secs";

    /**
     * A custom class that implements ITopologyValidator that is run whenever a
     * topology is submitted. Can be used to provide business-specific logic for
     * whether topologies are allowed to run or not.
     */
    @isString
    public static final String NIMBUS_TOPOLOGY_VALIDATOR = "nimbus.topology.validator";

    /**
     * Class name for authorization plugin for Nimbus.
     */
    @isString
    public static final String NIMBUS_AUTHORIZER = "nimbus.authorizer";

    /**
     * Impersonation user ACL config entries.
     */
    @isString
    public static final String NIMBUS_IMPERSONATION_AUTHORIZER = "nimbus.impersonation.authorizer";

    /**
     * How often nimbus should wake up to renew credentials if needed.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_CREDENTIAL_RENEW_FREQ_SECS = "nimbus.credential.renewers.freq.secs";

    /**
     * FQCN of a class that implements {@code I} @see org.apache.storm.nimbus.ITopologyActionNotifierPlugin for details.
     */
    @isImplementationOfClass(implementsClass = ITopologyActionNotifierPlugin.class)
    public static final String NIMBUS_TOPOLOGY_ACTION_NOTIFIER_PLUGIN = "nimbus.topology.action.notifier.plugin.class";

    /**
     * Storm UI binds to this host/interface.
     */
    @isString
    public static final String UI_HOST = "ui.host";

    /**
     * Storm UI binds to this port.
     */
    @isInteger
    @isPositiveNumber
    public static final String UI_PORT = "ui.port";

    /**
     * Storm UI Project BUGTRACKER Link for reporting issue.
     */
    @isString
    public static final String UI_PROJECT_BUGTRACKER_URL = "ui.project.bugtracker.url";

    /**
     * Storm UI Central Logging URL.
     */
    @isString
    public static final String UI_CENTRAL_LOGGING_URL = "ui.central.logging.url";

    /**
     * HTTP UI port for log viewer.
     */
    @isInteger
    @isPositiveNumber
    public static final String LOGVIEWER_PORT = "logviewer.port";

    /**
     * Childopts for log viewer java process.
     */
    @isStringOrStringList
    public static final String LOGVIEWER_CHILDOPTS = "logviewer.childopts";

    /**
     * How often to clean up old log files.
     */
    @isInteger
    @isPositiveNumber
    public static final String LOGVIEWER_CLEANUP_INTERVAL_SECS = "logviewer.cleanup.interval.secs";

    /**
     * How many minutes since a log was last modified for the log to be considered for clean-up.
     */
    @isInteger
    @isPositiveNumber
    public static final String LOGVIEWER_CLEANUP_AGE_MINS = "logviewer.cleanup.age.mins";

    /**
     * The maximum number of bytes all worker log files can take up in MB.
     */
    @isPositiveNumber
    public static final String LOGVIEWER_MAX_SUM_WORKER_LOGS_SIZE_MB = "logviewer.max.sum.worker.logs.size.mb";

    /**
     * The maximum number of bytes per worker's files can take up in MB.
     */
    @isPositiveNumber
    public static final String LOGVIEWER_MAX_PER_WORKER_LOGS_SIZE_MB = "logviewer.max.per.worker.logs.size.mb";

    /**
     * Storm Logviewer HTTPS port.
     */
    @isInteger
    @isPositiveNumber
    public static final String LOGVIEWER_HTTPS_PORT = "logviewer.https.port";

    /**
     * Path to the keystore containing the certs used by Storm Logviewer for HTTPS communications.
     */
    @isString
    public static final String LOGVIEWER_HTTPS_KEYSTORE_PATH = "logviewer.https.keystore.path";

    /**
     * Password for the keystore for HTTPS for Storm Logviewer.
     */
    @isString
    public static final String LOGVIEWER_HTTPS_KEYSTORE_PASSWORD = "logviewer.https.keystore.password";

    /**
     * Type of the keystore for HTTPS for Storm Logviewer.
     * see http://docs.oracle.com/javase/8/docs/api/java/security/KeyStore.html for more details.
     */
    @isString
    public static final String LOGVIEWER_HTTPS_KEYSTORE_TYPE = "logviewer.https.keystore.type";

    /**
     * Password to the private key in the keystore for setting up HTTPS (SSL).
     */
    @isString
    public static final String LOGVIEWER_HTTPS_KEY_PASSWORD = "logviewer.https.key.password";

    /**
     * Path to the truststore containing the certs used by Storm Logviewer for HTTPS communications.
     */
    @isString
    public static final String LOGVIEWER_HTTPS_TRUSTSTORE_PATH = "logviewer.https.truststore.path";

    /**
     * Password for the truststore for HTTPS for Storm Logviewer.
     */
    @isString
    public static final String LOGVIEWER_HTTPS_TRUSTSTORE_PASSWORD = "logviewer.https.truststore.password";

    /**
     * Type of the truststore for HTTPS for Storm Logviewer.
     * see http://docs.oracle.com/javase/8/docs/api/java/security/Truststore.html for more details.
     */
    @isString
    public static final String LOGVIEWER_HTTPS_TRUSTSTORE_TYPE = "logviewer.https.truststore.type";

    /**
     * Password to the truststore used by Storm Logviewer setting up HTTPS (SSL).
     */
    @isBoolean
    public static final String LOGVIEWER_HTTPS_WANT_CLIENT_AUTH = "logviewer.https.want.client.auth";

    @isBoolean
    public static final String LOGVIEWER_HTTPS_NEED_CLIENT_AUTH = "logviewer.https.need.client.auth";

    /**
     * A list of users allowed to view logs via the Log Viewer.
     */
    @isStringList
    public static final String LOGS_USERS = "logs.users";

    /**
     * A list of groups allowed to view logs via the Log Viewer.
     */
    @isStringList
    public static final String LOGS_GROUPS = "logs.groups";

    /**
     * Appender name used by log viewer to determine log directory.
     */
    @isString
    public static final String LOGVIEWER_APPENDER_NAME = "logviewer.appender.name";

    /**
     * Childopts for Storm UI Java process.
     */
    @isStringOrStringList
    public static final String UI_CHILDOPTS = "ui.childopts";

    /**
     * A class implementing javax.servlet.Filter for authenticating/filtering UI requests.
     */
    @isString
    public static final String UI_FILTER = "ui.filter";

    /**
     * Initialization parameters for the javax.servlet.Filter.
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String UI_FILTER_PARAMS = "ui.filter.params";

    /**
     * The size of the header buffer for the UI in bytes.
     */
    @isInteger
    @isPositiveNumber
    public static final String UI_HEADER_BUFFER_BYTES = "ui.header.buffer.bytes";

    /**
     * This port is used by Storm DRPC for receiving HTTPS (SSL) DPRC requests from clients.
     */
    @isInteger
    @isPositiveNumber
    public static final String UI_HTTPS_PORT = "ui.https.port";

    /**
     * Path to the keystore used by Storm UI for setting up HTTPS (SSL).
     */
    @isString
    public static final String UI_HTTPS_KEYSTORE_PATH = "ui.https.keystore.path";

    /**
     * Password to the keystore used by Storm UI for setting up HTTPS (SSL).
     */
    @isString
    public static final String UI_HTTPS_KEYSTORE_PASSWORD = "ui.https.keystore.password";

    /**
     * Type of keystore used by Storm UI for setting up HTTPS (SSL).
     * see http://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html for more details.
     */
    @isString
    public static final String UI_HTTPS_KEYSTORE_TYPE = "ui.https.keystore.type";

    /**
     * Password to the private key in the keystore for setting up HTTPS (SSL).
     */
    @isString
    public static final String UI_HTTPS_KEY_PASSWORD = "ui.https.key.password";

    /**
     * Path to the truststore used by Storm UI setting up HTTPS (SSL).
     */
    @isString
    public static final String UI_HTTPS_TRUSTSTORE_PATH = "ui.https.truststore.path";

    /**
     * Password to the truststore used by Storm UI setting up HTTPS (SSL).
     */
    @isString
    public static final String UI_HTTPS_TRUSTSTORE_PASSWORD = "ui.https.truststore.password";

    /**
     * Type of truststore used by Storm UI for setting up HTTPS (SSL).
     * see http://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html for more details.
     */
    @isString
    public static final String UI_HTTPS_TRUSTSTORE_TYPE = "ui.https.truststore.type";

    /**
     * Password to the truststore used by Storm DRPC setting up HTTPS (SSL).
     */
    @isBoolean
    public static final String UI_HTTPS_WANT_CLIENT_AUTH = "ui.https.want.client.auth";

    @isBoolean
    public static final String UI_HTTPS_NEED_CLIENT_AUTH = "ui.https.need.client.auth";

    /**
     * The maximum number of threads that should be used by the Pacemaker.
     * When Pacemaker gets loaded it will spawn new threads, up to
     * this many total, to handle the load.
     */
    @isNumber
    @isPositiveNumber
    public static final String PACEMAKER_MAX_THREADS = "pacemaker.max.threads";

    /**
     * This parameter is used by the storm-deploy project to configure the
     * jvm options for the pacemaker daemon.
     */
    @isStringOrStringList
    public static final String PACEMAKER_CHILDOPTS = "pacemaker.childopts";


    /**
     * This port is used by Storm DRPC for receiving HTTP DPRC requests from clients.
     */
    @isInteger
    public static final String DRPC_HTTP_PORT = "drpc.http.port";

    /**
     * This port is used by Storm DRPC for receiving HTTPS (SSL) DPRC requests from clients.
     */
    @isInteger
    public static final String DRPC_HTTPS_PORT = "drpc.https.port";

    /**
     * Path to the keystore used by Storm DRPC for setting up HTTPS (SSL).
     */
    @isString
    public static final String DRPC_HTTPS_KEYSTORE_PATH = "drpc.https.keystore.path";

    /**
     * Password to the keystore used by Storm DRPC for setting up HTTPS (SSL).
     */
    @isString
    public static final String DRPC_HTTPS_KEYSTORE_PASSWORD = "drpc.https.keystore.password";

    /**
     * Type of keystore used by Storm DRPC for setting up HTTPS (SSL).
     * see http://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html for more details.
     */
    @isString
    public static final String DRPC_HTTPS_KEYSTORE_TYPE = "drpc.https.keystore.type";

    /**
     * Password to the private key in the keystore for setting up HTTPS (SSL).
     */
    @isString
    public static final String DRPC_HTTPS_KEY_PASSWORD = "drpc.https.key.password";

    /**
     * Path to the truststore used by Storm DRPC setting up HTTPS (SSL).
     */
    @isString
    public static final String DRPC_HTTPS_TRUSTSTORE_PATH = "drpc.https.truststore.path";

    /**
     * Password to the truststore used by Storm DRPC setting up HTTPS (SSL).
     */
    @isString
    public static final String DRPC_HTTPS_TRUSTSTORE_PASSWORD = "drpc.https.truststore.password";

    /**
     * Type of truststore used by Storm DRPC for setting up HTTPS (SSL).
     * see http://docs.oracle.com/javase/7/docs/api/java/security/KeyStore.html for more details.
     */
    @isString
    public static final String DRPC_HTTPS_TRUSTSTORE_TYPE = "drpc.https.truststore.type";

    /**
     * Password to the truststore used by Storm DRPC setting up HTTPS (SSL).
     */
    @isBoolean
    public static final String DRPC_HTTPS_WANT_CLIENT_AUTH = "drpc.https.want.client.auth";

    @isBoolean
    public static final String DRPC_HTTPS_NEED_CLIENT_AUTH = "drpc.https.need.client.auth";

    /**
     * Class name for authorization plugin for DRPC client.
     */
    @isString
    public static final String DRPC_AUTHORIZER = "drpc.authorizer";

    /**
     * The timeout on DRPC requests within the DRPC server. Defaults to 10 minutes. Note that requests can also
     * timeout based on the socket timeout on the DRPC client, and separately based on the topology message
     * timeout for the topology implementing the DRPC function.
     */

    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String DRPC_REQUEST_TIMEOUT_SECS  = "drpc.request.timeout.secs";

    /**
     * Childopts for Storm DRPC Java process.
     */
    @isStringOrStringList
    public static final String DRPC_CHILDOPTS = "drpc.childopts";

    /**
     * the metadata configured on the supervisor.
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String SUPERVISOR_SCHEDULER_META = "supervisor.scheduler.meta";

    /**
     * A list of ports that can run workers on this supervisor. Each worker uses one port, and
     * the supervisor will only run one worker per port. Use this configuration to tune
     * how many workers run on each machine.
     */
    @isNoDuplicateInList
    @NotNull
    @isListEntryCustom(entryValidatorClasses = {ConfigValidation.IntegerValidator.class,ConfigValidation.PositiveNumberValidator.class})
    public static final String SUPERVISOR_SLOTS_PORTS = "supervisor.slots.ports";

    /**
     * What blobstore implementation the supervisor should use.
     */
    @isString
    public static final String SUPERVISOR_BLOBSTORE = "supervisor.blobstore.class";

    /**
     * The distributed cache target size in MB. This is a soft limit to the size of the distributed
     * cache contents.
     */
    @isPositiveNumber
    @isInteger
    public static final String SUPERVISOR_LOCALIZER_CACHE_TARGET_SIZE_MB = "supervisor.localizer.cache.target.size.mb";

    /**
     * The distributed cache cleanup interval. Controls how often it scans to attempt to cleanup
     * anything over the cache target size.
     */
    @isPositiveNumber
    @isInteger
    public static final String SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS = "supervisor.localizer.cleanup.interval.ms";

    /**
     * What blobstore download parallelism the supervisor should use.
     */
    @isPositiveNumber
    @isInteger
    public static final String SUPERVISOR_BLOBSTORE_DOWNLOAD_THREAD_COUNT = "supervisor.blobstore.download.thread.count";

    /**
     * Maximum number of retries a supervisor is allowed to make for downloading a blob.
     */
    @isPositiveNumber
    @isInteger
    public static final String SUPERVISOR_BLOBSTORE_DOWNLOAD_MAX_RETRIES = "supervisor.blobstore.download.max_retries";

    /**
     * What blobstore implementation nimbus should use.
     */
    @isString
    public static final String NIMBUS_BLOBSTORE = "nimbus.blobstore.class";

    /**
     * During operations with the blob store, via master, how long a connection
     * is idle before nimbus considers it dead and drops the session and any
     * associated connections.
     */
    @isPositiveNumber
    @isInteger
    public static final String NIMBUS_BLOBSTORE_EXPIRATION_SECS = "nimbus.blobstore.expiration.secs";

    /**
     * A number representing the maximum number of workers any single topology can acquire.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String NIMBUS_SLOTS_PER_TOPOLOGY = "nimbus.slots.perTopology";

    /**
     * A class implementing javax.servlet.Filter for DRPC HTTP requests.
     */
    @isString
    public static final String DRPC_HTTP_FILTER = "drpc.http.filter";

    /**
     * Initialization parameters for the javax.servlet.Filter of the DRPC HTTP
     * service.
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String DRPC_HTTP_FILTER_PARAMS = "drpc.http.filter.params";

    /**
     * A number representing the maximum number of executors any single topology can acquire.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String NIMBUS_EXECUTORS_PER_TOPOLOGY = "nimbus.executors.perTopology";

    /**
     * This parameter is used by the storm-deploy project to configure the
     * jvm options for the supervisor daemon.
     */
    @isStringOrStringList
    public static final String SUPERVISOR_CHILDOPTS = "supervisor.childopts";

    /**
     * How many seconds to sleep for before shutting down threads on worker.
     */
    @isInteger
    @isPositiveNumber
    public static final String SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS = "supervisor.worker.shutdown.sleep.secs";

    /**
     * How long a worker can go without heartbeating during the initial launch before
     * the supervisor tries to restart the worker process. This value override
     * supervisor.worker.timeout.secs during launch because there is additional
     * overhead to starting and configuring the JVM on launch.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String SUPERVISOR_WORKER_START_TIMEOUT_SECS = "supervisor.worker.start.timeout.secs";

    /**
     * Whether or not the supervisor should launch workers assigned to it. Defaults
     * to true -- and you should probably never change this value. This configuration
     * is used in the Storm unit tests.
     */
    @isBoolean
    public static final String SUPERVISOR_ENABLE = "supervisor.enable";

    /**
     * how often the supervisor sends a heartbeat to the master.
     */
    @isInteger
    public static final String SUPERVISOR_HEARTBEAT_FREQUENCY_SECS = "supervisor.heartbeat.frequency.secs";


    /**
     * How often the supervisor checks the worker heartbeats to see if any of them
     * need to be restarted.
     */
    @isInteger
    @isPositiveNumber
    public static final String SUPERVISOR_MONITOR_FREQUENCY_SECS = "supervisor.monitor.frequency.secs";




    /**
     * The jvm profiler opts provided to workers launched by this supervisor.
     */
    @isStringOrStringList
    public static final String WORKER_PROFILER_CHILDOPTS = "worker.profiler.childopts";

    /**
     * Enable profiling of worker JVMs using Oracle's Java Flight Recorder.
     * Unlocking commercial features requires a special license from Oracle.
     * See http://www.oracle.com/technetwork/java/javase/terms/products/index.html
     */
    @isBoolean
    public static final String WORKER_PROFILER_ENABLED = "worker.profiler.enabled";

    /**
     * The command launched supervisor with worker arguments
     * pid, action and [target_directory]
     * Where action is - start profile, stop profile, jstack, heapdump and kill against pid.
     *
     */
    @isString
    public static final String WORKER_PROFILER_COMMAND = "worker.profiler.command";

    /**
     * A list of classes implementing IClusterMetricsConsumer (See storm.yaml.example for exact config format).
     * Each listed class will be routed cluster related metrics data.
     * Each listed class maps 1:1 to a ClusterMetricsConsumerExecutor and they're executed in Nimbus.
     * Only consumers which run in leader Nimbus receives metrics data.
     */
    @isListEntryCustom(entryValidatorClasses = {ConfigValidation.ClusterMetricRegistryValidator.class})
    public static final String STORM_CLUSTER_METRICS_CONSUMER_REGISTER = "storm.cluster.metrics.consumer.register";

    /**
     * How often cluster metrics data is published to metrics consumer.
     */
    @NotNull
    @isPositiveNumber
    public static final String STORM_CLUSTER_METRICS_CONSUMER_PUBLISH_INTERVAL_SECS = "storm.cluster.metrics.consumer.publish.interval.secs";

    /**
     * Enables user-first classpath. See topology.classpath.beginning.
     */
    @isBoolean
    public static final String STORM_TOPOLOGY_CLASSPATH_BEGINNING_ENABLED="storm.topology.classpath.beginning.enabled";

    /**
     * This value is passed to spawned JVMs (e.g., Nimbus, Supervisor, and Workers)
     * for the java.library.path value. java.library.path tells the JVM where
     * to look for native libraries. It is necessary to set this config correctly since
     * Storm uses the ZeroMQ and JZMQ native libs.
     */
    @isString
    public static final String JAVA_LIBRARY_PATH = "java.library.path";

    /**
     * The path to use as the zookeeper dir when running a zookeeper server via
     * "storm dev-zookeeper". This zookeeper instance is only intended for development;
     * it is not a production grade zookeeper setup.
     */
    @isString
    public static final String DEV_ZOOKEEPER_PATH = "dev.zookeeper.path";

    /**
     * A map from topology name to the number of machines that should be dedicated for that topology. Set storm.scheduler
     * to org.apache.storm.scheduler.IsolationScheduler to make use of the isolation scheduler.
     */
    @isMapEntryType(keyType = String.class, valueType = Number.class)
    public static final String ISOLATION_SCHEDULER_MACHINES = "isolation.scheduler.machines";

    /**
     * For ArtifactoryConfigLoader, this can either be a reference to an individual file in Artifactory or to a directory.
     * If it is a directory, the file with the largest lexographic name will be returned. Users need to add "artifactory+" to the beginning of
     * the real URI to use ArtifactoryConfigLoader.
     * For FileConfigLoader, this is the URI pointing to a file.
     */
    @isString
    public static final String SCHEDULER_CONFIG_LOADER_URI = "scheduler.config.loader.uri";

    /**
     * It is the frequency at which the plugin will call out to artifactory instead of returning the most recently cached result.
     * Currently it's only used in ArtifactoryConfigLoader.
     */
    @isInteger
    @isPositiveNumber
    public static final String SCHEDULER_CONFIG_LOADER_POLLTIME_SECS = "scheduler.config.loader.polltime.secs";

    /**
     * It is the amount of time an http connection to the artifactory server will wait before timing out.
     * Currently it's only used in ArtifactoryConfigLoader.
     */
    @isInteger
    @isPositiveNumber
    public static final String SCHEDULER_CONFIG_LOADER_TIMEOUT_SECS = "scheduler.config.loader.timeout.secs";

    /**
     * It is the part of the uri, configurable in Artifactory, which represents the top of the directory tree.
     * It's only used in ArtifactoryConfigLoader.
     */
    @isString
    public static final String  SCHEDULER_CONFIG_LOADER_ARTIFACTORY_BASE_DIRECTORY = "scheduler.config.loader.artifactory.base.directory";

    /**
     * A map from the user name to the number of machines that should that user is allowed to use. Set storm.scheduler
     * to org.apache.storm.scheduler.multitenant.MultitenantScheduler
     */
    @isMapEntryType(keyType = String.class, valueType = Number.class)
    public static final String MULTITENANT_SCHEDULER_USER_POOLS = "multitenant.scheduler.user.pools";

    /**
     * A map of users to another map of the resource guarantees of the user. Used by Resource Aware Scheduler to ensure
     * per user resource guarantees.
     */
    @isMapEntryCustom(keyValidatorClasses = {ConfigValidation.StringValidator.class}, valueValidatorClasses = {ConfigValidation.UserResourcePoolEntryValidator.class})
    public static final String RESOURCE_AWARE_SCHEDULER_USER_POOLS = "resource.aware.scheduler.user.pools";

    /**
     * The class that specifies the eviction strategy to use in ResourceAwareScheduler.
     */
    @NotNull
    @isImplementationOfClass(implementsClass = IEvictionStrategy.class)
    public static final String RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY = "resource.aware.scheduler.eviction.strategy";

    /**
     * the class that specifies the scheduling priority strategy to use in ResourceAwareScheduler.
     */
    @NotNull
    @isImplementationOfClass(implementsClass = ISchedulingPriorityStrategy.class)
    public static final String RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY = "resource.aware.scheduler.priority.strategy";

    /**
     * How often nimbus's background thread to sync code for missing topologies should run.
     */
    @isInteger
    public static final String NIMBUS_CODE_SYNC_FREQ_SECS = "nimbus.code.sync.freq.secs";

    /**
     * The plugin to be used for resource isolation
     */
    @isImplementationOfClass(implementsClass = ResourceIsolationInterface.class)
    public static final String STORM_RESOURCE_ISOLATION_PLUGIN = "storm.resource.isolation.plugin";

    /**
     * CGroup Setting below
     */

    /**
     * resources to to be controlled by cgroups
     */
    @isStringList
    public static final String STORM_CGROUP_RESOURCES = "storm.cgroup.resources";

    /**
     * name for the cgroup hierarchy.
     */
    @isString
    public static final String STORM_CGROUP_HIERARCHY_NAME = "storm.cgroup.hierarchy.name";

    /**
     * flag to determine whether to use a resource isolation plugin
     * Also determines whether the unit tests for cgroup runs.
     * If storm.resource.isolation.plugin.enable is set to false the unit tests for cgroups will not run
     */
    @isBoolean
    public static final String STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE = "storm.resource.isolation.plugin.enable";

    /**
     * root directory for cgoups.
     */
    @isString
    public static String STORM_SUPERVISOR_CGROUP_ROOTDIR = "storm.supervisor.cgroup.rootdir";

    /**
     * the manually set memory limit (in MB) for each CGroup on supervisor node.
     */
    @isPositiveNumber
    public static String STORM_WORKER_CGROUP_MEMORY_MB_LIMIT = "storm.worker.cgroup.memory.mb.limit";

    /**
     * the manually set cpu share for each CGroup on supervisor node.
     */
    @isPositiveNumber
    public static String STORM_WORKER_CGROUP_CPU_LIMIT = "storm.worker.cgroup.cpu.limit";

    /**
     * full path to cgexec command.
     */
    @isString
    public static String STORM_CGROUP_CGEXEC_CMD = "storm.cgroup.cgexec.cmd";

    /**
     * Please use STORM_SUPERVISOR_MEMORY_LIMIT_TOLERANCE_MARGIN_MB instead. The amount of memory a
     * worker can exceed its allocation before cgroup will kill it.
     */
    @isPositiveNumber(includeZero = true)
    public static String STORM_CGROUP_MEMORY_LIMIT_TOLERANCE_MARGIN_MB =
        "storm.cgroup.memory.limit.tolerance.margin.mb";

    /**
     * Java does not always play nicely with cgroups. It is coming but not fully implemented and not
     * for the way storm uses cgroups. In the short term you can disable the hard memory enforcement
     * by cgroups and let the supervisor handle shooting workers going over their limit in a kinder
     * way.
     */
    @isBoolean
    public static String STORM_CGROUP_MEMORY_ENFORCEMENT_ENABLE = "storm.cgroup.memory.enforcement.enable";

    // Configs for memory enforcement done by the supervisor (not cgroups directly)

    /**
     * Memory given to each worker for free (because java and storm have some overhead). This is
     * memory on the box that the workers can use. This should not be included in
     * SUPERVISOR_MEMORY_CAPACITY_MB, as nimbus does not use this memory for scheduling.
     */
    @isPositiveNumber
    public static String STORM_SUPERVISOR_MEMORY_LIMIT_TOLERANCE_MARGIN_MB =
        "storm.supervisor.memory.limit.tolerance.margin.mb";

    /**
     * A multiplier for the memory limit of a worker that will have the supervisor shoot it
     * immediately. 1.0 means shoot the worker as soon as it goes over. 2.0 means shoot the worker if
     * its usage is double what was requested. This value is combined with
     * STORM_SUPERVISOR_HARD_MEMORY_LIMIT_OVERAGE and which ever is greater is used for enforcement.
     * This allows small workers to not be shot.
     */
    @isPositiveNumber
    public static String STORM_SUPERVISOR_HARD_MEMORY_LIMIT_MULTIPLIER =
        "storm.supervisor.hard.memory.limit.multiplier";

    /**
     * If the memory usage of a worker goes over its limit by this value is it shot immediately. This
     * value is combined with STORM_SUPERVISOR_HARD_LIMIT_MEMORY_MULTIPLIER and which ever is greater
     * is used for enforcement. This allows small workers to not be shot.
     */
    @isPositiveNumber(includeZero = true)
    public static String STORM_SUPERVISOR_HARD_LIMIT_MEMORY_OVERAGE_MB = "storm.supervisor.hard.memory.limit.overage.mb";

    /**
     * If the amount of memory that is free in the system (either on the box or in the supervisor's
     * cgroup) is below this number (in MB) consider the system to be in low memory mode and start
     * shooting workers if they are over their limit.
     */
    @isPositiveNumber
    public static String STORM_SUPERVISOR_LOW_MEMORY_THRESHOLD_MB = "storm.supervisor.low.memory.threshold.mb";

    /**
     * If the amount of memory that is free in the system (either on the box or in the supervisor's
     * cgroup) is below this number (in MB) consider the system to be a little low on memory and start
     * shooting workers if they are over their limit for a given grace period
     * STORM_SUPERVISOR_MEDIUM_MEMORY_GRACE_PERIOD_MS.
     */
    @isPositiveNumber
    public static String STORM_SUPERVISOR_MEDIUM_MEMORY_THRESHOLD_MB = "storm.supervisor.medium.memory.threshold.mb";

    /**
     * The number of milliseconds that a worker is allowed to be over their limit when there is a
     * medium amount of memory free in the system.
     */
    @isPositiveNumber
    public static String STORM_SUPERVISOR_MEDIUM_MEMORY_GRACE_PERIOD_MS =
        "storm.supervisor.medium.memory.grace.period.ms";

    // VALIDATION ONLY CONFIGS
    // Some configs inside Config.java may reference classes we don't want to expose in storm-client, but we still want to validate
    // That they reference a valid class.  To allow this to happen we do part of the validation on the client side with annotations on
    // static final members of the Config class, and other validations here.  We avoid naming them the same thing because clojure code
    // walks these two classes and creates clojure constants for these values.

    /**
     * Server side validation that @{see Config#TOPOLOGY_SCHEDULER_STRATEGY} is set ot a subclass of IStrategy.
     */
    @isImplementationOfClass(implementsClass = IStrategy.class)
    public static final String VALIDATE_TOPOLOGY_SCHEDULER_STRATEGY = Config.TOPOLOGY_SCHEDULER_STRATEGY;

    public static String getCgroupRootDir(Map<String, Object> conf) {
        return (String) conf.get(STORM_SUPERVISOR_CGROUP_ROOTDIR);
    }

    public static String getCgroupStormHierarchyDir(Map<String, Object> conf) {
        return (String) conf.get(Config.STORM_CGROUP_HIERARCHY_DIR);
    }

    /**
     * Get the cgroup resources from the conf
     *
     * @param conf the config to read
     * @return the resources.
     */
    public static ArrayList<String> getCgroupStormResources(Map<String, Object> conf) {
        ArrayList<String> ret = new ArrayList<>();
        for (String entry : ((Iterable<String>) conf.get(DaemonConfig.STORM_CGROUP_RESOURCES))) {
            ret.add(entry);
        }
        return ret;
    }

    public static String getCgroupStormHierarchyName(Map<String, Object> conf) {
        return (String) conf.get(DaemonConfig.STORM_CGROUP_HIERARCHY_NAME);
    }
}

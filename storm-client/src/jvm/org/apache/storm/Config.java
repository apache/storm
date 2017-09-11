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

import org.apache.storm.serialization.IKryoDecorator;
import org.apache.storm.serialization.IKryoFactory;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.validation.ConfigValidationAnnotations.*;
import org.apache.storm.validation.ConfigValidation.*;
import com.esotericsoftware.kryo.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Topology configs are specified as a plain old map. This class provides a
 * convenient way to create a topology config map by providing setter methods for
 * all the configs that can be set. It also makes it easier to do things like add
 * serializations.
 *
 * This class also provides constants for all the configurations possible on
 * a Storm cluster and Storm topology. Each constant is paired with an annotation
 * that defines the validity criterion of the corresponding field. Default
 * values for these configs can be found in defaults.yaml.
 *
 * Note that you may put other configurations in any of the configs. Storm
 * will ignore anything it doesn't recognize, but your topologies are free to make
 * use of them by reading them in the prepare method of Bolts or the open method of
 * Spouts.
 */
public class Config extends HashMap<String, Object> {

    //DO NOT CHANGE UNLESS WE ADD IN STATE NOT STORED IN THE PARENT CLASS
    private static final long serialVersionUID = -1550278723792864455L;

    /**
     * The serializer class for ListDelegate (tuple payload).
     * The default serializer will be ListDelegateSerializer
     */
    @isString
    public static final String TOPOLOGY_TUPLE_SERIALIZER = "topology.tuple.serializer";

    /**
     * Disable load aware grouping support.
     */
    @isBoolean
    @NotNull
    public static final String TOPOLOGY_DISABLE_LOADAWARE_MESSAGING = "topology.disable.loadaware.messaging";

    /**
     * Try to serialize all tuples, even for local transfers.  This should only be used
     * for testing, as a sanity check that all of your tuples are setup properly.
     */
    @isBoolean
    public static final String TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE = "topology.testing.always.try.serialize";

    /**
     * A map with blobstore keys mapped to each filename the worker will have access to in the
     * launch directory to the blob by local file name and uncompress flag. Both localname and
     * uncompress flag are optional. It uses the key is localname is not specified. Each topology
     * will have different map of blobs.  Example: topology.blobstore.map: {"blobstorekey" :
     * {"localname": "myblob", "uncompress": false}, "blobstorearchivekey" :
     * {"localname": "myarchive", "uncompress": true}}
     */
    @CustomValidator(validatorClass = MapOfStringToMapOfStringToObjectValidator.class)
    public static final String TOPOLOGY_BLOBSTORE_MAP = "topology.blobstore.map";

    /**
     * How often a worker should check dynamic log level timeouts for expiration.
     * For expired logger settings, the clean up polling task will reset the log levels
     * to the original levels (detected at startup), and will clean up the timeout map
     */
    @isInteger
    @isPositiveNumber
    public static final String WORKER_LOG_LEVEL_RESET_POLL_SECS = "worker.log.level.reset.poll.secs";

    /**
     * How often a task should sync credentials, worst case.
     */
    @isInteger
    @isPositiveNumber
    public static final String TASK_CREDENTIALS_POLL_SECS = "task.credentials.poll.secs";

    /**
     * How often to poll for changed topology backpressure flag from ZK
     */
    @isInteger
    @isPositiveNumber
    public static final String TASK_BACKPRESSURE_POLL_SECS = "task.backpressure.poll.secs";

    /**
     * Whether to enable backpressure in for a certain topology
     */
    @isBoolean
    public static final String TOPOLOGY_BACKPRESSURE_ENABLE = "topology.backpressure.enable";

    /**
     * This signifies the tuple congestion in a disruptor queue.
     * When the used ratio of a disruptor queue is higher than the high watermark,
     * the backpressure scheme, if enabled, should slow down the tuple sending speed of
     * the spouts until reaching the low watermark.
     */
    @isPositiveNumber
    public static final String BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK="backpressure.disruptor.high.watermark";

    /**
     * This signifies a state that a disruptor queue has left the congestion.
     * If the used ratio of a disruptor queue is lower than the low watermark,
     * it will unset the backpressure flag.
     */
    @isPositiveNumber
    public static final String BACKPRESSURE_DISRUPTOR_LOW_WATERMARK="backpressure.disruptor.low.watermark";

    /**
     * A list of users that are allowed to interact with the topology.  To use this set
     * nimbus.authorizer to org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String TOPOLOGY_USERS = "topology.users";

    /**
     * A list of groups that are allowed to interact with the topology.  To use this set
     * nimbus.authorizer to org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String TOPOLOGY_GROUPS = "topology.groups";

    /**
     * A list of readonly users that are allowed to interact with the topology.  To use this set
     * nimbus.authorizer to org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String TOPOLOGY_READONLY_USERS="topology.readonly.users";

    /**
     * A list of readonly groups that are allowed to interact with the topology.  To use this set
     * nimbus.authorizer to org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String TOPOLOGY_READONLY_GROUPS = "topology.readonly.groups";

    /**
     * True if Storm should timeout messages or not. Defaults to true. This is meant to be used
     * in unit tests to prevent tuples from being accidentally timed out during the test.
     */
    @isBoolean
    public static final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";

    /**
     * When set to true, Storm will log every message that's emitted.
     */
    @isBoolean
    public static final String TOPOLOGY_DEBUG = "topology.debug";

    /**
     * User defined version of this topology
     */
    @isString
    public static final String TOPOLOGY_VERSION = "topology.version";

    /**
     * The serializer for communication between shell components and non-JVM
     * processes
     */
    @isString
    public static final String TOPOLOGY_MULTILANG_SERIALIZER = "topology.multilang.serializer";

    /**
     * How many processes should be spawned around the cluster to execute this
     * topology. Each process will execute some number of tasks as threads within
     * them. This parameter should be used in conjunction with the parallelism hints
     * on each component in the topology to tune the performance of a topology.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_WORKERS = "topology.workers";

    /**
     * How many instances to create for a spout/bolt. A task runs on a thread with zero or more
     * other tasks for the same spout/bolt. The number of tasks for a spout/bolt is always
     * the same throughout the lifetime of a topology, but the number of executors (threads) for
     * a spout/bolt can change over time. This allows a topology to scale to more or less resources
     * without redeploying the topology or violating the constraints of Storm (such as a fields grouping
     * guaranteeing that the same value goes to the same task).
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_TASKS = "topology.tasks";

    /**
     * The maximum amount of memory an instance of a spout/bolt will take on heap. This enables the scheduler
     * to allocate slots on machines with enough available memory. A default value will be set for this config if user does not override
     */
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB = "topology.component.resources.onheap.memory.mb";

    /**
     * The maximum amount of memory an instance of a spout/bolt will take off heap. This enables the scheduler
     * to allocate slots on machines with enough available memory.  A default value will be set for this config if user does not override
     */
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB = "topology.component.resources.offheap.memory.mb";

    /**
     * The config indicates the percentage of cpu for a core an instance(executor) of a component will use.
     * Assuming the a core value to be 100, a value of 10 indicates 10% of the core.
     * The P in PCORE represents the term "physical".  A default value will be set for this config if user does not override
     */
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT = "topology.component.cpu.pcore.percent";

    /**
     * The maximum amount of memory an instance of an acker will take on heap. This enables the scheduler
     * to allocate slots on machines with enough available memory.  A default value will be set for this config if user does not override
     */
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB = "topology.acker.resources.onheap.memory.mb";

    /**
     * The maximum amount of memory an instance of an acker will take off heap. This enables the scheduler
     * to allocate slots on machines with enough available memory.  A default value will be set for this config if user does not override
     */
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_ACKER_RESOURCES_OFFHEAP_MEMORY_MB = "topology.acker.resources.offheap.memory.mb";

    /**
     * The config indicates the percentage of cpu for a core an instance(executor) of an acker will use.
     * Assuming the a core value to be 100, a value of 10 indicates 10% of the core.
     * The P in PCORE represents the term "physical".  A default value will be set for this config if user does not override
     */
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_ACKER_CPU_PCORE_PERCENT = "topology.acker.cpu.pcore.percent";

    /**
     * The class name of the {@link org.apache.storm.state.StateProvider} implementation. If not specified
     * defaults to {@link org.apache.storm.state.InMemoryKeyValueStateProvider}. This can be overridden
     * at the component level.
     */
    @isString
    public static final String TOPOLOGY_STATE_PROVIDER = "topology.state.provider";

    /**
     * The configuration specific to the {@link org.apache.storm.state.StateProvider} implementation.
     * This can be overridden at the component level. The value and the interpretation of this config
     * is based on the state provider implementation. For e.g. this could be just a config file name
     * which contains the config for the state provider implementation.
     */
    @isString
    public static final String TOPOLOGY_STATE_PROVIDER_CONFIG = "topology.state.provider.config";

    /**
     * Topology configuration to specify the checkpoint interval (in millis) at which the
     * topology state is saved when {@link org.apache.storm.topology.IStatefulBolt} bolts are involved.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_STATE_CHECKPOINT_INTERVAL = "topology.state.checkpoint.interval.ms";

    /**
     * A per topology config that specifies the maximum amount of memory a worker can use for that specific topology
     */
    @isPositiveNumber
    public static final String TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB = "topology.worker.max.heap.size.mb";

    /**
     * The strategy to use when scheduling a topology with Resource Aware Scheduler
     */
    @NotNull
    @isString
    //NOTE: @isImplementationOfClass(implementsClass = IStrategy.class) is enforced in DaemonConf, so
    // an error will be thrown by nimbus on topology submission and not by the client prior to submitting
    // the topology.
    public static final String TOPOLOGY_SCHEDULER_STRATEGY = "topology.scheduler.strategy";

    /**
     * A list of host names that this topology would prefer to be scheduled on (no guarantee is given though).
     * This is intended for debugging only.
     */
    @isStringList
    public static final String TOPOLOGY_SCHEDULER_FAVORED_NODES = "topology.scheduler.favored.nodes";

    /**
     * A list of host names that this topology would prefer to NOT be scheduled on (no guarantee is given though).
     * This is intended for debugging only.
     */
    @isStringList
    public static final String TOPOLOGY_SCHEDULER_UNFAVORED_NODES = "topology.scheduler.unfavored.nodes";

    /**
     * How many executors to spawn for ackers.
     *
     * <p>By not setting this variable or setting it as null, Storm will set the number of acker executors
     * to be equal to the number of workers configured for this topology. If this variable is set to 0,
     * then Storm will immediately ack tuples as soon as they come off the spout, effectively disabling reliability.</p>
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_ACKER_EXECUTORS = "topology.acker.executors";

    /**
     * How many executors to spawn for event logger.
     *
     * <p>By not setting this variable or setting it as null, Storm will set the number of eventlogger executors
     * to be equal to the number of workers configured for this topology. If this variable is set to 0,
     * event logging will be disabled.</p>
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_EVENTLOGGER_EXECUTORS = "topology.eventlogger.executors";

    /**
     * The maximum amount of time given to the topology to fully process a message
     * emitted by a spout. If the message is not acked within this time frame, Storm
     * will fail the message on the spout. Some spouts implementations will then replay
     * the message at a later time.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";

    /**
     * A list of serialization registrations for Kryo ( https://github.com/EsotericSoftware/kryo ),
     * the underlying serialization framework for Storm. A serialization can either
     * be the name of a class (in which case Kryo will automatically create a serializer for the class
     * that saves all the object's fields), or an implementation of com.esotericsoftware.kryo.Serializer.
     *
     * See Kryo's documentation for more information about writing custom serializers.
     */
    @isKryoReg
    public static final String TOPOLOGY_KRYO_REGISTER = "topology.kryo.register";

    /**
     * A list of classes that customize storm's kryo instance during start-up.
     * Each listed class name must implement IKryoDecorator. During start-up the
     * listed class is instantiated with 0 arguments, then its 'decorate' method
     * is called with storm's kryo instance as the only argument.
     */
    @isStringList
    public static final String TOPOLOGY_KRYO_DECORATORS = "topology.kryo.decorators";

    /**
     * Class that specifies how to create a Kryo instance for serialization. Storm will then apply
     * topology.kryo.register and topology.kryo.decorators on top of this. The default implementation
     * implements topology.fall.back.on.java.serialization and turns references off.
     */
    @isString
    public static final String TOPOLOGY_KRYO_FACTORY = "topology.kryo.factory";

    /**
     * Whether or not Storm should skip the loading of kryo registrations for which it
     * does not know the class or have the serializer implementation. Otherwise, the task will
     * fail to load and will throw an error at runtime. The use case of this is if you want to
     * declare your serializations on the storm.yaml files on the cluster rather than every single
     * time you submit a topology. Different applications may use different serializations and so
     * a single application may not have the code for the other serializers used by other apps.
     * By setting this config to true, Storm will ignore that it doesn't have those other serializations
     * rather than throw an error.
     */
    @isBoolean
    public static final String TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS= "topology.skip.missing.kryo.registrations";

    /**
     * List of classes to register during state serialization
     */
    @isStringList
    public static final String TOPOLOGY_STATE_KRYO_REGISTER = "topology.state.kryo.register";

    /**
     * A list of classes implementing IMetricsConsumer (See storm.yaml.example for exact config format).
     * Each listed class will be routed all the metrics data generated by the storm metrics API.
     * Each listed class maps 1:1 to a system bolt named __metrics_ClassName#N, and it's parallelism is configurable.
     */

    @isListEntryCustom(entryValidatorClasses={MetricRegistryValidator.class})
    public static final String TOPOLOGY_METRICS_CONSUMER_REGISTER = "topology.metrics.consumer.register";

    /**
     * A map of metric name to class name implementing IMetric that will be created once per worker JVM
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String TOPOLOGY_WORKER_METRICS = "topology.worker.metrics";

    /**
     * A map of metric name to class name implementing IMetric that will be created once per worker JVM
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String WORKER_METRICS = "worker.metrics";

    /**
     * The maximum parallelism allowed for a component in this topology. This configuration is
     * typically used in testing to limit the number of threads spawned in local mode.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_MAX_TASK_PARALLELISM="topology.max.task.parallelism";

    /**
     * The maximum number of tuples that can be pending on a spout task at any given time.
     * This config applies to individual tasks, not to spouts or topologies as a whole.
     *
     * A pending tuple is one that has been emitted from a spout but has not been acked or failed yet.
     * Note that this config parameter has no effect for unreliable spouts that don't tag
     * their tuples with a message id.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_MAX_SPOUT_PENDING="topology.max.spout.pending";

    /**
     * A class that implements a strategy for what to do when a spout needs to wait. Waiting is
     * triggered in one of two conditions:
     *
     * 1. nextTuple emits no tuples
     * 2. The spout has hit maxSpoutPending and can't emit any more tuples
     */
    @isString
    public static final String TOPOLOGY_SPOUT_WAIT_STRATEGY="topology.spout.wait.strategy";

    /**
     * The amount of milliseconds the SleepEmptyEmitStrategy should sleep for.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS="topology.sleep.spout.wait.strategy.time.ms";

    /**
     * The maximum amount of time a component gives a source of state to synchronize before it requests
     * synchronization again.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String TOPOLOGY_STATE_SYNCHRONIZATION_TIMEOUT_SECS="topology.state.synchronization.timeout.secs";

    /**
     * The percentage of tuples to sample to produce stats for a task.
     */
    @isPositiveNumber
    public static final String TOPOLOGY_STATS_SAMPLE_RATE="topology.stats.sample.rate";

    /**
     * The time period that builtin metrics data in bucketed into.
     */
    @isInteger
    public static final String TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS="topology.builtin.metrics.bucket.size.secs";

    /**
     * Whether or not to use Java serialization in a topology.
     */
    @isBoolean
    public static final String TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION="topology.fall.back.on.java.serialization";

    /**
     * Topology-specific options for the worker child process. This is used in addition to WORKER_CHILDOPTS.
     */
    @isStringOrStringList
    public static final String TOPOLOGY_WORKER_CHILDOPTS="topology.worker.childopts";

    /**
     * Topology-specific options GC for the worker child process. This overrides WORKER_GC_CHILDOPTS.
     */
    @isStringOrStringList
    public static final String TOPOLOGY_WORKER_GC_CHILDOPTS="topology.worker.gc.childopts";

    /**
     * Topology-specific options for the logwriter process of a worker.
     */
    @isStringOrStringList
    public static final String TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS="topology.worker.logwriter.childopts";

    /**
     * Topology-specific classpath for the worker child process. This is combined to the usual classpath.
     */
    @isStringOrStringList
    public static final String TOPOLOGY_CLASSPATH="topology.classpath";

    /**
     * Topology-specific classpath for the worker child process. This will be *prepended* to
     * the usual classpath, meaning it can override the Storm classpath. This is for debugging
     * purposes, and is disabled by default. To allow topologies to be submitted with user-first
     * classpaths, set the storm.topology.classpath.beginning.enabled config to true.
     */
    @isStringOrStringList
    public static final String TOPOLOGY_CLASSPATH_BEGINNING="topology.classpath.beginning";

    /**
     * Topology-specific environment variables for the worker child process.
     * This is added to the existing environment (that of the supervisor)
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String TOPOLOGY_ENVIRONMENT="topology.environment";

    /*
     * Topology-specific option to disable/enable bolt's outgoing overflow buffer.
     * Enabling this option ensures that the bolt can always clear the incoming messages,
     * preventing live-lock for the topology with cyclic flow.
     * The overflow buffer can fill degrading the performance gradually,
     * eventually running out of memory.
     */
    @isBoolean
    public static final String TOPOLOGY_BOLTS_OUTGOING_OVERFLOW_BUFFER_ENABLE="topology.bolts.outgoing.overflow.buffer.enable";

    /*
     * Bolt-specific configuration for windowed bolts to specify the window length as a count of number of tuples
     * in the window.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT = "topology.bolts.window.length.count";

    /*
     * Bolt-specific configuration for windowed bolts to specify the window length in time duration.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS = "topology.bolts.window.length.duration.ms";

    /*
     * Bolt-specific configuration for windowed bolts to specify the sliding interval as a count of number of tuples.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT = "topology.bolts.window.sliding.interval.count";

    /*
     * Bolt-specific configuration for windowed bolts to specify the sliding interval in time duration.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS = "topology.bolts.window.sliding.interval.duration.ms";

    /**
     * Bolt-specific configuration for windowed bolts to specify the name of the stream on which late tuples are
     * going to be emitted. This configuration should only be used from the BaseWindowedBolt.withLateTupleStream builder
     * method, and not as global parameter, otherwise IllegalArgumentException is going to be thrown.
     */
    @isString
    public static final String TOPOLOGY_BOLTS_LATE_TUPLE_STREAM = "topology.bolts.late.tuple.stream";

    /**
     * Bolt-specific configuration for windowed bolts to specify the maximum time lag of the tuple timestamp
     * in milliseconds. It means that the tuple timestamps cannot be out of order by more than this amount.
     * This config will be effective only if {@link org.apache.storm.windowing.TimestampExtractor} is specified.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS = "topology.bolts.tuple.timestamp.max.lag.ms";

    /*
     * Bolt-specific configuration for windowed bolts to specify the time interval for generating
     * watermark events. Watermark event tracks the progress of time when tuple timestamp is used.
     * This config is effective only if {@link org.apache.storm.windowing.TimestampExtractor} is specified.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS = "topology.bolts.watermark.event.interval.ms";

    /*
     * Bolt-specific configuration for windowed bolts to specify the name of the field in the tuple that holds
     * the message id. This is used to track the windowing boundaries and avoid re-evaluating the windows
     * during recovery of IStatefulWindowedBolt
     */
    @isString
    public static final String TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME = "topology.bolts.message.id.field.name";

    /**
     * This config is available for TransactionalSpouts, and contains the id ( a String) for
     * the transactional topology. This id is used to store the state of the transactional
     * topology in Zookeeper.
     */
    @isString
    public static final String TOPOLOGY_TRANSACTIONAL_ID="topology.transactional.id";

    /**
     * A list of task hooks that are automatically added to every spout and bolt in the topology. An example
     * of when you'd do this is to add a hook that integrates with your internal
     * monitoring system. These hooks are instantiated using the zero-arg constructor.
     */
    @isStringList
    public static final String TOPOLOGY_AUTO_TASK_HOOKS="topology.auto.task.hooks";

    /**
     * The size of the Disruptor receive queue for each executor. Must be a power of 2.
     */
    @isPowerOf2
    public static final String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE="topology.executor.receive.buffer.size";

    /**
     * The size of the Disruptor send queue for each executor. Must be a power of 2.
     */
    @isPowerOf2
    public static final String TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE="topology.executor.send.buffer.size";

    /**
     * The size of the Disruptor transfer queue for each worker.
     */
    @isInteger
    @isPowerOf2
    public static final String TOPOLOGY_TRANSFER_BUFFER_SIZE="topology.transfer.buffer.size";

    /**
     * How often a tick tuple from the "__system" component and "__tick" stream should be sent
     * to tasks. Meant to be used as a component-specific configuration.
     */
    @isInteger
    public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS="topology.tick.tuple.freq.secs";

    /**
     * @deprecated this is no longer supported
     * Configure the wait strategy used for internal queuing. Can be used to tradeoff latency
     * vs. throughput
     */
    @Deprecated
    @isString
    public static final String TOPOLOGY_DISRUPTOR_WAIT_STRATEGY="topology.disruptor.wait.strategy";

    /**
     * The size of the shared thread pool for worker tasks to make use of. The thread pool can be accessed
     * via the TopologyContext.
     */
    @isInteger
    public static final String TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE="topology.worker.shared.thread.pool.size";

    /**
     * The interval in seconds to use for determining whether to throttle error reported to Zookeeper. For example,
     * an interval of 10 seconds with topology.max.error.report.per.interval set to 5 will only allow 5 errors to be
     * reported to Zookeeper per task for every 10 second interval of time.
     */
    @isInteger
    public static final String TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS="topology.error.throttle.interval.secs";

    /**
     * See doc for TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL="topology.max.error.report.per.interval";

    /**
     * How often a batch can be emitted in a Trident topology.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS="topology.trident.batch.emit.interval.millis";

    /**
     * Maximum number of tuples that can be stored inmemory cache in windowing operators for fast access without fetching
     * them from store.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_TRIDENT_WINDOWING_INMEMORY_CACHE_LIMIT="topology.trident.windowing.cache.tuple.limit";

    /**
     * The id assigned to a running topology. The id is the storm name with a unique nonce appended.
     */
    @isString
    public static final String STORM_ID = "storm.id";

    /**
     * Name of the topology. This config is automatically set by Storm when the topology is submitted.
     */
    @isString
    public final static String TOPOLOGY_NAME="topology.name";

    /**
     * The principal who submitted a topology
     */
    @isString
    public final static String TOPOLOGY_SUBMITTER_PRINCIPAL = "topology.submitter.principal";

    /**
     * The local user name of the user who submitted a topology.
     */
    @isString
    public static final String TOPOLOGY_SUBMITTER_USER = "topology.submitter.user";

    /**
     * Array of components that scheduler should try to place on separate hosts.
     */
    @isStringList
    public static final String TOPOLOGY_SPREAD_COMPONENTS = "topology.spread.components";

    /**
     * A list of IAutoCredentials that the topology should load and use.
     */
    @isStringList
    public static final String TOPOLOGY_AUTO_CREDENTIALS = "topology.auto-credentials";

    /**
     * Max pending tuples in one ShellBolt
     */
    @NotNull
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_SHELLBOLT_MAX_PENDING="topology.shellbolt.max.pending";

    /**
     * How long a subprocess can go without heartbeating before the ShellSpout/ShellBolt tries to
     * suicide itself.
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_SUBPROCESS_TIMEOUT_SECS = "topology.subprocess.timeout.secs";

    /**
     * Topology central logging sensitivity to determine who has access to logs in central logging system.
     * The possible values are:
     *   S0 - Public (open to all users on grid)
     *   S1 - Restricted
     *   S2 - Confidential
     *   S3 - Secret (default.)
     */
    @isString(acceptedValues = {"S0", "S1", "S2", "S3"})
    public static final String TOPOLOGY_LOGGING_SENSITIVITY="topology.logging.sensitivity";

    /**
     * Sets the priority for a topology
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_PRIORITY = "topology.priority";

    /**
     * The root directory in ZooKeeper for metadata about TransactionalSpouts.
     */
    @isString
    public static final String TRANSACTIONAL_ZOOKEEPER_ROOT="transactional.zookeeper.root";

    /**
     * The list of zookeeper servers in which to keep the transactional state. If null (which is default),
     * will use storm.zookeeper.servers
     */
    @isStringList
    public static final String TRANSACTIONAL_ZOOKEEPER_SERVERS="transactional.zookeeper.servers";

    /**
     * The port to use to connect to the transactional zookeeper servers. If null (which is default),
     * will use storm.zookeeper.port
     */
    @isInteger
    @isPositiveNumber
    public static final String TRANSACTIONAL_ZOOKEEPER_PORT="transactional.zookeeper.port";

    /**
     * The user as which the nimbus client should be acquired to perform the operation.
     */
    @isString
    public static final String STORM_DO_AS_USER="storm.doAsUser";

    /**
     * The number of machines that should be used by this topology to isolate it from all others. Set storm.scheduler
     * to org.apache.storm.scheduler.multitenant.MultitenantScheduler
     */
    @isInteger
    @isPositiveNumber
    public static final String TOPOLOGY_ISOLATED_MACHINES = "topology.isolate.machines";

    /**
     * Configure timeout milliseconds used for disruptor queue wait strategy. Can be used to tradeoff latency
     * vs. CPU usage
     */
    @isInteger
    @NotNull
    public static final String TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS="topology.disruptor.wait.timeout.millis";

    /**
     * The number of tuples to batch before sending to the next thread.  This number is just an initial suggestion and
     * the code may adjust it as your topology runs.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String TOPOLOGY_DISRUPTOR_BATCH_SIZE="topology.disruptor.batch.size";

    /**
     * The maximum age in milliseconds a batch can be before being sent to the next thread.  This number is just an
     * initial suggestion and the code may adjust it as your topology runs.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS="topology.disruptor.batch.timeout.millis";

    /**
     * Minimum number of nimbus hosts where the code must be replicated before leader nimbus
     * is allowed to perform topology activation tasks like setting up heartbeats/assignments
     * and marking the topology as active. default is 0.
     */
    @isNumber
    public static final String TOPOLOGY_MIN_REPLICATION_COUNT = "topology.min.replication.count";

    /**
     * Maximum wait time for the nimbus host replication to achieve the nimbus.min.replication.count.
     * Once this time is elapsed nimbus will go ahead and perform topology activation tasks even
     * if required nimbus.min.replication.count is not achieved. The default is 0 seconds, a value of
     * -1 indicates to wait for ever.
     */
    @isNumber
    public static final String TOPOLOGY_MAX_REPLICATION_WAIT_TIME_SEC = "topology.max.replication.wait.time.sec";

    /**
     * This is a config that is not likely to be used.  Internally the disruptor queue will batch entries written
     * into the queue.  A background thread pool will flush those batches if they get too old.  By default that
     * pool can grow rather large, and sacrifice some CPU time to keep the latency low.  In some cases you may
     * want the queue to be smaller so there is less CPU used, but the latency will increase in some situations.
     * This configs is on a per cluster bases, if you want to control this on a per topology bases you need to set
     * the java System property for the worker "num_flusher_pool_threads" to the value you want.
     */
    @isInteger
    public static final String STORM_WORKER_DISRUPTOR_FLUSHER_MAX_POOL_SIZE = "storm.worker.disruptor.flusher.max.pool.size";

    /**
     * The list of servers that Pacemaker is running on.
     */
    @isStringList
    public static final String PACEMAKER_SERVERS = "pacemaker.servers";

    /**
     * The port Pacemaker should run on. Clients should
     * connect to this port to submit or read heartbeats.
     */
    @isNumber
    @isPositiveNumber
    public static final String PACEMAKER_PORT = "pacemaker.port";

    /**
     * This should be one of "DIGEST", "KERBEROS", or "NONE"
     * Determines the mode of authentication the pacemaker server and client use.
     * The client must either match the server, or be NONE. In the case of NONE,
     * no authentication is performed for the client, and if the server is running with
     * DIGEST or KERBEROS, the client can only write to the server (no reads).
     * This is intended to provide a primitive form of access-control.
     */
    @CustomValidator(validatorClass=ConfigValidation.PacemakerAuthTypeValidator.class)
    public static final String PACEMAKER_AUTH_METHOD = "pacemaker.auth.method";

    /**
     * Pacemaker Thrift Max Message Size (bytes).
     */
    @isInteger
    @isPositiveNumber
    public static final String PACEMAKER_THRIFT_MESSAGE_SIZE_MAX = "pacemaker.thrift.message.size.max";

    /**
     * Max no.of seconds group mapping service will cache user groups
     */
    @isInteger
    public static final String STORM_GROUP_MAPPING_SERVICE_CACHE_DURATION_SECS = "storm.group.mapping.service.cache.duration.secs";

    /**
     * List of DRPC servers so that the DRPCSpout knows who to talk to.
     */
    @isStringList
    public static final String DRPC_SERVERS = "drpc.servers";

    /**
     * This port on Storm DRPC is used by DRPC topologies to receive function invocations and send results back.
     */
    @isInteger
    @isPositiveNumber
    public static final String DRPC_INVOCATIONS_PORT = "drpc.invocations.port";

    /**
     * The number of times to retry a Nimbus operation.
     */
    @isNumber
    public static final String STORM_NIMBUS_RETRY_TIMES="storm.nimbus.retry.times";

    /**
     * The starting interval between exponential backoff retries of a Nimbus operation.
     */
    @isNumber
    public static final String STORM_NIMBUS_RETRY_INTERVAL="storm.nimbus.retry.interval.millis";

    /**
     * The ceiling of the interval between retries of a client connect to Nimbus operation.
     */
    @isNumber
    public static final String STORM_NIMBUS_RETRY_INTERVAL_CEILING="storm.nimbus.retry.intervalceiling.millis";

    /**
     * The Nimbus transport plug-in for Thrift client/server communication
     */
    @isString
    public static final String NIMBUS_THRIFT_TRANSPORT_PLUGIN = "nimbus.thrift.transport";

    /**
     * Which port the Thrift interface of Nimbus should run on. Clients should
     * connect to this port to upload jars and submit topologies.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_THRIFT_PORT = "nimbus.thrift.port";

    /**
     * Nimbus thrift server queue size, default is 100000. This is the request queue size , when there are more requests
     * than number of threads to serve the requests, those requests will be queued to this queue. If the request queue
     * size > this config, then the incoming requests will be rejected.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_QUEUE_SIZE = "nimbus.queue.size";

    /**
     * The number of threads that should be used by the nimbus thrift server.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_THRIFT_THREADS = "nimbus.thrift.threads";

    /**
     * The maximum buffer size thrift should use when reading messages.
     */
    @isInteger
    @isPositiveNumber
    public static final String NIMBUS_THRIFT_MAX_BUFFER_SIZE = "nimbus.thrift.max_buffer_size";

    /**
     * How long before a Thrift Client socket hangs before timeout
     * and restart the socket.
     */
    @isInteger
    public static final String STORM_THRIFT_SOCKET_TIMEOUT_MS = "storm.thrift.socket.timeout.ms";

    /**
     * The DRPC transport plug-in for Thrift client/server communication
     */
    @isString
    public static final String DRPC_THRIFT_TRANSPORT_PLUGIN = "drpc.thrift.transport";

    /**
     * This port is used by Storm DRPC for receiving DPRC requests from clients.
     */
    @isInteger
    @isPositiveNumber
    public static final String DRPC_PORT = "drpc.port";

    /**
     * DRPC thrift server queue size
     */
    @isInteger
    @isPositiveNumber
    public static final String DRPC_QUEUE_SIZE = "drpc.queue.size";

    /**
     * DRPC thrift server worker threads
     */
    @isInteger
    @isPositiveNumber
    public static final String DRPC_WORKER_THREADS = "drpc.worker.threads";

    /**
     * The maximum buffer size thrift should use when reading messages for DRPC.
     */
    @isNumber
    @isPositiveNumber
    public static final String DRPC_MAX_BUFFER_SIZE = "drpc.max_buffer_size";

    /**
     * The DRPC invocations transport plug-in for Thrift client/server communication
     */
    @isString
    public static final String DRPC_INVOCATIONS_THRIFT_TRANSPORT_PLUGIN = "drpc.invocations.thrift.transport";

    /**
     * DRPC invocations thrift server worker threads
     */
    @isInteger
    @isPositiveNumber
    public static final String DRPC_INVOCATIONS_THREADS = "drpc.invocations.threads";

    /**
     * The default transport plug-in for Thrift client/server communication
     */
    @isString
    public static final String STORM_THRIFT_TRANSPORT_PLUGIN = "storm.thrift.transport";

    /**
     * How long a worker can go without heartbeating before the supervisor tries to
     * restart the worker process.
     */
    @isInteger
    @isPositiveNumber
    @NotNull
    public static final String SUPERVISOR_WORKER_TIMEOUT_SECS = "supervisor.worker.timeout.secs";

    /**
     * A list of hosts of ZooKeeper servers used to manage the cluster.
     */
    @isStringList
    public static final String STORM_ZOOKEEPER_SERVERS = "storm.zookeeper.servers";

    /**
     * The port Storm will use to connect to each of the ZooKeeper servers.
     */
    @isInteger
    @isPositiveNumber
    public static final String STORM_ZOOKEEPER_PORT = "storm.zookeeper.port";

    /**
     * This is part of a temporary workaround to a ZK bug, it is the 'scheme:acl' for
     * the user Nimbus and Supervisors use to authenticate with ZK.
     */
    @isString
    public static final String STORM_ZOOKEEPER_SUPERACL = "storm.zookeeper.superACL";

    /**
     * The topology Zookeeper authentication scheme to use, e.g. "digest". It is the internal config and user shouldn't set it.
     */
    @isString
    public static final String STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME="storm.zookeeper.topology.auth.scheme";

    /**
     * The delegate for serializing metadata, should be used for serialized objects stored in zookeeper and on disk.
     * This is NOT used for compressing serialized tuples sent between topologies.
     */
    @isString
    public static final String STORM_META_SERIALIZATION_DELEGATE = "storm.meta.serialization.delegate";

    /**
     * What blobstore implementation the storm client should use.
     */
    @isString
    public static final String CLIENT_BLOBSTORE = "client.blobstore.class";


    /**
     * The blobstore super user has all read/write/admin permissions to all blobs - user running
     * the blobstore.
     */
    @isString
    public static final String BLOBSTORE_SUPERUSER = "blobstore.superuser";

    /**
     * What directory to use for the blobstore. The directory is expected to be an
     * absolute path when using HDFS blobstore, for LocalFsBlobStore it could be either
     * absolute or relative. If the setting is a relative directory, it is relative to
     * root directory of Storm installation.
     */
    @isString
    public static final String BLOBSTORE_DIR = "blobstore.dir";

    /**
     * Enable the blobstore cleaner. Certain blobstores may only want to run the cleaner
     * on one daemon. Currently Nimbus handles setting this.
     */
    @isBoolean
    public static final String BLOBSTORE_CLEANUP_ENABLE = "blobstore.cleanup.enable";

    /**
     * principal for nimbus/supervisor to use to access secure hdfs for the blobstore.
     */
    @isString
    public static final String BLOBSTORE_HDFS_PRINCIPAL = "blobstore.hdfs.principal";

    /**
     * keytab for nimbus/supervisor to use to access secure hdfs for the blobstore.
     */
    @isString
    public static final String BLOBSTORE_HDFS_KEYTAB = "blobstore.hdfs.keytab";

    /**
     *  Set replication factor for a blob in HDFS Blobstore Implementation
     */
    @isPositiveNumber
    @isInteger
    public static final String STORM_BLOBSTORE_REPLICATION_FACTOR = "storm.blobstore.replication.factor";

    /**
     * The hostname the supervisors/workers should report to nimbus. If unset, Storm will
     * get the hostname to report by calling <code>InetAddress.getLocalHost().getCanonicalHostName()</code>.
     *
     * You should set this config when you don't have a DNS which supervisors/workers
     * can utilize to find each other based on hostname got from calls to
     * <code>InetAddress.getLocalHost().getCanonicalHostName()</code>.
     */
    @isString
    public static final String STORM_LOCAL_HOSTNAME = "storm.local.hostname";

    /**
     * The host that the master server is running on, added only for backward compatibility,
     * the usage deprecated in favor of nimbus.seeds config.
     */
    @Deprecated
    @isString
    public static final String NIMBUS_HOST = "nimbus.host";

    /**
     * List of seed nimbus hosts to use for leader nimbus discovery.
     */
    @isStringList
    public static final String NIMBUS_SEEDS = "nimbus.seeds";

    /**
     * A list of users that are the only ones allowed to run user operation on storm cluster.
     * To use this set nimbus.authorizer to org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String NIMBUS_USERS = "nimbus.users";

    /**
     * A list of groups , users belong to these groups are the only ones allowed to run user operation on storm cluster.
     * To use this set nimbus.authorizer to org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String NIMBUS_GROUPS = "nimbus.groups";

    /**
     * The mode this Storm cluster is running in. Either "distributed" or "local".
     */
    @isString
    public static final String STORM_CLUSTER_MODE = "storm.cluster.mode";

    /**
     * The root location at which Storm stores data in ZooKeeper.
     */
    @isString
    public static final String STORM_ZOOKEEPER_ROOT = "storm.zookeeper.root";

    /**
     * A string representing the payload for topology Zookeeper authentication. It gets serialized using UTF-8 encoding during authentication.
     */
    @isString
    public static final String STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD="storm.zookeeper.topology.auth.payload";

    /**
     * The cluster Zookeeper authentication scheme to use, e.g. "digest". Defaults to no authentication.
     */
    @isString
    public static final String STORM_ZOOKEEPER_AUTH_SCHEME="storm.zookeeper.auth.scheme";

    /**
     * A string representing the payload for cluster Zookeeper authentication.
     * It gets serialized using UTF-8 encoding during authentication.
     * Note that if this is set to something with a secret (as when using
     * digest authentication) then it should only be set in the
     * storm-cluster-auth.yaml file.
     * This file storm-cluster-auth.yaml should then be protected with
     * appropriate permissions that deny access from workers.
     */
    @isString
    public static final String STORM_ZOOKEEPER_AUTH_PAYLOAD="storm.zookeeper.auth.payload";

    /**
     * What Network Topography detection classes should we use.
     * Given a list of supervisor hostnames (or IP addresses), this class would return a list of
     * rack names that correspond to the supervisors. This information is stored in Cluster.java, and
     * is used in the resource aware scheduler.
     */
    @NotNull
    @isImplementationOfClass(implementsClass = org.apache.storm.networktopography.DNSToSwitchMapping.class)
    public static final String STORM_NETWORK_TOPOGRAPHY_PLUGIN = "storm.network.topography.plugin";

    /**
     * The jvm opts provided to workers launched by this supervisor for GC. All "%ID%" substrings are replaced
     * with an identifier for this worker.  Because the JVM complains about multiple GC opts the topology
     * can override this default value by setting topology.worker.gc.childopts.
     */
    @isStringOrStringList
    public static final String WORKER_GC_CHILDOPTS = "worker.gc.childopts";

    /**
     * The jvm opts provided to workers launched by this supervisor.
     * All "%ID%", "%WORKER-ID%", "%TOPOLOGY-ID%",
     * "%WORKER-PORT%" and "%HEAP-MEM%" substrings are replaced with:
     * %ID%          -> port (for backward compatibility),
     * %WORKER-ID%   -> worker-id,
     * %TOPOLOGY-ID%    -> topology-id,
     * %WORKER-PORT% -> port.
     * %HEAP-MEM% -> mem-onheap.
     */
    @isStringOrStringList
    public static final String WORKER_CHILDOPTS = "worker.childopts";

    /**
     * The default heap memory size in MB per worker, used in the jvm -Xmx opts for launching the worker
     */
    @isInteger
    @isPositiveNumber
    public static final String WORKER_HEAP_MEMORY_MB = "worker.heap.memory.mb";

    /**
     * The total amount of memory (in MiB) a supervisor is allowed to give to its workers.
     *  A default value will be set for this config if user does not override
     */
    @isPositiveNumber
    public static final String SUPERVISOR_MEMORY_CAPACITY_MB = "supervisor.memory.capacity.mb";

    /**
     * The total amount of CPU resources a supervisor is allowed to give to its workers.
     * By convention 1 cpu core should be about 100, but this can be adjusted if needed
     * using 100 makes it simple to set the desired value to the capacity measurement
     * for single threaded bolts.  A default value will be set for this config if user does not override
     */
    @isPositiveNumber
    public static final String SUPERVISOR_CPU_CAPACITY = "supervisor.cpu.capacity";

    /**
     * Whether or not to use ZeroMQ for messaging in local mode. If this is set
     * to false, then Storm will use a pure-Java messaging system. The purpose
     * of this flag is to make it easy to run Storm in local mode by eliminating
     * the need for native dependencies, which can be difficult to install.
     *
     * Defaults to false.
     */
    @isBoolean
    public static final String STORM_LOCAL_MODE_ZMQ = "storm.local.mode.zmq";

    /**
     * The transporter for communication among Storm tasks
     */
    @isString
    public static final String STORM_MESSAGING_TRANSPORT = "storm.messaging.transport";

    /**
     * Netty based messaging: Is authentication required for Netty messaging from client worker process to server worker process.
     */
    @isBoolean
    public static final String STORM_MESSAGING_NETTY_AUTHENTICATION = "storm.messaging.netty.authentication";

    /**
     * Netty based messaging: The buffer size for send/recv buffer
     */
    @isInteger
    @isPositiveNumber
    public static final String STORM_MESSAGING_NETTY_BUFFER_SIZE = "storm.messaging.netty.buffer_size";

    /**
     * Netty based messaging: Sets the backlog value to specify when the channel binds to a local address
     */
    @isInteger
    @isPositiveNumber
    public static final String STORM_MESSAGING_NETTY_SOCKET_BACKLOG = "storm.messaging.netty.socket.backlog";

    /**
     * Netty based messaging: The # of worker threads for the server.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS = "storm.messaging.netty.server_worker_threads";

    /**
     * If the Netty messaging layer is busy, the Netty client will try to batch message as more as possible up to the size of STORM_NETTY_MESSAGE_BATCH_SIZE bytes
     */
    @isInteger
    public static final String STORM_NETTY_MESSAGE_BATCH_SIZE = "storm.messaging.netty.transfer.batch.size";

    /**
     * Netty based messaging: The max # of retries that a peer will perform when a remote is not accessible
     *@deprecated "Since netty clients should never stop reconnecting - this does not make sense anymore.
     */
    @Deprecated
    @isInteger
    public static final String STORM_MESSAGING_NETTY_MAX_RETRIES = "storm.messaging.netty.max_retries";

    /**
     * Netty based messaging: The min # of milliseconds that a peer will wait.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String STORM_MESSAGING_NETTY_MIN_SLEEP_MS = "storm.messaging.netty.min_wait_ms";

    /**
     * Netty based messaging: The max # of milliseconds that a peer will wait.
     */
    @isInteger
    @isPositiveNumber(includeZero = true)
    public static final String STORM_MESSAGING_NETTY_MAX_SLEEP_MS = "storm.messaging.netty.max_wait_ms";

    /**
     * Netty based messaging: The # of worker threads for the client.
     */
    @isInteger
    public static final String STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS = "storm.messaging.netty.client_worker_threads";

    /**
     * Should the supervior try to run the worker as the lauching user or not.  Defaults to false.
     */
    @isBoolean
    public static final String SUPERVISOR_RUN_WORKER_AS_USER = "supervisor.run.worker.as.user";

    /**
     * On some systems (windows for example) symlinks require special privileges that not everyone wants to
     * grant a headless user.  You can completely disable the use of symlinks by setting this config to true, but
     * by doing so you may also lose some features from storm.  For example the blobstore feature
     * does not currently work without symlinks enabled.
     */
    @isBoolean
    public static final String DISABLE_SYMLINKS = "storm.disable.symlinks";

    /**
     * The plugin that will convert a principal to a local user.
     */
    @isString
    public static final String STORM_PRINCIPAL_TO_LOCAL_PLUGIN = "storm.principal.tolocal";

    /**
     * The plugin that will provide user groups service
     */
    @isString
    public static final String STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN = "storm.group.mapping.service";

    /**
     * A list of credential renewers that nimbus should load.
     */
    @isStringList
    public static final String NIMBUS_CREDENTIAL_RENEWERS = "nimbus.credential.renewers.classes";

    /**
     * A list of plugins that nimbus should load during submit topology to populate
     * credentials on user's behalf.
     */
    @isStringList
    public static final String NIMBUS_AUTO_CRED_PLUGINS = "nimbus.autocredential.plugins.classes";

    /**
     * Class name of the HTTP credentials plugin for the UI.
     */
    @isString
    public static final String UI_HTTP_CREDS_PLUGIN = "ui.http.creds.plugin";

    /**
     * Class name of the HTTP credentials plugin for DRPC.
     */
    @isString
    public static final String DRPC_HTTP_CREDS_PLUGIN = "drpc.http.creds.plugin";

    /**
     * A list of users that run the supervisors and should be authorized to interact with
     * nimbus as a supervisor would.  To use this set
     * nimbus.authorizer to org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String NIMBUS_SUPERVISOR_USERS = "nimbus.supervisor.users";

    /**
     * A list of users that are cluster admins and can run any command.  To use this set
     * nimbus.authorizer to org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @isStringList
    public static final String NIMBUS_ADMINS = "nimbus.admins";

    /**
     *  For secure mode we would want to turn on this config
     *  By default this is turned off assuming the default is insecure
     */
    @isBoolean
    public static final String STORM_BLOBSTORE_ACL_VALIDATION_ENABLED = "storm.blobstore.acl.validation.enabled";

    /**
     * What buffer size to use for the blobstore uploads.
     */
    @isPositiveNumber
    @isInteger
    public static final String STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES = "storm.blobstore.inputstream.buffer.size.bytes";

    /**
     * FQCN of a class that implements {@code ISubmitterHook} @see ISubmitterHook for details.
     */
    @isString
    public static final String STORM_TOPOLOGY_SUBMISSION_NOTIFIER_PLUGIN = "storm.topology.submission.notifier.plugin.class";

    /**
     * Impersonation user ACL config entries.
     */
    @isMapEntryCustom(keyValidatorClasses = {ConfigValidation.StringValidator.class}, valueValidatorClasses = {ConfigValidation.ImpersonationAclUserEntryValidator.class})
    public static final String NIMBUS_IMPERSONATION_ACL = "nimbus.impersonation.acl";

    /**
     * A whitelist of the RAS scheduler strategies allowed by nimbus. Should be a list of fully-qualified class names
     * or null to allow all.
     */
    @isStringList
    public static final String NIMBUS_SCHEDULER_STRATEGY_CLASS_WHITELIST = "nimbus.scheduler.strategy.class.whitelist";

    /**
     * Full path to the worker-laucher executable that will be used to lauch workers when
     * SUPERVISOR_RUN_WORKER_AS_USER is set to true.
     */
    @isString
    public static final String SUPERVISOR_WORKER_LAUNCHER = "supervisor.worker.launcher";

    /**
     * Map a version of storm to a worker classpath that can be used to run it.
     * This allows the supervisor to select an available version of storm that is compatible with what a
     * topology was launched with.
     *
     * Only the major and minor version numbers are used, although this may change in the
     * future.  The code will first try to find a version that is the same or higher than the requested version,
     * but with the same major version number.  If it cannot it will fall back to using one with a lower
     * minor version, but in some cases this might fail as some features may be missing.
     * 
     * Because of how this selection process works please don't include two releases
     * with the same major and minor versions as it is undefined which will be selected.  Also it is good
     * practice to just include one release for each major version you want to support unless the
     * minor versions are truly not compatible with each other. This is to avoid
     * maintenance and testing overhead.
     *
     * This config needs to be set on all supervisors and on nimbus.  In general this can be the output of
     * calling storm classpath on the version you want and adding in an entry for the config directory for
     * that release.  You should modify the storm.yaml of each of these versions to match the features
     * and settings you want on the main version.
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP = "supervisor.worker.version.classpath.map";

    /**
     * Map a version of storm to a worker's main class.  In most cases storm should have correct defaults and
     * just setting SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP is enough.
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String SUPERVISOR_WORKER_VERSION_MAIN_MAP = "supervisor.worker.version.main.map";
    
    /**
     * Map a version of storm to a worker's logwriter class. In most cases storm should have correct defaults and
     * just setting SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP is enough.
     */
    @isMapEntryType(keyType = String.class, valueType = String.class)
    public static final String SUPERVISOR_WORKER_VERSION_LOGWRITER_MAP = "supervisor.worker.version.logwriter.map";

    /**
     * The version of storm to assume a topology should run as if not version is given by the client when
     * submitting the topology.
     */
    @isString
    public static final String SUPERVISOR_WORKER_DEFAULT_VERSION = "supervisor.worker.default.version";

    /**
     * A directory on the local filesystem used by Storm for any local
     * filesystem usage it needs. The directory must exist and the Storm daemons must
     * have permission to read/write from this location. It could be either absolute or relative.
     * If the setting is a relative directory, it is relative to root directory of Storm installation.
     */
    @isString
    public static final String STORM_LOCAL_DIR = "storm.local.dir";

    /**
     * The workers-artifacts directory (where we place all workers' logs), can be either absolute or relative.
     * By default, ${storm.log.dir}/workers-artifacts is where worker logs go.
     * If the setting is a relative directory, it is relative to storm.log.dir.
     */
    @isString
    public static final String STORM_WORKERS_ARTIFACTS_DIR = "storm.workers.artifacts.dir";

    /**
     * A list of hosts of Exhibitor servers used to discover/maintain connection to ZooKeeper cluster.
     * Any configured ZooKeeper servers will be used for the curator/exhibitor backup connection string.
     */
    @isStringList
    public static final String STORM_EXHIBITOR_SERVERS = "storm.exhibitor.servers";

    /**
     * The port Storm will use to connect to each of the exhibitor servers.
     */
    @isInteger
    @isPositiveNumber
    public static final String STORM_EXHIBITOR_PORT = "storm.exhibitor.port";

    /*
 * How often to poll Exhibitor cluster in millis.
 */
    @isString
    public static final String STORM_EXHIBITOR_URIPATH="storm.exhibitor.poll.uripath";

    /**
     * How often to poll Exhibitor cluster in millis.
     */
    @isInteger
    public static final String STORM_EXHIBITOR_POLL="storm.exhibitor.poll.millis";

    /**
     * The number of times to retry an Exhibitor operation.
     */
    @isInteger
    public static final String STORM_EXHIBITOR_RETRY_TIMES="storm.exhibitor.retry.times";

    /**
     * The interval between retries of an Exhibitor operation.
     */
    @isInteger
    public static final String STORM_EXHIBITOR_RETRY_INTERVAL="storm.exhibitor.retry.interval";

    /**
     * The ceiling of the interval between retries of an Exhibitor operation.
     */
    @isInteger
    public static final String STORM_EXHIBITOR_RETRY_INTERVAL_CEILING="storm.exhibitor.retry.intervalceiling.millis";

    /**
     * The connection timeout for clients to ZooKeeper.
     */
    @isInteger
    public static final String STORM_ZOOKEEPER_CONNECTION_TIMEOUT = "storm.zookeeper.connection.timeout";

    /**
     * The session timeout for clients to ZooKeeper.
     */
    @isInteger
    public static final String STORM_ZOOKEEPER_SESSION_TIMEOUT = "storm.zookeeper.session.timeout";

    /**
     * The interval between retries of a Zookeeper operation.
     */
    @isInteger
    public static final String STORM_ZOOKEEPER_RETRY_INTERVAL="storm.zookeeper.retry.interval";

    /**
     * The ceiling of the interval between retries of a Zookeeper operation.
     */
    @isInteger
    public static final String STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING="storm.zookeeper.retry.intervalceiling.millis";

    /**
     * The number of times to retry a Zookeeper operation.
     */
    @isInteger
    public static final String STORM_ZOOKEEPER_RETRY_TIMES="storm.zookeeper.retry.times";

    /**
     * The ClusterState factory that worker will use to create a ClusterState
     * to store state in. Defaults to ZooKeeper.
     */
    @isString
    public static final String STORM_CLUSTER_STATE_STORE = "storm.cluster.state.store";

    /**
     * How often this worker should heartbeat to the supervisor.
     */
    @isInteger
    @isPositiveNumber
    public static final String WORKER_HEARTBEAT_FREQUENCY_SECS = "worker.heartbeat.frequency.secs";

    /**
     * How often a task should heartbeat its status to the master.
     */
    @isInteger
    @isPositiveNumber
    public static final String TASK_HEARTBEAT_FREQUENCY_SECS = "task.heartbeat.frequency.secs";

    /**
     * How often a task should sync its connections with other tasks (if a task is
     * reassigned, the other tasks sending messages to it need to refresh their connections).
     * In general though, when a reassignment happens other tasks will be notified
     * almost immediately. This configuration is here just in case that notification doesn't
     * come through.
     */
    @isInteger
    @isPositiveNumber
    public static final String TASK_REFRESH_POLL_SECS = "task.refresh.poll.secs";

    /**
     * The Access Control List for the DRPC Authorizer.
     * @see org.apache.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer
     */
    @isType(type=Map.class)
    public static final String DRPC_AUTHORIZER_ACL = "drpc.authorizer.acl";

    /**
     * File name of the DRPC Authorizer ACL.
     * @see org.apache.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer
     */
    @isString
    public static final String DRPC_AUTHORIZER_ACL_FILENAME = "drpc.authorizer.acl.filename";

    /**
     * Whether the DRPCSimpleAclAuthorizer should deny requests for operations
     * involving functions that have no explicit ACL entry. When set to false
     * (the default) DRPC functions that have no entry in the ACL will be
     * permitted, which is appropriate for a development environment. When set
     * to true, explicit ACL entries are required for every DRPC function, and
     * any request for functions will be denied.
     * @see org.apache.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer
     */
    @isBoolean
    public static final String DRPC_AUTHORIZER_ACL_STRICT = "drpc.authorizer.acl.strict";

    /**
     * root directory of the storm cgroup hierarchy
     */
    @isString
    public static final String STORM_CGROUP_HIERARCHY_DIR = "storm.cgroup.hierarchy.dir";

    /**
     * The number of Buckets
     */
    @isInteger
    @isPositiveNumber
    public static final String NUM_STAT_BUCKETS = "num.stat.buckets";

    /**
     * Interval to check for the worker to check for updated blobs and refresh worker state accordingly.
     * The default is 10 seconds
     */
    @isInteger
    @isPositiveNumber
    public static final String WORKER_BLOB_UPDATE_POLL_INTERVAL_SECS = "worker.blob.update.poll.interval.secs";

    public static void setClasspath(Map<String, Object> conf, String cp) {
        conf.put(Config.TOPOLOGY_CLASSPATH, cp);
    }

    public void setClasspath(String cp) {
        setClasspath(this, cp);
    }

    public static void setEnvironment(Map<String, Object> conf, Map env) {
        conf.put(Config.TOPOLOGY_ENVIRONMENT, env);
    }

    public void setEnvironment(Map env) {
        setEnvironment(this, env);
    }

    public static void setDebug(Map<String, Object> conf, boolean isOn) {
        conf.put(Config.TOPOLOGY_DEBUG, isOn);
    }

    public void setDebug(boolean isOn) {
        setDebug(this, isOn);
    }

    public static void setTopologyVersion(Map<String, Object> conf, String version) {
        conf.put(Config.TOPOLOGY_VERSION, version);
    }

    public void setTopologyVersion(String version) {
        setTopologyVersion(this, version);
    }

    public static void setNumWorkers(Map<String, Object> conf, int workers) {
        conf.put(Config.TOPOLOGY_WORKERS, workers);
    }

    public void setNumWorkers(int workers) {
        setNumWorkers(this, workers);
    }

    public static void setNumAckers(Map<String, Object> conf, int numExecutors) {
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, numExecutors);
    }

    public void setNumAckers(int numExecutors) {
        setNumAckers(this, numExecutors);
    }

    public static void setNumEventLoggers(Map<String, Object> conf, int numExecutors) {
        conf.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, numExecutors);
    }

    public void setNumEventLoggers(int numExecutors) {
        setNumEventLoggers(this, numExecutors);
    }


    public static void setMessageTimeoutSecs(Map<String, Object> conf, int secs) {
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, secs);
    }

    public void setMessageTimeoutSecs(int secs) {
        setMessageTimeoutSecs(this, secs);
    }

    public static void registerSerialization(Map<String, Object> conf, Class klass) {
        getRegisteredSerializations(conf).add(klass.getName());
    }

    public void registerSerialization(Class klass) {
        registerSerialization(this, klass);
    }

    public static void registerSerialization(Map<String, Object> conf, Class klass, Class<? extends Serializer> serializerClass) {
        Map<String, String> register = new HashMap<String, String>();
        register.put(klass.getName(), serializerClass.getName());
        getRegisteredSerializations(conf).add(register);
    }

    public void registerSerialization(Class klass, Class<? extends Serializer> serializerClass) {
        registerSerialization(this, klass, serializerClass);
    }

    public static void registerMetricsConsumer(Map<String, Object> conf, Class klass, Object argument, long parallelismHint) {
        HashMap m = new HashMap();
        m.put("class", klass.getCanonicalName());
        m.put("parallelism.hint", parallelismHint);
        m.put("argument", argument);

        List l = (List)conf.get(TOPOLOGY_METRICS_CONSUMER_REGISTER);
        if (l == null) { l = new ArrayList(); }
        l.add(m);
        conf.put(TOPOLOGY_METRICS_CONSUMER_REGISTER, l);
    }

    public void registerMetricsConsumer(Class klass, Object argument, long parallelismHint) {
        registerMetricsConsumer(this, klass, argument, parallelismHint);
    }

    public static void registerMetricsConsumer(Map<String, Object> conf, Class klass, long parallelismHint) {
        registerMetricsConsumer(conf, klass, null, parallelismHint);
    }

    public void registerMetricsConsumer(Class klass, long parallelismHint) {
        registerMetricsConsumer(this, klass, parallelismHint);
    }

    public static void registerMetricsConsumer(Map<String, Object> conf, Class klass) {
        registerMetricsConsumer(conf, klass, null, 1L);
    }

    public void registerMetricsConsumer(Class klass) {
        registerMetricsConsumer(this, klass);
    }

    public static void registerDecorator(Map<String, Object> conf, Class<? extends IKryoDecorator> klass) {
        getRegisteredDecorators(conf).add(klass.getName());
    }

    public void registerDecorator(Class<? extends IKryoDecorator> klass) {
        registerDecorator(this, klass);
    }

    public static void setKryoFactory(Map<String, Object> conf, Class<? extends IKryoFactory> klass) {
        conf.put(Config.TOPOLOGY_KRYO_FACTORY, klass.getName());
    }

    public void setKryoFactory(Class<? extends IKryoFactory> klass) {
        setKryoFactory(this, klass);
    }

    public static void setSkipMissingKryoRegistrations(Map<String, Object> conf, boolean skip) {
        conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, skip);
    }

    public void setSkipMissingKryoRegistrations(boolean skip) {
        setSkipMissingKryoRegistrations(this, skip);
    }

    public static void setMaxTaskParallelism(Map<String, Object> conf, int max) {
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, max);
    }

    public void setMaxTaskParallelism(int max) {
        setMaxTaskParallelism(this, max);
    }

    public static void setMaxSpoutPending(Map<String, Object> conf, int max) {
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, max);
    }

    public void setMaxSpoutPending(int max) {
        setMaxSpoutPending(this, max);
    }

    public static void setStatsSampleRate(Map<String, Object> conf, double rate) {
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, rate);
    }

    public void setStatsSampleRate(double rate) {
        setStatsSampleRate(this, rate);
    }

    public static void setFallBackOnJavaSerialization(Map<String, Object> conf, boolean fallback) {
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, fallback);
    }

    public void setFallBackOnJavaSerialization(boolean fallback) {
        setFallBackOnJavaSerialization(this, fallback);
    }

    private static List getRegisteredSerializations(Map<String, Object> conf) {
        List ret;
        if(!conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
            ret = new ArrayList();
        } else {
            ret = new ArrayList((List) conf.get(Config.TOPOLOGY_KRYO_REGISTER));
        }
        conf.put(Config.TOPOLOGY_KRYO_REGISTER, ret);
        return ret;
    }

    private static List getRegisteredDecorators(Map<String, Object> conf) {
        List ret;
        if(!conf.containsKey(Config.TOPOLOGY_KRYO_DECORATORS)) {
            ret = new ArrayList();
        } else {
            ret = new ArrayList((List) conf.get(Config.TOPOLOGY_KRYO_DECORATORS));
        }
        conf.put(Config.TOPOLOGY_KRYO_DECORATORS, ret);
        return ret;
    }

    /**
     * set the max heap size allow per worker for this topology
     * @param size
     */
    public void setTopologyWorkerMaxHeapSize(Number size) {
        if(size != null) {
            this.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, size);
        }
    }

    /**
     * set the priority for a topology
     * @param priority
     */
    public void setTopologyPriority(int priority) {
        this.put(Config.TOPOLOGY_PRIORITY, priority);
    }

    public void setTopologyStrategy(String strategy) {
        this.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, strategy);
    }

}

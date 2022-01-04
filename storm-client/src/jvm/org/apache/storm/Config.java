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

import com.esotericsoftware.kryo.Serializer;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.metric.IEventLogger;
import org.apache.storm.policy.IWaitStrategy;
import org.apache.storm.serialization.IKryoDecorator;
import org.apache.storm.serialization.IKryoFactory;
import org.apache.storm.utils.ShellLogHandler;
import org.apache.storm.utils.Utils;
import org.apache.storm.validation.ConfigValidation;
import org.apache.storm.validation.ConfigValidation.EventLoggerRegistryValidator;
import org.apache.storm.validation.ConfigValidation.ListOfListOfStringValidator;
import org.apache.storm.validation.ConfigValidation.MapOfStringToMapOfStringToObjectValidator;
import org.apache.storm.validation.ConfigValidation.MetricRegistryValidator;
import org.apache.storm.validation.ConfigValidation.MetricReportersValidator;
import org.apache.storm.validation.ConfigValidation.RasConstraintsTypeValidator;
import org.apache.storm.validation.ConfigValidationAnnotations;
import org.apache.storm.validation.ConfigValidationAnnotations.CustomValidator;
import org.apache.storm.validation.ConfigValidationAnnotations.IsBoolean;
import org.apache.storm.validation.ConfigValidationAnnotations.IsExactlyOneOf;
import org.apache.storm.validation.ConfigValidationAnnotations.IsImplementationOfClass;
import org.apache.storm.validation.ConfigValidationAnnotations.IsInteger;
import org.apache.storm.validation.ConfigValidationAnnotations.IsKryoReg;
import org.apache.storm.validation.ConfigValidationAnnotations.IsListEntryCustom;
import org.apache.storm.validation.ConfigValidationAnnotations.IsMapEntryCustom;
import org.apache.storm.validation.ConfigValidationAnnotations.IsMapEntryType;
import org.apache.storm.validation.ConfigValidationAnnotations.IsNumber;
import org.apache.storm.validation.ConfigValidationAnnotations.IsPositiveNumber;
import org.apache.storm.validation.ConfigValidationAnnotations.IsString;
import org.apache.storm.validation.ConfigValidationAnnotations.IsStringList;
import org.apache.storm.validation.ConfigValidationAnnotations.IsStringOrStringList;
import org.apache.storm.validation.ConfigValidationAnnotations.IsType;
import org.apache.storm.validation.ConfigValidationAnnotations.NotNull;
import org.apache.storm.validation.ConfigValidationAnnotations.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Topology configs are specified as a plain old map. This class provides a convenient way to create a topology config map by providing
 * setter methods for all the configs that can be set. It also makes it easier to do things like add serializations.
 *
 * <p>This class also provides constants for all the configurations possible on a Storm cluster and Storm topology. Each constant is paired
 * with an annotation that defines the validity criterion of the corresponding field. Default values for these configs can be found in
 * defaults.yaml.
 *
 * <p>Note that you may put other configurations in any of the configs. Storm will ignore anything it doesn't recognize, but your topologies
 * are free to make use of them by reading them in the prepare method of Bolts or the open method of Spouts.
 */
public class Config extends HashMap<String, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    /**
     * The serializer class for ListDelegate (tuple payload). The default serializer will be ListDelegateSerializer
     */
    @IsString
    public static final String TOPOLOGY_TUPLE_SERIALIZER = "topology.tuple.serializer";
    /**
     * Disable load aware grouping support.
     */
    @IsBoolean
    @NotNull
    public static final String TOPOLOGY_DISABLE_LOADAWARE_MESSAGING = "topology.disable.loadaware.messaging";
    /**
     * This signifies the load congestion among target tasks in scope. Currently it's only used in LoadAwareShuffleGrouping. When the
     * average load is higher than the higher bound, the executor should choose target tasks in a higher scope, The scopes and their orders
     * are: EVERYTHING > RACK_LOCAL > HOST_LOCAL > WORKER_LOCAL
     */
    @IsPositiveNumber
    @NotNull
    public static final String TOPOLOGY_LOCALITYAWARE_HIGHER_BOUND = "topology.localityaware.higher.bound";
    /**
     * This signifies the load congestion among target tasks in scope. Currently it's only used in LoadAwareShuffleGrouping. When the
     * average load is lower than the lower bound, the executor should choose target tasks in a lower scope. The scopes and their orders
     * are: EVERYTHING > RACK_LOCAL > HOST_LOCAL > WORKER_LOCAL
     */
    @IsPositiveNumber
    @NotNull
    public static final String TOPOLOGY_LOCALITYAWARE_LOWER_BOUND = "topology.localityaware.lower.bound";
    /**
     * Try to serialize all tuples, even for local transfers.  This should only be used for testing, as a sanity check that all of your
     * tuples are setup properly.
     */
    @IsBoolean
    public static final String TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE = "topology.testing.always.try.serialize";
    /**
     * A map with blobstore keys mapped to each filename the worker will have access to in the launch directory to the blob by local file
     * name, uncompress flag, and if the worker should restart when the blob is updated. localname, workerRestart, and uncompress are
     * optional. If localname is not specified the name of the key is used instead. Each topologywill have different map of blobs.  Example:
     * topology.blobstore.map: {"blobstorekey" : {"localname": "myblob", "uncompress": false}, "blobstorearchivekey" : {"localname":
     * "myarchive", "uncompress": true, "workerRestart": true}}
     */
    @CustomValidator(validatorClass = MapOfStringToMapOfStringToObjectValidator.class)
    public static final String TOPOLOGY_BLOBSTORE_MAP = "topology.blobstore.map";
    /**
     * How often a worker should check dynamic log level timeouts for expiration. For expired logger settings, the clean up polling task
     * will reset the log levels to the original levels (detected at startup), and will clean up the timeout map
     */
    @IsInteger
    @IsPositiveNumber
    public static final String WORKER_LOG_LEVEL_RESET_POLL_SECS = "worker.log.level.reset.poll.secs";
    /**
     * How often a task should sync credentials, worst case.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TASK_CREDENTIALS_POLL_SECS = "task.credentials.poll.secs";
    /**
     * Whether to enable backpressure in for a certain topology.
     *
     * @deprecated: In Storm 2.0. Retained for enabling transition from 1.x. Will be removed soon.
     */
    @Deprecated
    @IsBoolean
    public static final String TOPOLOGY_BACKPRESSURE_ENABLE = "topology.backpressure.enable";
    /**
     * A list of users that are allowed to interact with the topology.  To use this set nimbus.authorizer to
     * org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @IsStringOrStringList
    public static final String TOPOLOGY_USERS = "topology.users";
    /**
     * A list of groups that are allowed to interact with the topology.  To use this set nimbus.authorizer to
     * org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @IsStringOrStringList
    public static final String TOPOLOGY_GROUPS = "topology.groups";
    /**
     * A list of readonly users that are allowed to interact with the topology.  To use this set nimbus.authorizer to
     * org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @IsStringOrStringList
    public static final String TOPOLOGY_READONLY_USERS = "topology.readonly.users";
    /**
     * A list of readonly groups that are allowed to interact with the topology.  To use this set nimbus.authorizer to
     * org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @IsStringOrStringList
    public static final String TOPOLOGY_READONLY_GROUPS = "topology.readonly.groups";
    /**
     * True if Storm should timeout messages or not. Defaults to true. This is meant to be used in unit tests to prevent tuples from being
     * accidentally timed out during the test.
     */
    @IsBoolean
    public static final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";
    /**
     * When set to true, Storm will log every message that's emitted.
     */
    @IsBoolean
    public static final String TOPOLOGY_DEBUG = "topology.debug";
    /**
     * User defined version of this topology.
     */
    @IsString
    public static final String TOPOLOGY_VERSION = "topology.version";
    /**
     * The fully qualified name of a {@link ShellLogHandler} to handle output from non-JVM processes e.g.
     * "com.mycompany.CustomShellLogHandler". If not provided, org.apache.storm.utils.DefaultLogHandler will be used.
     */
    @IsString
    public static final String TOPOLOGY_MULTILANG_LOG_HANDLER = "topology.multilang.log.handler";
    /**
     * The serializer for communication between shell components and non-JVM processes.
     */
    @IsString
    public static final String TOPOLOGY_MULTILANG_SERIALIZER = "topology.multilang.serializer";
    /**
     * How many processes should be spawned around the cluster to execute this topology. Each process will execute some number of tasks as
     * threads within them. This parameter should be used in conjunction with the parallelism hints on each component in the topology to
     * tune the performance of a topology. The number of workers will be dynamically calculated when the Resource Aware scheduler is used,
     * in which case this parameter will not be honored.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_WORKERS = "topology.workers";
    /**
     * How many instances to create for a spout/bolt. A task runs on a thread with zero or more other tasks for the same spout/bolt. The
     * number of tasks for a spout/bolt is always the same throughout the lifetime of a topology, but the number of executors (threads) for
     * a spout/bolt can change over time. This allows a topology to scale to more or less resources without redeploying the topology or
     * violating the constraints of Storm (such as a fields grouping guaranteeing that the same value goes to the same task).
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_TASKS = "topology.tasks";
    /**
     * A map of resources used by each component e.g {"cpu.pcore.percent" : 200.0. "onheap.memory.mb": 256.0, "gpu.count" : 2 }
     */
    @IsMapEntryType(keyType = String.class, valueType = Number.class)
    public static final String TOPOLOGY_COMPONENT_RESOURCES_MAP = "topology.component.resources.map";
    /**
     * The maximum amount of memory an instance of a spout/bolt will take on heap. This enables the scheduler to allocate slots on machines
     * with enough available memory. A default value will be set for this config if user does not override
     */
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB = "topology.component.resources.onheap.memory.mb";
    /**
     * The maximum amount of memory an instance of a spout/bolt will take off heap. This enables the scheduler to allocate slots on machines
     * with enough available memory.  A default value will be set for this config if user does not override
     */
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB = "topology.component.resources.offheap.memory.mb";
    /**
     * The config indicates the percentage of cpu for a core an instance(executor) of a component will use. Assuming the a core value to be
     * 100, a value of 10 indicates 10% of the core. The P in PCORE represents the term "physical".  A default value will be set for this
     * config if user does not override
     */
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT = "topology.component.cpu.pcore.percent";
    /**
     * The maximum amount of memory an instance of an acker will take on heap. This enables the scheduler to allocate slots on machines with
     * enough available memory.  A default value will be set for this config if user does not override
     */
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_ACKER_RESOURCES_ONHEAP_MEMORY_MB = "topology.acker.resources.onheap.memory.mb";
    /**
     * The maximum amount of memory an instance of an acker will take off heap. This enables the scheduler to allocate slots on machines
     * with enough available memory.  A default value will be set for this config if user does not override
     */
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_ACKER_RESOURCES_OFFHEAP_MEMORY_MB = "topology.acker.resources.offheap.memory.mb";
    /**
     * The config indicates the percentage of cpu for a core an instance(executor) of an acker will use. Assuming the a core value to be
     * 100, a value of 10 indicates 10% of the core. The P in PCORE represents the term "physical".  A default value will be set for this
     * config if user does not override
     */
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_ACKER_CPU_PCORE_PERCENT = "topology.acker.cpu.pcore.percent";
    /**
     * The maximum amount of memory an instance of a metrics consumer will take on heap. This enables the scheduler to allocate slots on
     * machines with enough available memory.  A default value will be set for this config if user does not override
     */
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_METRICS_CONSUMER_RESOURCES_ONHEAP_MEMORY_MB =
        "topology.metrics.consumer.resources.onheap.memory.mb";
    /**
     * The maximum amount of memory an instance of a metrics consumer will take off heap. This enables the scheduler to allocate slots on
     * machines with enough available memory.  A default value will be set for this config if user does not override
     */
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_METRICS_CONSUMER_RESOURCES_OFFHEAP_MEMORY_MB =
        "topology.metrics.consumer.resources.offheap.memory.mb";
    /**
     * The config indicates the percentage of cpu for a core an instance(executor) of a metrics consumer will use. Assuming the a core value
     * to be 100, a value of 10 indicates 10% of the core. The P in PCORE represents the term "physical".  A default value will be set for
     * this config if user does not override
     */
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_METRICS_CONSUMER_CPU_PCORE_PERCENT = "topology.metrics.consumer.cpu.pcore.percent";

    /**
     * This config allows a topology to report metrics data points from the V2 metrics API through the metrics tick.
     */
    @IsBoolean
    public static final String TOPOLOGY_ENABLE_V2_METRICS_TICK = "topology.enable.v2.metrics.tick";

    /**
     * Topology configuration to specify the V2 metrics tick interval in seconds.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_V2_METRICS_TICK_INTERVAL_SECONDS = "topology.v2.metrics.tick.interval.seconds";

    /**
     * This config allows a topology to enable/disable reporting of __send-iconnection metrics.
     */
    @IsBoolean
    public static final String TOPOLOGY_ENABLE_SEND_ICONNECTION_METRICS = "topology.enable.send.iconnection.metrics";

    /**
     * The class name of the {@link org.apache.storm.state.StateProvider} implementation. If not specified defaults to {@link
     * org.apache.storm.state.InMemoryKeyValueStateProvider}. This can be overridden at the component level.
     */
    @IsString
    public static final String TOPOLOGY_STATE_PROVIDER = "topology.state.provider";
    /**
     * The configuration specific to the {@link org.apache.storm.state.StateProvider} implementation. This can be overridden at the
     * component level. The value and the interpretation of this config is based on the state provider implementation. For e.g. this could
     * be just a config file name which contains the config for the state provider implementation.
     */
    @IsString
    public static final String TOPOLOGY_STATE_PROVIDER_CONFIG = "topology.state.provider.config";
    /**
     * Topology configuration to specify the checkpoint interval (in millis) at which the topology state is saved when {@link
     * org.apache.storm.topology.IStatefulBolt} bolts are involved.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_STATE_CHECKPOINT_INTERVAL = "topology.state.checkpoint.interval.ms";
    /**
     * A per topology config that specifies the maximum amount of memory a worker can use for that specific topology.
     */
    @IsPositiveNumber
    public static final String TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB = "topology.worker.max.heap.size.mb";
    /**
     * The strategy to use when scheduling a topology with Resource Aware Scheduler.
     */
    @NotNull
    @IsString
    //NOTE: @IsImplementationOfClass(implementsClass = IStrategy.class) is enforced in DaemonConf, so
    // an error will be thrown by nimbus on topology submission and not by the client prior to submitting
    // the topology.
    public static final String TOPOLOGY_SCHEDULER_STRATEGY = "topology.scheduler.strategy";

    /**
     * If set to true, unassigned executors will be sorted by topological order with network proximity needs before being scheduled.
     * This is a best-effort to split the topology to slices and allocate executors in each slice to closest physical location as possible.
     */
    public static final String TOPOLOGY_RAS_ORDER_EXECUTORS_BY_PROXIMITY_NEEDS = "topology.ras.order.executors.by.proximity.needs";

    /**
     * Declare scheduling constraints for a topology used by the constraint solver strategy. The format can be either
     * old style (validated by ListOfListOfStringValidator.class or the newer style, which is a list of specific type of
     * Maps (validated by RasConstraintsTypeValidator.class). The value must be in one or the other format.
     *
     * <p>
     * Old style Config.TOPOLOGY_RAS_CONSTRAINTS (ListOfListOfString) specified a list of components that cannot
     * co-exist on the same Worker.
     * </p>
     *
     * <p>
     * New style Config.TOPOLOGY_RAS_CONSTRAINTS is map where each component has a list of other incompatible components
     * (which serves the same function as the old style configuration) and optional number that specifies
     * the maximum co-location count for the component on a node.
     * </p>
     *
     * <p>comp-1 cannot exist on same worker as comp-2 or comp-3, and at most "2" comp-1 on same node</p>
     * <p>comp-2 and comp-4 cannot be on same worker (missing comp-1 is implied from comp-1 constraint)</p>
     *
     *  <p>
     *      { "comp-1": { "maxNodeCoLocationCnt": 2, "incompatibleComponents": ["comp-2", "comp-3" ] },
     *        "comp-2": { "incompatibleComponents": [ "comp-4" ] }
     *      }
     *  </p>
     */
    @IsExactlyOneOf(valueValidatorClasses = { ListOfListOfStringValidator.class, RasConstraintsTypeValidator.class })
    public static final String TOPOLOGY_RAS_CONSTRAINTS = "topology.ras.constraints";

    /**
     * Array of components that scheduler should try to place on separate hosts when using the constraint solver strategy or the
     * multi-tenant scheduler. Note that this configuration can be specified in TOPOLOGY_RAS_CONSTRAINTS using the
     * "maxNodeCoLocationCnt" map entry with value of 1.
     */
    @Deprecated
    @IsStringList
    public static final String TOPOLOGY_SPREAD_COMPONENTS = "topology.spread.components";
    /**
     * The maximum number of states that will be searched looking for a solution in resource aware strategies, e.g.
     * in BaseResourceAwareStrategy.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH = "topology.ras.constraint.max.state.search";
    /*
     * Whether to limit each worker to one executor. This is useful for debugging topologies to clearly identify workers that
     * are slow/crashing and for estimating resource requirements and capacity.
     * If both {@link #TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER} and {@link #TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER} are enabled,
     * {@link #TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER} is ignored.
     */
    @IsBoolean
    public static final String TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER = "topology.ras.one.executor.per.worker";
    /**
     * Whether to limit each worker to one component. This is useful for debugging topologies to clearly identify workers that
     * are slow/crashing and for estimating resource requirements and capacity.
     * If both TOPOLOGY_RAS_ONE_EXECUTOR_PER_WORKER and TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER are enabled,
     * TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER is ignored.
     */
    @IsBoolean
    public static final String TOPOLOGY_RAS_ONE_COMPONENT_PER_WORKER = "topology.ras.one.component.per.worker";
    /**
     * The maximum number of seconds to spend scheduling a topology using resource aware strategies, e.g.
     * in BaseResourceAwareStrategy. Null means no limit.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_RAS_CONSTRAINT_MAX_TIME_SECS = "topology.ras.constraint.max.time.secs";
    /**
     * A list of host names that this topology would prefer to be scheduled on (no guarantee is given though). This is intended for
     * debugging only.
     *
     * <p>Favored nodes are moved to the front of the node selection list.
     * If the same node is also present in {@link #TOPOLOGY_SCHEDULER_UNFAVORED_NODES}
     * then the node is considered only as a favored node and is removed from the unfavored list.
     * </p>
     */
    @IsStringList
    public static final String TOPOLOGY_SCHEDULER_FAVORED_NODES = "topology.scheduler.favored.nodes";
    /**
     * A list of host names that this topology would prefer to NOT be scheduled on (no guarantee is given though). This is intended for
     * debugging only.
     *
     * <p>Unfavored nodes are moved to the end of the node selection list.
     * If the same node is also present in {@link #TOPOLOGY_SCHEDULER_FAVORED_NODES}
     * then the node is considered only as a favored node and is removed from the unfavored list.
     * </p>
     */
    @IsStringList
    public static final String TOPOLOGY_SCHEDULER_UNFAVORED_NODES = "topology.scheduler.unfavored.nodes";
    /**
     * How many executors to spawn for ackers.
     *
     * <p>
     * 1. If not setting this variable or setting it as null,
     *   a. If RAS is not used:
     *        Nimbus will set it to {@link Config#TOPOLOGY_WORKERS}.
     *   b. If RAS is used:
     *        Nimbus will set it to (the estimate number of workers *  {@link Config#TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER}).
     *        {@link Config#TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER} is default to be 1 if not set.
     * 2. If this variable is set to 0,
     *    then Storm will immediately ack tuples as soon as they come off the spout,
     *    effectively disabling reliability.
     * 3. If this variable is set to a positive integer,
     *    Storm will not honor {@link Config#TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER} setting.
     *    Instead, nimbus will set it as (this variable / estimate num of workers).
     * </p>
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_ACKER_EXECUTORS = "topology.acker.executors";

    /**
     * How many ackers to put in when launching a new worker until we run out of ackers.
     *
     * <p>
     * This setting is RAS specific.
     * If {@link Config#TOPOLOGY_ACKER_EXECUTORS} is not configured,
     * this setting will be used to calculate {@link Config#TOPOLOGY_ACKER_EXECUTORS}.
     *
     * If {@link Config#TOPOLOGY_ACKER_EXECUTORS} is configured,
     * nimbus will ignore this and set it as ({@link Config#TOPOLOGY_ACKER_EXECUTORS} / estimate num of workers).
     * </p>
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_RAS_ACKER_EXECUTORS_PER_WORKER = "topology.ras.acker.executors.per.worker";

    /**
     * A list of classes implementing IEventLogger (See storm.yaml.example for exact config format). Each listed class will be routed all
     * the events sampled from emitting tuples. If there's no class provided to the option, default event logger will be initialized and
     * used unless you disable event logger executor.
     *
     * <p>Note that EventLoggerBolt takes care of all the implementations of IEventLogger, hence registering many
     * implementations (especially they're implemented as 'blocking' manner) would slow down overall topology.
     */
    @IsListEntryCustom(entryValidatorClasses = { EventLoggerRegistryValidator.class })
    public static final String TOPOLOGY_EVENT_LOGGER_REGISTER = "topology.event.logger.register";
    /**
     * How many executors to spawn for event logger.
     *
     * <p>By setting it as null, Storm will set the number of eventlogger executors to be equal to the number of workers
     * configured for this topology (or the estimated number of workers if the Resource Aware Scheduler is used).
     * If this variable is set to 0, event logging will be disabled.</p>
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_EVENTLOGGER_EXECUTORS = "topology.eventlogger.executors";
    /**
     * The maximum amount of time given to the topology to fully process a message emitted by a spout. If the message is not acked within
     * this time frame, Storm will fail the message on the spout. Some spouts implementations will then replay the message at a later time.
     */
    @IsInteger
    @IsPositiveNumber
    @NotNull
    public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";
    /**
     * A list of serialization registrations for Kryo ( https://github.com/EsotericSoftware/kryo ), the underlying serialization framework
     * for Storm. A serialization can either be the name of a class (in which case Kryo will automatically create a serializer for the class
     * that saves all the object's fields), or an implementation of com.esotericsoftware.kryo.Serializer.
     *
     * <p>See Kryo's documentation for more information about writing custom serializers.
     */
    @IsKryoReg
    public static final String TOPOLOGY_KRYO_REGISTER = "topology.kryo.register";
    /**
     * A list of classes that customize storm's kryo instance during start-up. Each listed class name must implement IKryoDecorator. During
     * start-up the listed class is instantiated with 0 arguments, then its 'decorate' method is called with storm's kryo instance as the
     * only argument.
     */
    @IsStringList
    public static final String TOPOLOGY_KRYO_DECORATORS = "topology.kryo.decorators";
    /**
     * Class that specifies how to create a Kryo instance for serialization. Storm will then apply topology.kryo.register and
     * topology.kryo.decorators on top of this. The default implementation implements topology.fall.back.on.java.serialization and turns
     * references off.
     */
    @IsString
    public static final String TOPOLOGY_KRYO_FACTORY = "topology.kryo.factory";
    /**
     * Whether or not Storm should skip the loading of kryo registrations for which it does not know the class or have the serializer
     * implementation. Otherwise, the task will fail to load and will throw an error at runtime. The use case of this is if you want to
     * declare your serializations on the storm.yaml files on the cluster rather than every single time you submit a topology. Different
     * applications may use different serializations and so a single application may not have the code for the other serializers used by
     * other apps. By setting this config to true, Storm will ignore that it doesn't have those other serializations rather than throw an
     * error.
     */
    @IsBoolean
    public static final String TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS = "topology.skip.missing.kryo.registrations";
    /**
     * List of classes to register during state serialization.
     */
    @IsStringList
    public static final String TOPOLOGY_STATE_KRYO_REGISTER = "topology.state.kryo.register";
    /**
     * A list of classes implementing IMetricsConsumer (See storm.yaml.example for exact config format). Each listed class will be routed
     * all the metrics data generated by the storm metrics API. Each listed class maps 1:1 to a system bolt named __metrics_ClassName#N, and
     * it's parallelism is configurable.
     */

    @IsListEntryCustom(entryValidatorClasses = { MetricRegistryValidator.class })
    public static final String TOPOLOGY_METRICS_CONSUMER_REGISTER = "topology.metrics.consumer.register";
    /**
     * Enable tracking of network message byte counts per source-destination task. This is off by default as it creates tasks^2 metric
     * values, but is useful for debugging as it exposes data skew when tuple sizes are uneven.
     */
    @IsBoolean
    public static final String TOPOLOGY_SERIALIZED_MESSAGE_SIZE_METRICS = "topology.serialized.message.size.metrics";
    /**
     * A map of metric name to class name implementing IMetric that will be created once per worker JVM.
     */
    @IsMapEntryType(keyType = String.class, valueType = String.class)
    public static final String TOPOLOGY_WORKER_METRICS = "topology.worker.metrics";
    /**
     * A map of metric name to class name implementing IMetric that will be created once per worker JVM.
     */
    @IsMapEntryType(keyType = String.class, valueType = String.class)
    public static final String WORKER_METRICS = "worker.metrics";
    /**
     * The maximum parallelism allowed for a component in this topology. This configuration is typically used in testing to limit the number
     * of threads spawned in local mode.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_MAX_TASK_PARALLELISM = "topology.max.task.parallelism";
    /**
     * The maximum number of tuples that can be pending on a spout task at any given time. This config applies to individual tasks, not to
     * spouts or topologies as a whole.
     *
     * <p>A pending tuple is one that has been emitted from a spout but has not been acked or failed yet. Note that this
     * config parameter has no effect for unreliable spouts that don't tag their tuples with a message id.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_MAX_SPOUT_PENDING = "topology.max.spout.pending";
    /**
     * The amount of milliseconds the SleepEmptyEmitStrategy should sleep for.
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS = "topology.sleep.spout.wait.strategy.time.ms";
    /**
     * The maximum amount of time a component gives a source of state to synchronize before it requests synchronization again.
     */
    @IsInteger
    @IsPositiveNumber
    @NotNull
    public static final String TOPOLOGY_STATE_SYNCHRONIZATION_TIMEOUT_SECS = "topology.state.synchronization.timeout.secs";
    /**
     * The percentage of tuples to sample to produce stats for a task.
     */
    @IsPositiveNumber
    public static final String TOPOLOGY_STATS_SAMPLE_RATE = "topology.stats.sample.rate";
    /**
     * The time period that builtin metrics data in bucketed into.
     */
    @IsInteger
    public static final String TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS = "topology.builtin.metrics.bucket.size.secs";
    /**
     * Whether or not to use Java serialization in a topology. Default is set false for security reasons.
     */
    @IsBoolean
    public static final String TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION = "topology.fall.back.on.java.serialization";
    /**
     * Topology-specific options for the worker child process. This is used in addition to WORKER_CHILDOPTS.
     */
    @IsStringOrStringList
    public static final String TOPOLOGY_WORKER_CHILDOPTS = "topology.worker.childopts";
    /**
     * Topology-specific options GC for the worker child process. This overrides WORKER_GC_CHILDOPTS.
     */
    @IsStringOrStringList
    public static final String TOPOLOGY_WORKER_GC_CHILDOPTS = "topology.worker.gc.childopts";
    /**
     * Topology-specific options for the logwriter process of a worker.
     */
    @IsStringOrStringList
    public static final String TOPOLOGY_WORKER_LOGWRITER_CHILDOPTS = "topology.worker.logwriter.childopts";
    /**
     * Topology-specific classpath for the worker child process. This is combined to the usual classpath.
     */
    @IsStringOrStringList
    public static final String TOPOLOGY_CLASSPATH = "topology.classpath";
    /**
     * Topology-specific classpath for the worker child process. This will be *prepended* to the usual classpath, meaning it can override
     * the Storm classpath. This is for debugging purposes, and is disabled by default. To allow topologies to be submitted with user-first
     * classpaths, set the storm.topology.classpath.beginning.enabled config to true.
     */
    @IsStringOrStringList
    public static final String TOPOLOGY_CLASSPATH_BEGINNING = "topology.classpath.beginning";
    /**
     * Topology-specific environment variables for the worker child process. This is added to the existing environment (that of the
     * supervisor)
     */
    @IsMapEntryType(keyType = String.class, valueType = String.class)
    public static final String TOPOLOGY_ENVIRONMENT = "topology.environment";
    /*
     * Bolt-specific configuration for windowed bolts to specify the window length as a count of number of tuples
     * in the window.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT = "topology.bolts.window.length.count";
    /*
     * Bolt-specific configuration for windowed bolts to specify the window length in time duration.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS = "topology.bolts.window.length.duration.ms";
    /*
     * Bolt-specific configuration for windowed bolts to specify the sliding interval as a count of number of tuples.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT = "topology.bolts.window.sliding.interval.count";
    /*
     * Bolt-specific configuration for windowed bolts to specify the sliding interval in time duration.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS = "topology.bolts.window.sliding.interval.duration.ms";
    /**
     * Bolt-specific configuration for windowed bolts to specify the name of the stream on which late tuples are going to be emitted. This
     * configuration should only be used from the BaseWindowedBolt.withLateTupleStream builder method, and not as global parameter,
     * otherwise IllegalArgumentException is going to be thrown.
     */
    @IsString
    public static final String TOPOLOGY_BOLTS_LATE_TUPLE_STREAM = "topology.bolts.late.tuple.stream";
    /**
     * Bolt-specific configuration for windowed bolts to specify the maximum time lag of the tuple timestamp in milliseconds. It means that
     * the tuple timestamps cannot be out of order by more than this amount. This config will be effective only if {@link
     * org.apache.storm.windowing.TimestampExtractor} is specified.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS = "topology.bolts.tuple.timestamp.max.lag.ms";
    /*
     * Bolt-specific configuration for windowed bolts to specify the time interval for generating
     * watermark events. Watermark event tracks the progress of time when tuple timestamp is used.
     * This config is effective only if {@link org.apache.storm.windowing.TimestampExtractor} is specified.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS = "topology.bolts.watermark.event.interval.ms";
    /*
     * Bolt-specific configuration for windowed bolts to specify the name of the field in the tuple that holds
     * the message id. This is used to track the windowing boundaries and avoid re-evaluating the windows
     * during recovery of IStatefulWindowedBolt
     */
    @IsString
    public static final String TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME = "topology.bolts.message.id.field.name";
    /**
     * This config is available for TransactionalSpouts, and contains the id ( a String) for the transactional topology. This id is used to
     * store the state of the transactional topology in Zookeeper.
     */
    @IsString
    public static final String TOPOLOGY_TRANSACTIONAL_ID = "topology.transactional.id";
    /**
     * A list of task hooks that are automatically added to every spout and bolt in the topology. An example of when you'd do this is to add
     * a hook that integrates with your internal monitoring system. These hooks are instantiated using the zero-arg constructor.
     */
    @IsStringList
    public static final String TOPOLOGY_AUTO_TASK_HOOKS = "topology.auto.task.hooks";
    /**
     * The size of the receive queue for each executor.
     */
    @IsPositiveNumber
    @IsInteger
    public static final String TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE = "topology.executor.receive.buffer.size";
    /**
     * The size of the transfer queue for each worker.
     */
    @IsPositiveNumber
    @IsInteger
    public static final String TOPOLOGY_TRANSFER_BUFFER_SIZE = "topology.transfer.buffer.size";
    /**
     * The size of the transfer queue for each worker.
     */
    @IsPositiveNumber
    @IsInteger
    public static final String TOPOLOGY_TRANSFER_BATCH_SIZE = "topology.transfer.batch.size";
    /**
     * How often a tick tuple from the "__system" component and "__tick" stream should be sent to tasks. Meant to be used as a
     * component-specific configuration.
     */
    @IsInteger
    public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs";
    /**
     * The number of tuples to batch before sending to the destination executor.
     */
    @IsInteger
    @IsPositiveNumber
    @NotNull
    public static final String TOPOLOGY_PRODUCER_BATCH_SIZE = "topology.producer.batch.size";
    /**
     * If number of items in task's overflowQ exceeds this, new messages coming from other workers to this task will be dropped This
     * prevents OutOfMemoryException that can occur in rare scenarios in the presence of BackPressure. This affects only inter-worker
     * messages. Messages originating from within the same worker will not be dropped.
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    @NotNull
    public static final String TOPOLOGY_EXECUTOR_OVERFLOW_LIMIT = "topology.executor.overflow.limit";
    /**
     * How often a worker should check and notify upstream workers about its tasks that are no longer experiencing BP and able to receive
     * new messages.
     */
    @IsInteger
    @IsPositiveNumber
    @NotNull
    public static final String TOPOLOGY_BACKPRESSURE_CHECK_MILLIS = "topology.backpressure.check.millis";
    /**
     * How often to send flush tuple to the executors for flushing out batched events.
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    @NotNull
    public static final String TOPOLOGY_BATCH_FLUSH_INTERVAL_MILLIS = "topology.batch.flush.interval.millis";
    /**
     * The size of the shared thread pool for worker tasks to make use of. The thread pool can be accessed via the TopologyContext.
     */
    @IsInteger
    public static final String TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE = "topology.worker.shared.thread.pool.size";
    /**
     * The interval in seconds to use for determining whether to throttle error reported to Zookeeper. For example, an interval of 10
     * seconds with topology.max.error.report.per.interval set to 5 will only allow 5 errors to be reported to Zookeeper per task for every
     * 10 second interval of time.
     */
    @IsInteger
    public static final String TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS = "topology.error.throttle.interval.secs";
    /**
     * See doc for {@link #TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS}.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL = "topology.max.error.report.per.interval";
    /**
     * How often a batch can be emitted in a Trident topology.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS = "topology.trident.batch.emit.interval.millis";
    /**
     * Maximum number of tuples that can be stored inmemory cache in windowing operators for fast access without fetching them from store.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_TRIDENT_WINDOWING_INMEMORY_CACHE_LIMIT = "topology.trident.windowing.cache.tuple.limit";
    /**
     * The id assigned to a running topology. The id is the storm name with a unique nonce appended.
     */
    @IsString
    public static final String STORM_ID = "storm.id";
    /**
     * Name of the topology. This config is automatically set by Storm when the topology is submitted.
     */
    @IsString
    public static final String TOPOLOGY_NAME = "topology.name";
    /**
     * The principal who submitted a topology.
     */
    @IsString
    public static final String TOPOLOGY_SUBMITTER_PRINCIPAL = "topology.submitter.principal";
    /**
     * The local user name of the user who submitted a topology.
     */
    @IsString
    public static final String TOPOLOGY_SUBMITTER_USER = "topology.submitter.user";
    /**
     * A list of IAutoCredentials that the topology should load and use.
     */
    @IsStringList
    public static final String TOPOLOGY_AUTO_CREDENTIALS = "topology.auto-credentials";
    /**
     * Max pending tuples in one ShellBolt.
     */
    @NotNull
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_SHELLBOLT_MAX_PENDING = "topology.shellbolt.max.pending";
    /**
     * How long a subprocess can go without heartbeating before the ShellSpout/ShellBolt tries to suicide itself.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_SUBPROCESS_TIMEOUT_SECS = "topology.subprocess.timeout.secs";
    /**
     * Topology central logging sensitivity to determine who has access to logs in central logging system. The possible values are: S0 -
     * Public (open to all users on grid) S1 - Restricted S2 - Confidential S3 - Secret (default.)
     */
    @IsString(acceptedValues = { "S0", "S1", "S2", "S3" })
    public static final String TOPOLOGY_LOGGING_SENSITIVITY = "topology.logging.sensitivity";
    /**
     * Log file the user can use to configure Log4j2.
     * Can be a resource in the jar (specified with classpath:/path/to/resource) or a file.
     * This configuration is applied in addition to the regular worker log4j2 configuration.
     * The configs are merged according to the rules here:
     *   https://logging.apache.org/log4j/2.x/manual/configuration.html#CompositeConfiguration
     */
    @IsString
    public static final String TOPOLOGY_LOGGING_CONFIG_FILE = "topology.logging.config";

    /**
     * Sets the priority for a topology.
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_PRIORITY = "topology.priority";
    /**
     * The root directory in ZooKeeper for metadata about TransactionalSpouts.
     */
    @IsString
    public static final String TRANSACTIONAL_ZOOKEEPER_ROOT = "transactional.zookeeper.root";
    /**
     * The list of zookeeper servers in which to keep the transactional state. If null (which is default), will use storm.zookeeper.servers
     */
    @IsStringList
    public static final String TRANSACTIONAL_ZOOKEEPER_SERVERS = "transactional.zookeeper.servers";
    /**
     * The port to use to connect to the transactional zookeeper servers. If null (which is default), will use storm.zookeeper.port
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TRANSACTIONAL_ZOOKEEPER_PORT = "transactional.zookeeper.port";
    /**
     * The user as which the nimbus client should be acquired to perform the operation.
     */
    @IsString
    public static final String STORM_DO_AS_USER = "storm.doAsUser";
    /**
     * The number of machines that should be used by this topology to isolate it from all others. Set storm.scheduler to
     * org.apache.storm.scheduler.multitenant.MultitenantScheduler
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TOPOLOGY_ISOLATED_MACHINES = "topology.isolate.machines";
    /**
     * A class that implements a wait strategy for spout. Waiting is triggered in one of two conditions:
     *
     * <p>1. nextTuple emits no tuples 2. The spout has hit maxSpoutPending and can't emit any more tuples
     *
     * <p>This class must implement {@link IWaitStrategy}.
     */
    @IsString
    public static final String TOPOLOGY_SPOUT_WAIT_STRATEGY = "topology.spout.wait.strategy";
    /**
     * Configures park time for WaitStrategyPark for spout.  If set to 0, returns immediately (i.e busy wait).
     */
    @NotNull
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_SPOUT_WAIT_PARK_MICROSEC = "topology.spout.wait.park.microsec";
    /**
     * Configures number of iterations to spend in level 1 of WaitStrategyProgressive, before progressing to level 2.
     */
    @NotNull
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_SPOUT_WAIT_PROGRESSIVE_LEVEL1_COUNT = "topology.spout.wait.progressive.level1.count";
    /**
     * Configures number of iterations to spend in level 2 of WaitStrategyProgressive, before progressing to level 3.
     */
    @NotNull
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_SPOUT_WAIT_PROGRESSIVE_LEVEL2_COUNT = "topology.spout.wait.progressive.level2.count";
    /**
     * Configures sleep time for WaitStrategyProgressive.
     */
    @NotNull
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_SPOUT_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS = "topology.spout.wait.progressive.level3.sleep.millis";
    /**
     * Selects the Bolt's Wait Strategy to use when there are no incoming msgs. Used to trade off latency vs CPU usage. This class must
     * implement {@link IWaitStrategy}.
     */
    @IsString
    public static final String TOPOLOGY_BOLT_WAIT_STRATEGY = "topology.bolt.wait.strategy";
    /**
     * Configures park time for WaitStrategyPark.  If set to 0, returns immediately (i.e busy wait).
     */
    @NotNull
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_BOLT_WAIT_PARK_MICROSEC = "topology.bolt.wait.park.microsec";
    /**
     * Configures number of iterations to spend in level 1 of WaitStrategyProgressive, before progressing to level 2.
     */
    @NotNull
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_BOLT_WAIT_PROGRESSIVE_LEVEL1_COUNT = "topology.bolt.wait.progressive.level1.count";
    /**
     * Configures number of iterations to spend in level 2 of WaitStrategyProgressive, before progressing to level 3.
     */
    @NotNull
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_BOLT_WAIT_PROGRESSIVE_LEVEL2_COUNT = "topology.bolt.wait.progressive.level2.count";
    /**
     * Configures sleep time for WaitStrategyProgressive.
     */
    @NotNull
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_BOLT_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS = "topology.bolt.wait.progressive.level3.sleep.millis";
    /**
     * A class that implements a wait strategy for an upstream component (spout/bolt) trying to write to a downstream component whose recv
     * queue is full
     *
     * <p>1. nextTuple emits no tuples 2. The spout has hit maxSpoutPending and can't emit any more tuples
     *
     * <p>This class must implement {@link IWaitStrategy}.
     */
    @IsString
    public static final String TOPOLOGY_BACKPRESSURE_WAIT_STRATEGY = "topology.backpressure.wait.strategy";
    /**
     * Configures park time if using WaitStrategyPark for BackPressure. If set to 0, returns immediately (i.e busy wait).
     */
    @NotNull
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_BACKPRESSURE_WAIT_PARK_MICROSEC = "topology.backpressure.wait.park.microsec";
    /**
     * Configures sleep time if using WaitStrategyProgressive for BackPressure.
     */
    @NotNull
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS =
        "topology.backpressure.wait.progressive.level3.sleep.millis";
    /**
     * Configures steps used to determine progression to the next level of wait .. if using WaitStrategyProgressive for BackPressure.
     */
    @NotNull
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL1_COUNT = "topology.backpressure.wait.progressive.level1.count";
    /**
     * Configures steps used to determine progression to the next level of wait .. if using WaitStrategyProgressive for BackPressure.
     */
    @NotNull
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL2_COUNT = "topology.backpressure.wait.progressive.level2.count";
    /**
     * Check recvQ after every N invocations of Spout's nextTuple() [when ACKing is disabled]. Spouts receive very few msgs if ACK is
     * disabled. This avoids checking the recvQ after each nextTuple().
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    @NotNull
    public static final String TOPOLOGY_SPOUT_RECVQ_SKIPS = "topology.spout.recvq.skips";
    /**
     * Minimum number of nimbus hosts where the code must be replicated before leader nimbus is allowed to perform topology activation tasks
     * like setting up heartbeats/assignments and marking the topology as active. default is 0.
     */
    @IsNumber
    public static final String TOPOLOGY_MIN_REPLICATION_COUNT = "topology.min.replication.count";
    /**
     * Maximum wait time for the nimbus host replication to achieve the nimbus.min.replication.count. Once this time is elapsed nimbus will
     * go ahead and perform topology activation tasks even if required nimbus.min.replication.count is not achieved. The default is 0
     * seconds, a value of -1 indicates to wait for ever.
     */
    @IsNumber
    public static final String TOPOLOGY_MAX_REPLICATION_WAIT_TIME_SEC = "topology.max.replication.wait.time.sec";
    /**
     * The list of servers that Pacemaker is running on.
     */
    @IsStringList
    public static final String PACEMAKER_SERVERS = "pacemaker.servers";
    /**
     * The port Pacemaker should run on. Clients should connect to this port to submit or read heartbeats.
     */
    @IsNumber
    @IsPositiveNumber
    public static final String PACEMAKER_PORT = "pacemaker.port";
    /**
     * The maximum number of threads that should be used by the Pacemaker client.
     * When Pacemaker gets loaded it will spawn new threads, up to
     * this many total, to handle the load.
     */
    @IsNumber
    @IsPositiveNumber
    public static final String PACEMAKER_CLIENT_MAX_THREADS = "pacemaker.client.max.threads";
    /**
     * This should be one of "DIGEST", "KERBEROS", or "NONE" Determines the mode of authentication the pacemaker server and client use. The
     * client must either match the server, or be NONE. In the case of NONE, no authentication is performed for the client, and if the
     * server is running with DIGEST or KERBEROS, the client can only write to the server (no reads). This is intended to provide a
     * primitive form of access-control.
     */
    @CustomValidator(validatorClass = ConfigValidation.PacemakerAuthTypeValidator.class)
    public static final String PACEMAKER_AUTH_METHOD = "pacemaker.auth.method";
    /**
     * Pacemaker Thrift Max Message Size (bytes).
     */
    @IsInteger
    @IsPositiveNumber
    public static final String PACEMAKER_THRIFT_MESSAGE_SIZE_MAX = "pacemaker.thrift.message.size.max";
    /**
     * Max no.of seconds group mapping service will cache user groups
     */
    @IsInteger
    public static final String STORM_GROUP_MAPPING_SERVICE_CACHE_DURATION_SECS = "storm.group.mapping.service.cache.duration.secs";
    /**
     * List of DRPC servers so that the DRPCSpout knows who to talk to.
     */
    @IsStringList
    public static final String DRPC_SERVERS = "drpc.servers";
    /**
     * This port on Storm DRPC is used by DRPC topologies to receive function invocations and send results back.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String DRPC_INVOCATIONS_PORT = "drpc.invocations.port";
    /**
     * The number of times to retry a Nimbus operation.
     */
    @IsNumber
    public static final String STORM_NIMBUS_RETRY_TIMES = "storm.nimbus.retry.times";
    /**
     * The starting interval between exponential backoff retries of a Nimbus operation.
     */
    @IsNumber
    public static final String STORM_NIMBUS_RETRY_INTERVAL = "storm.nimbus.retry.interval.millis";
    /**
     * The ceiling of the interval between retries of a client connect to Nimbus operation.
     */
    @IsNumber
    public static final String STORM_NIMBUS_RETRY_INTERVAL_CEILING = "storm.nimbus.retry.intervalceiling.millis";
    /**
     * The Nimbus transport plug-in for Thrift client/server communication.
     */
    @IsString
    public static final String NIMBUS_THRIFT_TRANSPORT_PLUGIN = "nimbus.thrift.transport";
    /**
     * Which port the Thrift interface of Nimbus should run on. Clients should connect to this port to upload jars and submit topologies.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String NIMBUS_THRIFT_PORT = "nimbus.thrift.port";
    /**
     * Nimbus thrift server queue size, default is 100000. This is the request queue size , when there are more requests than number of
     * threads to serve the requests, those requests will be queued to this queue. If the request queue size > this config, then the
     * incoming requests will be rejected.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String NIMBUS_QUEUE_SIZE = "nimbus.queue.size";
    /**
     * Nimbus assignments backend for storing local assignments. We will use it to store physical plan and runtime storm ids.
     */
    @IsString
    @ConfigValidationAnnotations.IsImplementationOfClass(implementsClass = org.apache.storm.assignments.ILocalAssignmentsBackend.class)
    public static final String NIMBUS_LOCAL_ASSIGNMENTS_BACKEND_CLASS = "nimbus.local.assignments.backend.class";
    /**
     * The number of threads that should be used by the nimbus thrift server.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String NIMBUS_THRIFT_THREADS = "nimbus.thrift.threads";
    /**
     * The maximum buffer size thrift should use when reading messages.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String NIMBUS_THRIFT_MAX_BUFFER_SIZE = "nimbus.thrift.max_buffer_size";
    /**
     * How long before a Thrift Client socket hangs before timeout and restart the socket.
     */
    @IsInteger
    public static final String STORM_THRIFT_SOCKET_TIMEOUT_MS = "storm.thrift.socket.timeout.ms";
    /**
     * The DRPC transport plug-in for Thrift client/server communication.
     */
    @IsString
    public static final String DRPC_THRIFT_TRANSPORT_PLUGIN = "drpc.thrift.transport";
    /**
     * This port is used by Storm DRPC for receiving DPRC requests from clients.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String DRPC_PORT = "drpc.port";
    /**
     * DRPC thrift server queue size.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String DRPC_QUEUE_SIZE = "drpc.queue.size";
    /**
     * DRPC thrift server worker threads.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String DRPC_WORKER_THREADS = "drpc.worker.threads";
    /**
     * The maximum buffer size thrift should use when reading messages for DRPC.
     */
    @IsNumber
    @IsPositiveNumber
    public static final String DRPC_MAX_BUFFER_SIZE = "drpc.max_buffer_size";
    /**
     * The DRPC invocations transport plug-in for Thrift client/server communication.
     */
    @IsString
    public static final String DRPC_INVOCATIONS_THRIFT_TRANSPORT_PLUGIN = "drpc.invocations.thrift.transport";
    /**
     * DRPC invocations thrift server worker threads.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String DRPC_INVOCATIONS_THREADS = "drpc.invocations.threads";
    /**
     * Initialization parameters for the group mapping service plugin. Provides a way for a
     * {@link #STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN} implementation to access optional settings.
     */
    @IsType(type = Map.class)
    public static final String STORM_GROUP_MAPPING_SERVICE_PARAMS = "storm.group.mapping.service.params";
    /**
     * The default transport plug-in for Thrift client/server communication.
     */
    @IsString
    public static final String STORM_THRIFT_TRANSPORT_PLUGIN = "storm.thrift.transport";
    /**
     * How long a worker can go without heartbeating before the supervisor tries to restart the worker process.
     * Can be overridden by {@link #TOPOLOGY_WORKER_TIMEOUT_SECS}, if set.
     */
    @IsInteger
    @IsPositiveNumber
    @NotNull
    public static final String SUPERVISOR_WORKER_TIMEOUT_SECS = "supervisor.worker.timeout.secs";
    /**
     * Enforce maximum on {@link #TOPOLOGY_WORKER_TIMEOUT_SECS}.
     */
    @IsInteger
    @IsPositiveNumber
    @NotNull
    public static final String WORKER_MAX_TIMEOUT_SECS = "worker.max.timeout.secs";
    /**
     * Topology configurable worker heartbeat timeout before the supervisor tries to restart the worker process.
     * Maximum value constrained by {@link #WORKER_MAX_TIMEOUT_SECS}.
     * When topology timeout is greater, the following configs are effectively overridden:
     * {@link #SUPERVISOR_WORKER_TIMEOUT_SECS}, SUPERVISOR_WORKER_START_TIMEOUT_SECS, NIMBUS_TASK_TIMEOUT_SECS and NIMBUS_TASK_LAUNCH_SECS.
     */
    @IsInteger
    @IsPositiveNumber
    @NotNull
    public static final String TOPOLOGY_WORKER_TIMEOUT_SECS = "topology.worker.timeout.secs";
    /**
     * How many seconds to allow for graceful worker shutdown when killing workers before resorting to force kill.
     * If a worker fails to shut down gracefully within this delay, it will either suicide or be forcibly killed by the supervisor.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String SUPERVISOR_WORKER_SHUTDOWN_SLEEP_SECS = "supervisor.worker.shutdown.sleep.secs";
    /**
     * A list of hosts of ZooKeeper servers used to manage the cluster.
     */
    @IsStringList
    public static final String STORM_ZOOKEEPER_SERVERS = "storm.zookeeper.servers";
    /**
     * The port Storm will use to connect to each of the ZooKeeper servers.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String STORM_ZOOKEEPER_PORT = "storm.zookeeper.port";
    /**
     * This is part of a temporary workaround to a ZK bug, it is the 'scheme:acl' for the user Nimbus and Supervisors use to authenticate
     * with ZK.
     */
    @IsString
    public static final String STORM_ZOOKEEPER_SUPERACL = "storm.zookeeper.superACL";
    /**
     * The ACL of the drpc user in zookeeper so the drpc servers can verify worker tokens.
     *
     * <p>Should be in the form 'scheme:acl' just like STORM_ZOOKEEPER_SUPERACL.
     */
    @IsString
    public static final String STORM_ZOOKEEPER_DRPC_ACL = "storm.zookeeper.drpcACL";
    /**
     * The topology Zookeeper authentication scheme to use, e.g. "digest". It is the internal config and user shouldn't set it.
     */
    @IsString
    public static final String STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME = "storm.zookeeper.topology.auth.scheme";
    /**
     * The delegate for serializing metadata, should be used for serialized objects stored in zookeeper and on disk. This is NOT used for
     * compressing serialized tuples sent between topologies.
     */
    @IsString
    public static final String STORM_META_SERIALIZATION_DELEGATE = "storm.meta.serialization.delegate";

    /**
     * Configure the topology metrics reporters to be used on workers.
     */
    @IsListEntryCustom(entryValidatorClasses = { MetricReportersValidator.class })
    public static final String TOPOLOGY_METRICS_REPORTERS = "topology.metrics.reporters";

    /**
     * A list of system metrics reporters that will get added to each topology.
     */
    @IsListEntryCustom(entryValidatorClasses = { MetricReportersValidator.class })
    public static final String STORM_TOPOLOGY_METRICS_SYSTEM_REPORTERS = "storm.topology.metrics.system.reporters";

    /**
     * Configure the topology metrics reporters to be used on workers.
     * @deprecated Use {@link Config#TOPOLOGY_METRICS_REPORTERS} instead.
     */
    @Deprecated
    @IsListEntryCustom(entryValidatorClasses = { MetricReportersValidator.class })
    public static final String STORM_METRICS_REPORTERS = "storm.metrics.reporters";

    /**
     * What blobstore implementation the storm client should use.
     */
    @IsString
    public static final String CLIENT_BLOBSTORE = "client.blobstore.class";

    /**
     * What directory to use for the blobstore. The directory is expected to be an absolute path when using HDFS blobstore, for
     * LocalFsBlobStore it could be either absolute or relative. If the setting is a relative directory, it is relative to root directory of
     * Storm installation.
     */
    @IsString
    public static final String BLOBSTORE_DIR = "blobstore.dir";
    /**
     * Enable the blobstore cleaner. Certain blobstores may only want to run the cleaner on one daemon. Currently Nimbus handles setting
     * this.
     */
    @IsBoolean
    public static final String BLOBSTORE_CLEANUP_ENABLE = "blobstore.cleanup.enable";
    /**
     * principal for nimbus/supervisor to use to access secure hdfs for the blobstore.
     * The format is generally "primary/instance@REALM", where "instance" field is optional.
     * If the instance field of the principal is the string "_HOST", it will
     * be replaced with the host name of the server the daemon is running on
     * (by calling {@link #getBlobstoreHDFSPrincipal(Map conf)} method).
     * @Deprecated Use {@link Config#STORM_HDFS_LOGIN_PRINCIPAL} instead.
     */
    @Deprecated
    @IsString
    public static final String BLOBSTORE_HDFS_PRINCIPAL = "blobstore.hdfs.principal";
    /**
     * keytab for nimbus/supervisor to use to access secure hdfs for the blobstore.
     * @Deprecated Use {@link Config#STORM_HDFS_LOGIN_KEYTAB} instead.
     */
    @Deprecated
    @IsString
    public static final String BLOBSTORE_HDFS_KEYTAB = "blobstore.hdfs.keytab";
    /**
     * Set replication factor for a blob in HDFS Blobstore Implementation.
     */
    @IsPositiveNumber
    @IsInteger
    public static final String STORM_BLOBSTORE_REPLICATION_FACTOR = "storm.blobstore.replication.factor";
    /**
     * The principal for nimbus/supervisor to use to access secure hdfs.
     * The format is generally "primary/instance@REALM", where "instance" field is optional.
     * If the instance field of the principal is the string "_HOST", it will
     * be replaced with the host name of the server the daemon is running on
     * (by calling {@link #getHdfsPrincipal} method).
     */
    @IsString
    public static final String STORM_HDFS_LOGIN_PRINCIPAL = "storm.hdfs.login.principal";

    /**
     * The keytab for nimbus/supervisor to use to access secure hdfs.
     */
    @IsString
    public static final String STORM_HDFS_LOGIN_KEYTAB = "storm.hdfs.login.keytab";
    /**
     * The hostname the supervisors/workers should report to nimbus. If unset, Storm will get the hostname to report by calling
     * <code>InetAddress.getLocalHost().getCanonicalHostName()</code>.
     *
     * <p>You should set this config when you don't have a DNS which supervisors/workers can utilize to find each other
     * based on hostname got from calls to
     * <code>InetAddress.getLocalHost().getCanonicalHostName()</code>.
     */
    @IsString
    public static final String STORM_LOCAL_HOSTNAME = "storm.local.hostname";
    /**
     * List of seed nimbus hosts to use for leader nimbus discovery.
     */
    @IsStringList
    public static final String NIMBUS_SEEDS = "nimbus.seeds";
    /**
     * A list of users that are the only ones allowed to run user operation on storm cluster. To use this set nimbus.authorizer to
     * org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @IsStringList
    public static final String NIMBUS_USERS = "nimbus.users";
    /**
     * A list of groups , users belong to these groups are the only ones allowed to run user operation on storm cluster. To use this set
     * nimbus.authorizer to org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @IsStringList
    public static final String NIMBUS_GROUPS = "nimbus.groups";
    /**
     * The mode this Storm cluster is running in. Either "distributed" or "local".
     */
    @IsString
    public static final String STORM_CLUSTER_MODE = "storm.cluster.mode";
    /**
     * The root location at which Storm stores data in ZooKeeper.
     */
    @IsString
    public static final String STORM_ZOOKEEPER_ROOT = "storm.zookeeper.root";
    /**
     * A string representing the payload for topology Zookeeper authentication. It gets serialized using UTF-8 encoding during
     * authentication.
     */
    @IsString
    @Password
    public static final String STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD = "storm.zookeeper.topology.auth.payload";
    /**
     * The cluster Zookeeper authentication scheme to use, e.g. "digest". Defaults to no authentication.
     */
    @IsString
    public static final String STORM_ZOOKEEPER_AUTH_SCHEME = "storm.zookeeper.auth.scheme";
    /**
     * A string representing the payload for cluster Zookeeper authentication. It gets serialized using UTF-8 encoding during
     * authentication. Note that if this is set to something with a secret (as when using digest authentication) then it should only be set
     * in the storm-cluster-auth.yaml file. This file storm-cluster-auth.yaml should then be protected with appropriate permissions that
     * deny access from workers.
     */
    @IsString
    public static final String STORM_ZOOKEEPER_AUTH_PAYLOAD = "storm.zookeeper.auth.payload";
    /**
     * What Network Topography detection classes should we use. Given a list of supervisor hostnames (or IP addresses), this class would
     * return a list of rack names that correspond to the supervisors. This information is stored in Cluster.java, and is used in the
     * resource aware scheduler.
     */
    @NotNull
    @IsImplementationOfClass(implementsClass = org.apache.storm.networktopography.DNSToSwitchMapping.class)
    public static final String STORM_NETWORK_TOPOGRAPHY_PLUGIN = "storm.network.topography.plugin";
    /**
     * The jvm opts provided to workers launched by this supervisor for GC. All "%ID%" substrings are replaced with an identifier for this
     * worker.  Because the JVM complains about multiple GC opts the topology can override this default value by setting
     * topology.worker.gc.childopts.
     */
    @IsStringOrStringList
    public static final String WORKER_GC_CHILDOPTS = "worker.gc.childopts";
    /**
     * The jvm opts provided to workers launched by this supervisor. All "%ID%", "%WORKER-ID%", "%TOPOLOGY-ID%", "%WORKER-PORT%" and
     * "%HEAP-MEM%" substrings are replaced with: %ID%          -> port (for backward compatibility), %WORKER-ID%   -> worker-id,
     * %TOPOLOGY-ID%    -> topology-id, %WORKER-PORT% -> port. %HEAP-MEM% -> mem-onheap.
     */
    @IsStringOrStringList
    public static final String WORKER_CHILDOPTS = "worker.childopts";
    /**
     * The default heap memory size in MB per worker, used in the jvm -Xmx opts for launching the worker.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String WORKER_HEAP_MEMORY_MB = "worker.heap.memory.mb";
    /**
     * The total amount of memory (in MiB) a supervisor is allowed to give to its workers. A default value will be set for this config if
     * user does not override
     */
    @IsPositiveNumber
    public static final String SUPERVISOR_MEMORY_CAPACITY_MB = "supervisor.memory.capacity.mb";
    /**
     * The total amount of CPU resources a supervisor is allowed to give to its workers. By convention 1 cpu core should be about 100, but
     * this can be adjusted if needed using 100 makes it simple to set the desired value to the capacity measurement for single threaded
     * bolts.  A default value will be set for this config if user does not override
     */
    @IsPositiveNumber
    public static final String SUPERVISOR_CPU_CAPACITY = "supervisor.cpu.capacity";
    @IsInteger
    @IsPositiveNumber
    /**
     * Port used for supervisor thrift server.
     */
    public static final String SUPERVISOR_THRIFT_PORT = "supervisor.thrift.port";
    @IsString
    /**
     * The Supervisor invocations transport plug-in for Thrift client/server communication.
     */
    public static final String SUPERVISOR_THRIFT_TRANSPORT_PLUGIN = "supervisor.thrift.transport";
    @IsInteger
    @IsPositiveNumber
    /**
     * Supervisor thrift server queue size.
     */
    public static final String SUPERVISOR_QUEUE_SIZE = "supervisor.queue.size";
    @IsInteger
    @IsPositiveNumber
    /**
     * The number of threads that should be used by the supervisor thrift server.
     */
    public static final String SUPERVISOR_THRIFT_THREADS = "supervisor.thrift.threads";
    @IsNumber
    @IsPositiveNumber
    public static final String SUPERVISOR_THRIFT_MAX_BUFFER_SIZE = "supervisor.thrift.max_buffer_size";
    /**
     * How long before a supervisor Thrift Client socket hangs before timeout and restart the socket.
     */
    @IsInteger
    public static final String SUPERVISOR_THRIFT_SOCKET_TIMEOUT_MS = "supervisor.thrift.socket.timeout.ms";
    /**
     * A map of resources the Supervisor has e.g {"cpu.pcore.percent" : 200.0. "onheap.memory.mb": 256.0, "gpu.count" : 2.0 }
     */
    @IsMapEntryType(keyType = String.class, valueType = Number.class)
    public static final String SUPERVISOR_RESOURCES_MAP = "supervisor.resources.map";
    /**
     * Whether or not to use ZeroMQ for messaging in local mode. If this is set to false, then Storm will use a pure-Java messaging system.
     * The purpose of this flag is to make it easy to run Storm in local mode by eliminating the need for native dependencies, which can be
     * difficult to install.
     *
     * <p>Defaults to false.
     */
    @IsBoolean
    public static final String STORM_LOCAL_MODE_ZMQ = "storm.local.mode.zmq";
    /**
     * The transporter for communication among Storm tasks.
     */
    @IsString
    public static final String STORM_MESSAGING_TRANSPORT = "storm.messaging.transport";
    /**
     * Netty based messaging: Is authentication required for Netty messaging from client worker process to server worker process.
     */
    @IsBoolean
    public static final String STORM_MESSAGING_NETTY_AUTHENTICATION = "storm.messaging.netty.authentication";
    /**
     * Netty based messaging: The buffer size for send/recv buffer.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String STORM_MESSAGING_NETTY_BUFFER_SIZE = "storm.messaging.netty.buffer_size";
    /**
     * Netty based messaging: The netty write buffer high watermark in bytes.
     * <p>
     * If the number of bytes queued in the netty's write buffer exceeds this value, the netty {@code Channel.isWritable()} will start to
     * return {@code false}. The client will wait until the value falls below the {@linkplain #STORM_MESSAGING_NETTY_BUFFER_LOW_WATERMARK
     * low water mark}.
     * </p>
     */
    @IsInteger
    @IsPositiveNumber
    public static final String STORM_MESSAGING_NETTY_BUFFER_HIGH_WATERMARK = "storm.messaging.netty.buffer.high.watermark";
    /**
     * Netty based messaging: The netty write buffer low watermark in bytes.
     * <p>
     * Once the number of bytes queued in the write buffer exceeded the {@linkplain #STORM_MESSAGING_NETTY_BUFFER_HIGH_WATERMARK high water
     * mark} and then dropped down below this value, the netty {@code Channel.isWritable()} will start to return true.
     * </p>
     */
    @IsInteger
    @IsPositiveNumber
    public static final String STORM_MESSAGING_NETTY_BUFFER_LOW_WATERMARK = "storm.messaging.netty.buffer.low.watermark";
    /**
     * Netty based messaging: Sets the backlog value to specify when the channel binds to a local address.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String STORM_MESSAGING_NETTY_SOCKET_BACKLOG = "storm.messaging.netty.socket.backlog";
    /**
     * Netty based messaging: The # of worker threads for the server.
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String STORM_MESSAGING_NETTY_SERVER_WORKER_THREADS = "storm.messaging.netty.server_worker_threads";
    /**
     * If the Netty messaging layer is busy, the Netty client will try to batch message as more as possible up to the size of
     * STORM_NETTY_MESSAGE_BATCH_SIZE bytes.
     */
    @IsInteger
    public static final String STORM_NETTY_MESSAGE_BATCH_SIZE = "storm.messaging.netty.transfer.batch.size";
    /**
     * Netty based messaging: The min # of milliseconds that a peer will wait.
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String STORM_MESSAGING_NETTY_MIN_SLEEP_MS = "storm.messaging.netty.min_wait_ms";
    /**
     * Netty based messaging: The max # of milliseconds that a peer will wait.
     */
    @IsInteger
    @IsPositiveNumber(includeZero = true)
    public static final String STORM_MESSAGING_NETTY_MAX_SLEEP_MS = "storm.messaging.netty.max_wait_ms";
    /**
     * Netty based messaging: The # of worker threads for the client.
     */
    @IsInteger
    public static final String STORM_MESSAGING_NETTY_CLIENT_WORKER_THREADS = "storm.messaging.netty.client_worker_threads";
    /**
     * Should the supervior try to run the worker as the lauching user or not.  Defaults to false.
     */
    @IsBoolean
    public static final String SUPERVISOR_RUN_WORKER_AS_USER = "supervisor.run.worker.as.user";
    /**
     * max timeout for supervisor reported heartbeats when master gains leadership.
     */
    @IsInteger
    public static final String SUPERVISOR_WORKER_HEARTBEATS_MAX_TIMEOUT_SECS = "supervisor.worker.heartbeats.max.timeout.secs";
    /**
     * On some systems (windows for example) symlinks require special privileges that not everyone wants to grant a headless user.  You can
     * completely disable the use of symlinks by setting this config to true, but by doing so you may also lose some features from storm.
     * For example the blobstore feature does not currently work without symlinks enabled.
     */
    @IsBoolean
    public static final String DISABLE_SYMLINKS = "storm.disable.symlinks";
    /**
     * The plugin that will convert a principal to a local user.
     */
    @IsString
    public static final String STORM_PRINCIPAL_TO_LOCAL_PLUGIN = "storm.principal.tolocal";
    /**
     * The plugin that will provide user groups service.
     */
    @IsString
    public static final String STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN = "storm.group.mapping.service";
    /**
     * A list of credential renewers that nimbus should load.
     */
    @IsStringList
    public static final String NIMBUS_CREDENTIAL_RENEWERS = "nimbus.credential.renewers.classes";
    /**
     * A list of plugins that nimbus should load during submit topology to populate credentials on user's behalf.
     */
    @IsStringList
    public static final String NIMBUS_AUTO_CRED_PLUGINS = "nimbus.autocredential.plugins.classes";

    /**
     * A list of users that run the supervisors and should be authorized to interact with nimbus as a supervisor would.  To use this set
     * nimbus.authorizer to org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer.
     */
    @IsStringList
    public static final String NIMBUS_SUPERVISOR_USERS = "nimbus.supervisor.users";
    /**
     * A list of users that nimbus runs as and should be authorized to interact with the supervisor as nimbus would. To use this set
     * supervisor.authorizer to org.apache.storm.security.auth.authorizer.SupervisorSimpleACLAuthorizer.
     */
    @IsStringList
    public static final String NIMBUS_DAEMON_USERS = "nimbus.daemon.users";
    /**
     * A list of users that are cluster admins and can run any command.  To use this set nimbus.authorizer to
     * org.apache.storm.security.auth.authorizer.SimpleACLAuthorizer
     */
    @IsStringList
    public static final String NIMBUS_ADMINS = "nimbus.admins";
    /**
     * A list of groups that are cluster admins and can run any command.
     */
    @IsStringList
    public static final String NIMBUS_ADMINS_GROUPS = "nimbus.admins.groups";
    /**
     * For secure mode we would want to turn on this config By default this is turned off assuming the default is insecure.
     */
    @IsBoolean
    public static final String STORM_BLOBSTORE_ACL_VALIDATION_ENABLED = "storm.blobstore.acl.validation.enabled";
    /**
     * What buffer size to use for the blobstore uploads.
     */
    @IsPositiveNumber
    @IsInteger
    public static final String STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES = "storm.blobstore.inputstream.buffer.size.bytes";
    /**
     * What chunk size to use for storm client to upload dependency jars.
     */
    @IsPositiveNumber
    @IsInteger
    public static final String STORM_BLOBSTORE_DEPENDENCY_JAR_UPLOAD_CHUNK_SIZE_BYTES =
            "storm.blobstore.dependency.jar.upload.chunk.size.bytes";
    /**
     * FQCN of a class that implements {@code ISubmitterHook} @see ISubmitterHook for details.
     */
    @IsString
    public static final String STORM_TOPOLOGY_SUBMISSION_NOTIFIER_PLUGIN = "storm.topology.submission.notifier.plugin.class";
    /**
     * Impersonation user ACL config entries.
     */
    @IsMapEntryCustom(keyValidatorClasses = { ConfigValidation.StringValidator.class },
        valueValidatorClasses = { ConfigValidation.ImpersonationAclUserEntryValidator.class })
    public static final String NIMBUS_IMPERSONATION_ACL = "nimbus.impersonation.acl";
    /**
     * A whitelist of the RAS scheduler strategies allowed by nimbus. Should be a list of fully-qualified class names or null to allow all.
     */
    @IsStringList
    public static final String NIMBUS_SCHEDULER_STRATEGY_CLASS_WHITELIST = "nimbus.scheduler.strategy.class.whitelist";
    /**
     * Full path to the worker-laucher executable that will be used to lauch workers when SUPERVISOR_RUN_WORKER_AS_USER is set to true.
     */
    @IsString
    public static final String SUPERVISOR_WORKER_LAUNCHER = "supervisor.worker.launcher";
    /**
     * Map a version of storm to a worker classpath that can be used to run it. This allows the supervisor to select an available version of
     * storm that is compatible with what a topology was launched with.
     *
     * <p>Only the major and minor version numbers are used, although this may change in the future.  The code will
     * first try to find a version
     * that is the same or higher than the requested version, but with the same major version number.  If it cannot it will fall back to
     * using one with a lower minor version, but in some cases this might fail as some features may be missing.
     *
     * <p>Because of how this selection process works please don't include two releases with the same major and minor versions as it is
     * undefined which will be selected.  Also it is good practice to just include one release for each major version you want to support
     * unless the minor versions are truly not compatible with each other. This is to avoid maintenance and testing overhead.
     *
     * <p>This config needs to be set on all supervisors and on nimbus.  In general this can be the output of calling storm classpath on the
     * version you want and adding in an entry for the config directory for that release.  You should modify the storm.yaml of each of these
     * versions to match the features and settings you want on the main version.
     */
    @IsMapEntryType(keyType = String.class, valueType = String.class)
    public static final String SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP = "supervisor.worker.version.classpath.map";
    /**
     * Map a version of storm to a worker's main class.  In most cases storm should have correct defaults and just setting
     * SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP is enough.
     */
    @IsMapEntryType(keyType = String.class, valueType = String.class)
    public static final String SUPERVISOR_WORKER_VERSION_MAIN_MAP = "supervisor.worker.version.main.map";
    /**
     * Map a version of storm to a worker's logwriter class. In most cases storm should have correct defaults and just setting
     * SUPERVISOR_WORKER_VERSION_CLASSPATH_MAP is enough.
     */
    @IsMapEntryType(keyType = String.class, valueType = String.class)
    public static final String SUPERVISOR_WORKER_VERSION_LOGWRITER_MAP = "supervisor.worker.version.logwriter.map";
    /**
     * The version of storm to assume a topology should run as if not version is given by the client when submitting the topology.
     */
    @IsString
    public static final String SUPERVISOR_WORKER_DEFAULT_VERSION = "supervisor.worker.default.version";
    /**
     * A directory on the local filesystem used by Storm for any local filesystem usage it needs. The directory must exist and the Storm
     * daemons must have permission to read/write from this location. It could be either absolute or relative. If the setting is a relative
     * directory, it is relative to root directory of Storm installation.
     */
    @IsString
    public static final String STORM_LOCAL_DIR = "storm.local.dir";
    /**
     * The workers-artifacts directory (where we place all workers' logs), can be either absolute or relative. By default,
     * ${storm.log.dir}/workers-artifacts is where worker logs go. If the setting is a relative directory, it is relative to storm.log.dir.
     */
    @IsString
    public static final String STORM_WORKERS_ARTIFACTS_DIR = "storm.workers.artifacts.dir";
    /**
     * A list of hosts of Exhibitor servers used to discover/maintain connection to ZooKeeper cluster. Any configured ZooKeeper servers will
     * be used for the curator/exhibitor backup connection string.
     */
    @IsStringList
    public static final String STORM_EXHIBITOR_SERVERS = "storm.exhibitor.servers";
    /**
     * The port Storm will use to connect to each of the exhibitor servers.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String STORM_EXHIBITOR_PORT = "storm.exhibitor.port";
    /*
     * How often to poll Exhibitor cluster in millis.
     */
    @IsString
    public static final String STORM_EXHIBITOR_URIPATH = "storm.exhibitor.poll.uripath";
    /**
     * How often to poll Exhibitor cluster in millis.
     */
    @IsInteger
    public static final String STORM_EXHIBITOR_POLL = "storm.exhibitor.poll.millis";
    /**
     * The number of times to retry an Exhibitor operation.
     */
    @IsInteger
    public static final String STORM_EXHIBITOR_RETRY_TIMES = "storm.exhibitor.retry.times";
    /*
     * The interval between retries of an Exhibitor operation.
     */
    @IsInteger
    public static final String STORM_EXHIBITOR_RETRY_INTERVAL = "storm.exhibitor.retry.interval";
    /**
     * The ceiling of the interval between retries of an Exhibitor operation.
     */
    @IsInteger
    public static final String STORM_EXHIBITOR_RETRY_INTERVAL_CEILING = "storm.exhibitor.retry.intervalceiling.millis";
    /**
     * The connection timeout for clients to ZooKeeper.
     */
    @IsInteger
    public static final String STORM_ZOOKEEPER_CONNECTION_TIMEOUT = "storm.zookeeper.connection.timeout";
    /**
     * The session timeout for clients to ZooKeeper.
     */
    @IsInteger
    public static final String STORM_ZOOKEEPER_SESSION_TIMEOUT = "storm.zookeeper.session.timeout";
    /**
     * The interval between retries of a Zookeeper operation.
     */
    @IsInteger
    public static final String STORM_ZOOKEEPER_RETRY_INTERVAL = "storm.zookeeper.retry.interval";
    /**
     * The ceiling of the interval between retries of a Zookeeper operation.
     */
    @IsInteger
    public static final String STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING = "storm.zookeeper.retry.intervalceiling.millis";
    /**
     * The number of times to retry a Zookeeper operation.
     */
    @IsInteger
    public static final String STORM_ZOOKEEPER_RETRY_TIMES = "storm.zookeeper.retry.times";
    /**
     * The ClusterState factory that worker will use to create a ClusterState to store state in. Defaults to ZooKeeper.
     */
    @IsString
    public static final String STORM_CLUSTER_STATE_STORE = "storm.cluster.state.store";
    /**
     * How often this worker should heartbeat to the supervisor.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String WORKER_HEARTBEAT_FREQUENCY_SECS = "worker.heartbeat.frequency.secs";
    /**
     * How often executor metrics should report to master, used for RPC heartbeat mode.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String EXECUTOR_METRICS_FREQUENCY_SECS = "executor.metrics.frequency.secs";
    /**
     * How often a task should heartbeat its status to the Pacamker. For 2.0 RPC heartbeat reporting, see {@code
     * EXECUTOR_METRICS_FREQUENCY_SECS }.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TASK_HEARTBEAT_FREQUENCY_SECS = "task.heartbeat.frequency.secs";
    /**
     * How often a task should sync its connections with other tasks (if a task is reassigned, the other tasks sending messages to it need
     * to refresh their connections). In general though, when a reassignment happens other tasks will be notified almost immediately. This
     * configuration is here just in case that notification doesn't come through.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String TASK_REFRESH_POLL_SECS = "task.refresh.poll.secs";
    /**
     * The Access Control List for the DRPC Authorizer.
     *
     * @see org.apache.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer
     */
    @IsType(type = Map.class)
    public static final String DRPC_AUTHORIZER_ACL = "drpc.authorizer.acl";
    /**
     * File name of the DRPC Authorizer ACL.
     *
     * @see org.apache.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer
     */
    @IsString
    public static final String DRPC_AUTHORIZER_ACL_FILENAME = "drpc.authorizer.acl.filename";
    /**
     * Whether the DRPCSimpleAclAuthorizer should deny requests for operations involving functions that have no explicit ACL entry. When set
     * to false (the default) DRPC functions that have no entry in the ACL will be permitted, which is appropriate for a development
     * environment. When set to true, explicit ACL entries are required for every DRPC function, and any request for functions will be
     * denied.
     *
     * @see org.apache.storm.security.auth.authorizer.DRPCSimpleACLAuthorizer
     */
    @IsBoolean
    public static final String DRPC_AUTHORIZER_ACL_STRICT = "drpc.authorizer.acl.strict";
    /**
     * root directory of the storm cgroup hierarchy.
     */
    @IsString
    public static final String STORM_CGROUP_HIERARCHY_DIR = "storm.cgroup.hierarchy.dir";
    /**
     * The number of Buckets.
     */
    @IsInteger
    @IsPositiveNumber
    public static final String NUM_STAT_BUCKETS = "num.stat.buckets";
    /**
     * The root of cgroup for oci to use. On RHEL7, it should be "/sys/fs/cgroup".
     */
    @IsString
    @NotNull
    public static String STORM_OCI_CGROUP_ROOT = "storm.oci.cgroup.root";
    /**
     * Specify the oci image to use.
     */
    @IsString
    public static String TOPOLOGY_OCI_IMAGE = "topology.oci.image";
    /**
     * Interval to check for the worker to check for updated blobs and refresh worker state accordingly. The default is 10 seconds
     */
    @IsInteger
    @IsPositiveNumber
    public static final String WORKER_BLOB_UPDATE_POLL_INTERVAL_SECS = "worker.blob.update.poll.interval.secs";

    /**
     * Specify the Locale for daemon metrics reporter plugin. Use the specified IETF BCP 47 language tag string for a Locale.
     * This config should have been placed in the DaemonConfig class since it is intended only for use by daemons.
     * Keeping it here only for backwards compatibility.
     */
    @IsString
    public static final String STORM_DAEMON_METRICS_REPORTER_PLUGIN_LOCALE = "storm.daemon.metrics.reporter.plugin.locale";

    /**
     * Specify the rate unit in TimeUnit for daemon metrics reporter plugin.
     * This config should have been placed in the DaemonConfig class since it is intended only for use by daemons.
     * Keeping it here only for backwards compatibility.
     */
    @IsString
    public static final String STORM_DAEMON_METRICS_REPORTER_PLUGIN_RATE_UNIT = "storm.daemon.metrics.reporter.plugin.rate.unit";

    /**
     * Specify the duration unit in TimeUnit for daemon metrics reporter plugin.
     * This config should have been placed in the DaemonConfig class since it is intended only for use by daemons.
     * Keeping it here only for backwards compatibility.
     */
    @IsString
    public static final String STORM_DAEMON_METRICS_REPORTER_PLUGIN_DURATION_UNIT = "storm.daemon.metrics.reporter.plugin.duration.unit";

    //DO NOT CHANGE UNLESS WE ADD IN STATE NOT STORED IN THE PARENT CLASS
    private static final long serialVersionUID = -1550278723792864455L;

    public static void setClasspath(Map<String, Object> conf, String cp) {
        conf.put(Config.TOPOLOGY_CLASSPATH, cp);
    }

    public static void setEnvironment(Map<String, Object> conf, Map<String, Object> env) {
        conf.put(Config.TOPOLOGY_ENVIRONMENT, env);
    }

    public static void setDebug(Map<String, Object> conf, boolean isOn) {
        conf.put(Config.TOPOLOGY_DEBUG, isOn);
    }

    public static void setTopologyVersion(Map<String, Object> conf, String version) {
        conf.put(Config.TOPOLOGY_VERSION, version);
    }

    public static void setNumWorkers(Map<String, Object> conf, int workers) {
        conf.put(Config.TOPOLOGY_WORKERS, workers);
    }

    public static void setNumAckers(Map<String, Object> conf, int numExecutors) {
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, numExecutors);
    }

    public static void setNumEventLoggers(Map<String, Object> conf, int numExecutors) {
        conf.put(Config.TOPOLOGY_EVENTLOGGER_EXECUTORS, numExecutors);
    }

    public static void setMessageTimeoutSecs(Map<String, Object> conf, int secs) {
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, secs);
    }

    public static void registerSerialization(Map<String, Object> conf, Class klass) {
        getRegisteredSerializations(conf).add(klass.getName());
    }

    public static void registerSerialization(Map<String, Object> conf, Class klass, Class<? extends Serializer> serializerClass) {
        Map<String, String> register = new HashMap<String, String>();
        register.put(klass.getName(), serializerClass.getName());
        getRegisteredSerializations(conf).add(register);
    }

    public static void registerEventLogger(Map<String, Object> conf, Class<? extends IEventLogger> klass, Map<String, Object> argument) {
        Map<String, Object> m = new HashMap<>();
        m.put("class", klass.getCanonicalName());
        m.put("arguments", argument);

        List<Map<String, Object>> l = (List<Map<String, Object>>) conf.get(TOPOLOGY_EVENT_LOGGER_REGISTER);
        if (l == null) {
            l = new ArrayList<>();
        }
        l.add(m);

        conf.put(TOPOLOGY_EVENT_LOGGER_REGISTER, l);
    }

    public static void registerEventLogger(Map<String, Object> conf, Class<? extends IEventLogger> klass) {
        registerEventLogger(conf, klass, null);
    }

    public static void registerMetricsConsumer(Map<String, Object> conf, Class klass, Object argument, long parallelismHint) {
        HashMap<String, Object> m = new HashMap<>();
        m.put("class", klass.getCanonicalName());
        m.put("parallelism.hint", parallelismHint);
        m.put("argument", argument);

        List l = (List) conf.get(TOPOLOGY_METRICS_CONSUMER_REGISTER);
        if (l == null) {
            l = new ArrayList();
        }
        l.add(m);
        conf.put(TOPOLOGY_METRICS_CONSUMER_REGISTER, l);
    }

    public static void registerMetricsConsumer(Map<String, Object> conf, Class klass, long parallelismHint) {
        registerMetricsConsumer(conf, klass, null, parallelismHint);
    }

    public static void registerMetricsConsumer(Map<String, Object> conf, Class klass) {
        registerMetricsConsumer(conf, klass, null, 1L);
    }

    public static void registerDecorator(Map<String, Object> conf, Class<? extends IKryoDecorator> klass) {
        getRegisteredDecorators(conf).add(klass.getName());
    }

    public static void setKryoFactory(Map<String, Object> conf, Class<? extends IKryoFactory> klass) {
        conf.put(Config.TOPOLOGY_KRYO_FACTORY, klass.getName());
    }

    public static void setSkipMissingKryoRegistrations(Map<String, Object> conf, boolean skip) {
        conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, skip);
    }

    public static void setMaxTaskParallelism(Map<String, Object> conf, int max) {
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, max);
    }

    public static void setMaxSpoutPending(Map<String, Object> conf, int max) {
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, max);
    }

    public static void setStatsSampleRate(Map<String, Object> conf, double rate) {
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, rate);
    }

    public static void setFallBackOnJavaSerialization(Map<String, Object> conf, boolean fallback) {
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, fallback);
    }

    private static List getRegisteredSerializations(Map<String, Object> conf) {
        List ret;
        if (!conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
            ret = new ArrayList();
        } else {
            ret = new ArrayList((List) conf.get(Config.TOPOLOGY_KRYO_REGISTER));
        }
        conf.put(Config.TOPOLOGY_KRYO_REGISTER, ret);
        return ret;
    }

    private static List getRegisteredDecorators(Map<String, Object> conf) {
        List ret;
        if (!conf.containsKey(Config.TOPOLOGY_KRYO_DECORATORS)) {
            ret = new ArrayList();
        } else {
            ret = new ArrayList((List) conf.get(Config.TOPOLOGY_KRYO_DECORATORS));
        }
        conf.put(Config.TOPOLOGY_KRYO_DECORATORS, ret);
        return ret;
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setClasspath(String cp) {
        setClasspath(this, cp);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setEnvironment(Map<String, Object> env) {
        setEnvironment(this, env);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setDebug(boolean isOn) {
        setDebug(this, isOn);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setTopologyVersion(String version) {
        setTopologyVersion(this, version);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setNumWorkers(int workers) {
        setNumWorkers(this, workers);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setNumAckers(int numExecutors) {
        setNumAckers(this, numExecutors);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setNumEventLoggers(int numExecutors) {
        setNumEventLoggers(this, numExecutors);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setMessageTimeoutSecs(int secs) {
        setMessageTimeoutSecs(this, secs);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void registerSerialization(Class klass) {
        registerSerialization(this, klass);
    }

    public void registerSerialization(Class klass, Class<? extends Serializer> serializerClass) {
        registerSerialization(this, klass, serializerClass);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void registerEventLogger(Class<? extends IEventLogger> klass, Map<String, Object> argument) {
        registerEventLogger(this, klass, argument);
    }

    public void registerEventLogger(Class<? extends IEventLogger> klass) {
        registerEventLogger(this, klass, null);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void registerMetricsConsumer(Class klass, Object argument, long parallelismHint) {
        registerMetricsConsumer(this, klass, argument, parallelismHint);
    }

    public void registerMetricsConsumer(Class klass, long parallelismHint) {
        registerMetricsConsumer(this, klass, parallelismHint);
    }

    public void registerMetricsConsumer(Class klass) {
        registerMetricsConsumer(this, klass);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void registerDecorator(Class<? extends IKryoDecorator> klass) {
        registerDecorator(this, klass);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setKryoFactory(Class<? extends IKryoFactory> klass) {
        setKryoFactory(this, klass);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setSkipMissingKryoRegistrations(boolean skip) {
        setSkipMissingKryoRegistrations(this, skip);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setMaxTaskParallelism(int max) {
        setMaxTaskParallelism(this, max);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setMaxSpoutPending(int max) {
        setMaxSpoutPending(this, max);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setStatsSampleRate(double rate) {
        setStatsSampleRate(this, rate);
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public void setFallBackOnJavaSerialization(boolean fallback) {
        setFallBackOnJavaSerialization(this, fallback);
    }

    /**
     * Set the max heap size allow per worker for this topology.
     *
     * @param size the maximum heap size for a worker.
     */
    public void setTopologyWorkerMaxHeapSize(Number size) {
        if (size != null) {
            this.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, size);
        }
    }

    /**
     * Declares executors of component1 cannot be on the same worker as executors of component2. This function is additive. Thus a user can
     * setTopologyComponentWorkerConstraints("A", "B") and then setTopologyComponentWorkerConstraints("B", "C") Which means executors form
     * component A cannot be on the same worker with executors of component B and executors of Component B cannot be on workers with
     * executors of component C
     *
     * @param component1 a component that should not coexist with component2
     * @param component2 a component that should not coexist with component1
     */
    public void setTopologyComponentWorkerConstraints(String component1, String component2) {
        if (component1 != null && component2 != null) {
            List<String> constraintPair = Arrays.asList(component1, component2);
            List<List<String>> constraints = (List<List<String>>) computeIfAbsent(Config.TOPOLOGY_RAS_CONSTRAINTS,
                (k) -> new ArrayList<>(1));
            constraints.add(constraintPair);
        }
    }

    /**
     * Sets the maximum number of states that will be searched in the constraint solver strategy.
     *
     * @param numStates maximum number of stats to search.
     */
    public void setTopologyConstraintsMaxStateSearch(int numStates) {
        this.put(Config.TOPOLOGY_RAS_CONSTRAINT_MAX_STATE_SEARCH, numStates);
    }

    /**
     * Set the priority for a topology.
     */
    public void setTopologyPriority(int priority) {
        this.put(Config.TOPOLOGY_PRIORITY, priority);
    }

    public void setTopologyStrategy(String strategy) {
        this.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, strategy);
    }

    private static final String HOSTNAME_PATTERN = "_HOST";

    private static String substituteHostnameInPrincipal(String principal) throws UnknownHostException {
        if (principal != null) {
            String[] components = principal.split("[/@]");
            if (components.length == 3 && components[1].equals(HOSTNAME_PATTERN)) {
                principal = components[0] + "/" + Utils.localHostname() + "@" + components[2];
            }
        }
        return principal;
    }

    @Deprecated
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static String getBlobstoreHDFSPrincipal(Map conf) throws UnknownHostException {
        return getHdfsPrincipal(conf);
    }

    /**
     * Get the hostname substituted hdfs principal.
     * @param conf the storm Configuration
     * @return the principal
     * @throws UnknownHostException on UnknowHostException
     */
    public static String getHdfsPrincipal(Map<String, Object> conf) throws UnknownHostException {
        String ret;

        String blobstorePrincipal = (String) conf.get(Config.BLOBSTORE_HDFS_PRINCIPAL);
        String hdfsPrincipal = (String) conf.get(Config.STORM_HDFS_LOGIN_PRINCIPAL);
        if (blobstorePrincipal == null && hdfsPrincipal == null) {
            return null;
        } else if (blobstorePrincipal == null) {
            ret = hdfsPrincipal;
        } else if (hdfsPrincipal == null) {
            LOG.warn("{} is used as the hdfs principal. Please use {} instead",
                Config.BLOBSTORE_HDFS_PRINCIPAL, Config.STORM_HDFS_LOGIN_PRINCIPAL);
            ret = blobstorePrincipal;
        } else {
            //both not null;
            LOG.warn("Both {} and {} are set. Use {} only.",
                Config.BLOBSTORE_HDFS_PRINCIPAL, Config.STORM_HDFS_LOGIN_PRINCIPAL, Config.STORM_HDFS_LOGIN_PRINCIPAL);
            ret = hdfsPrincipal;
        }
        return substituteHostnameInPrincipal(ret);
    }

    /**
     * Get the hdfs keytab.
     * @param conf the storm Configuration
     * @return the keytab
     */
    public static String getHdfsKeytab(Map<String, Object> conf) {
        String ret;

        String blobstoreKeyTab = (String) conf.get(Config.BLOBSTORE_HDFS_KEYTAB);
        String hdfsKeyTab = (String) conf.get(Config.STORM_HDFS_LOGIN_KEYTAB);
        if (blobstoreKeyTab == null && hdfsKeyTab == null) {
            return null;
        } else if (blobstoreKeyTab == null) {
            ret = hdfsKeyTab;
        } else if (hdfsKeyTab == null) {
            LOG.warn("{} is used as the hdfs keytab. Please use {} instead",
                Config.BLOBSTORE_HDFS_KEYTAB, Config.STORM_HDFS_LOGIN_KEYTAB);
            ret = blobstoreKeyTab;
        } else {
            //both not null;
            LOG.warn("Both {} and {} are set. Use {} only.",
                Config.BLOBSTORE_HDFS_KEYTAB, Config.STORM_HDFS_LOGIN_KEYTAB, Config.STORM_HDFS_LOGIN_KEYTAB);
            ret = hdfsKeyTab;
        }
        return ret;
    }

}

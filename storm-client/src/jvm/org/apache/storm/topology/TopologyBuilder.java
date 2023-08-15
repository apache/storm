/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.topology;

import static org.apache.storm.spout.CheckpointSpout.CHECKPOINT_COMPONENT_ID;
import static org.apache.storm.spout.CheckpointSpout.CHECKPOINT_STREAM_ID;
import static org.apache.storm.utils.Utils.parseJson;

import java.io.NotSerializableException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.storm.Config;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.ComponentObject;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.NullStruct;
import org.apache.storm.generated.SharedMemory;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.grouping.PartialKeyGrouping;
import org.apache.storm.hooks.IWorkerHook;
import org.apache.storm.lambda.LambdaBiConsumerBolt;
import org.apache.storm.lambda.LambdaConsumerBolt;
import org.apache.storm.lambda.LambdaSpout;
import org.apache.storm.lambda.SerializableBiConsumer;
import org.apache.storm.lambda.SerializableConsumer;
import org.apache.storm.lambda.SerializableSupplier;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.spout.CheckpointSpout;
import org.apache.storm.state.State;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.apache.storm.windowing.TupleWindow;

/**
 * TopologyBuilder exposes the Java API for specifying a topology for Storm to execute. Topologies are Thrift structures in the end, but
 * since the Thrift API is so verbose, TopologyBuilder greatly eases the process of creating topologies. The template for creating and
 * submitting a topology looks something like:
 *
 * <p>```java TopologyBuilder builder = new TopologyBuilder();
 *
 * <p>builder.setSpout("1", new TestWordSpout(true), 5); builder.setSpout("2", new TestWordSpout(true), 3); builder.setBolt("3", new
 * TestWordCounter(), 3) .fieldsGrouping("1", new Fields("word")) .fieldsGrouping("2", new Fields("word")); builder.setBolt("4", new
 * TestGlobalCount()) .globalGrouping("1");
 *
 * <p>Map&lt;String, Object&gt; conf = new HashMap(); conf.put(Config.TOPOLOGY_WORKERS, 4);
 *
 * <p>StormSubmitter.submitTopology("mytopology", conf, builder.createTopology()); ```
 *
 * <p>Running the exact same topology in local mode (in process), and configuring it to log all tuples emitted, looks
 * like the following. Note that it lets the topology run for 10 seconds before shutting down the local cluster.
 *
 * <p>```java TopologyBuilder builder = new TopologyBuilder();
 *
 * <p>builder.setSpout("1", new TestWordSpout(true), 5); builder.setSpout("2", new TestWordSpout(true), 3); builder.setBolt("3", new
 * TestWordCounter(), 3) .fieldsGrouping("1", new Fields("word")) .fieldsGrouping("2", new Fields("word")); builder.setBolt("4", new
 * TestGlobalCount()) .globalGrouping("1");
 *
 * <p>Map&lt;String, Object&gt; conf = new HashMap(); conf.put(Config.TOPOLOGY_WORKERS, 4); conf.put(Config.TOPOLOGY_DEBUG, true);
 *
 * <p>try (LocalCluster cluster = new LocalCluster(); LocalTopology topo = cluster.submitTopology("mytopology", conf,
 * builder.createTopology());){ Utils.sleep(10000); } ```
 *
 * <p>The pattern for `TopologyBuilder` is to map component ids to components using the setSpout and setBolt methods. Those methods return
 * objects that are then used to declare the inputs for that component.
 */
public class TopologyBuilder {
    private final Map<String, IRichBolt> bolts = new HashMap<>();
    private final Map<String, IRichSpout> spouts = new HashMap<>();
    private final Map<String, ComponentCommon> commons = new HashMap<>();
    private final Map<String, Set<String>> componentToSharedMemory = new HashMap<>();
    private final Map<String, SharedMemory> sharedMemory = new HashMap<>();
    private boolean hasStatefulBolt = false;

    private Map<String, StateSpoutSpec> stateSpouts = new HashMap<>();
    private List<ByteBuffer> workerHooks = new ArrayList<>();

    private static String mergeIntoJson(Map<String, Object> into, Map<String, Object> newMap) {
        Map<String, Object> res = new HashMap<>(into);
        res.putAll(newMap);
        return JSONValue.toJSONString(res);
    }

    public StormTopology createTopology() {
        Map<String, Bolt> boltSpecs = new HashMap<>();
        Map<String, SpoutSpec> spoutSpecs = new HashMap<>();
        maybeAddCheckpointSpout();
        for (String boltId : bolts.keySet()) {
            IRichBolt bolt = bolts.get(boltId);
            bolt = maybeAddCheckpointTupleForwarder(bolt);
            ComponentCommon common = getComponentCommon(boltId, bolt);
            try {
                maybeAddCheckpointInputs(common);
                boltSpecs.put(boltId, new Bolt(ComponentObject.serialized_java(Utils.javaSerialize(bolt)), common));
            } catch (RuntimeException wrapperCause) {
                if (wrapperCause.getCause() != null && NotSerializableException.class.equals(wrapperCause.getCause().getClass())) {
                    throw new IllegalStateException("Bolt '" + boltId + "' contains a non-serializable field of type "
                                    + wrapperCause.getCause().getMessage() + ", "
                                    + "which was instantiated prior to topology creation. "
                                    + wrapperCause.getCause().getMessage()
                                    + " "
                                    + "should be instantiated within the prepare method of '"
                                    + boltId
                                    + " at the earliest.",
                            wrapperCause);
                }
                throw wrapperCause;
            }
        }
        for (String spoutId : spouts.keySet()) {
            IRichSpout spout = spouts.get(spoutId);
            ComponentCommon common = getComponentCommon(spoutId, spout);
            try {
                spoutSpecs.put(spoutId, new SpoutSpec(ComponentObject.serialized_java(Utils.javaSerialize(spout)), common));
            } catch (RuntimeException wrapperCause) {
                if (wrapperCause.getCause() != null && NotSerializableException.class.equals(wrapperCause.getCause().getClass())) {
                    throw new IllegalStateException(
                            "Spout '" + spoutId + "' contains a non-serializable field of type "
                                    + wrapperCause.getCause().getMessage()
                                    + ", which was instantiated prior to topology creation. "
                                    + wrapperCause.getCause().getMessage()
                                    + " should be instantiated within the open method of '"
                                    + spoutId
                                    + " at the earliest.",
                            wrapperCause);
                }
                throw wrapperCause;
            }
        }

        StormTopology stormTopology = new StormTopology(spoutSpecs,
                                                        boltSpecs,
                                                        new HashMap<>());

        stormTopology.set_worker_hooks(workerHooks);

        if (!componentToSharedMemory.isEmpty()) {
            stormTopology.set_component_to_shared_memory(componentToSharedMemory);
            stormTopology.set_shared_memory(sharedMemory);
        }

        return Utils.addVersions(stormTopology);
    }

    /**
     * Define a new bolt in this topology with parallelism of just one thread.
     *
     * @param id   the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology with the specified amount of parallelism.
     *
     * @param id               the id of this component. This id is referenced by other components that want to consume this bolt's
     *                         outputs.
     * @param bolt             the bolt
     * @param parallelismHint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process
     *                         somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) throws IllegalArgumentException {
        validateUnusedId(id);
        initCommon(id, bolt, parallelismHint);
        bolts.put(id, bolt);
        return new BoltGetter(id);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a simpler to use but more restricted kind of bolt. Basic
     * bolts are intended for non-aggregation processing and automate the anchoring/acking process to achieve proper reliability in the
     * topology.
     *
     * @param id   the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a simpler to use but more restricted kind of bolt. Basic
     * bolts are intended for non-aggregation processing and automate the anchoring/acking process to achieve proper reliability in the
     * topology.
     *
     * @param id               the id of this component. This id is referenced by other components that want to consume this bolt's
     *                         outputs.
     * @param bolt             the basic bolt
     * @param parallelismHint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process
     *                         somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelismHint) throws IllegalArgumentException {
        return setBolt(id, new BasicBoltExecutor(bolt), parallelismHint);
    }

    /**
     * Define a new bolt in this topology. This defines a windowed bolt, intended for windowing operations. The {@link
     * IWindowedBolt#execute(TupleWindow)} method is triggered for each window interval with the list of current events in the window.
     *
     * @param id   the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the windowed bolt
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IWindowedBolt bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology. This defines a windowed bolt, intended for windowing operations. The {@link
     * IWindowedBolt#execute(TupleWindow)} method is triggered for each window interval with the list of current events in the window.
     *
     * @param id               the id of this component. This id is referenced by other components that want to consume this bolt's
     *                         outputs.
     * @param bolt             the windowed bolt
     * @param parallelismHint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process
     *                         somwehere around the cluster.
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IWindowedBolt bolt, Number parallelismHint) throws IllegalArgumentException {
        return setBolt(id, new WindowedBoltExecutor(bolt), parallelismHint);
    }

    /**
     * Define a new bolt in this topology. This defines a stateful bolt, that requires its state (of computation) to be saved. When this
     * bolt is initialized, the {@link IStatefulBolt#initState(State)} method is invoked after {@link IStatefulBolt#prepare(Map,
     * TopologyContext, OutputCollector)} but before {@link IStatefulBolt#execute(Tuple)} with its previously saved state.
     * <p>
     * The framework provides at-least once guarantee for the state updates. Bolts (both stateful and non-stateful) in a stateful topology
     * are expected to anchor the tuples while emitting and ack the input tuples once its processed.
     * </p>
     *
     * @param id   the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the stateful bolt
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends State> BoltDeclarer setBolt(String id, IStatefulBolt<T> bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology. This defines a stateful bolt, that requires its state (of computation) to be saved. When this
     * bolt is initialized, the {@link IStatefulBolt#initState(State)} method is invoked after {@link IStatefulBolt#prepare(Map,
     * TopologyContext, OutputCollector)} but before {@link IStatefulBolt#execute(Tuple)} with its previously saved state.
     * <p>
     * The framework provides at-least once guarantee for the state updates. Bolts (both stateful and non-stateful) in a stateful topology
     * are expected to anchor the tuples while emitting and ack the input tuples once its processed.
     * </p>
     *
     * @param id               the id of this component. This id is referenced by other components that want to consume this bolt's
     *                         outputs.
     * @param bolt             the stateful bolt
     * @param parallelismHint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process
     *                         somwehere around the cluster.
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends State> BoltDeclarer setBolt(String id, IStatefulBolt<T> bolt, Number parallelismHint) throws
        IllegalArgumentException {
        hasStatefulBolt = true;
        return setBolt(id, new StatefulBoltExecutor<T>(bolt), parallelismHint);
    }

    /**
     * Define a new bolt in this topology. This defines a stateful windowed bolt, intended for stateful windowing operations. The {@link
     * IStatefulWindowedBolt#execute(TupleWindow)} method is triggered for each window interval with the list of current events in the
     * window. During initialization of this bolt {@link IStatefulWindowedBolt#initState(State)} is invoked with its previously saved
     * state.
     *
     * @param id   the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the stateful windowed bolt
     * @param <T>  the type of the state (e.g. {@link org.apache.storm.state.KeyValueState})
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends State> BoltDeclarer setBolt(String id, IStatefulWindowedBolt<T> bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology. This defines a stateful windowed bolt, intended for stateful windowing operations. The {@link
     * IStatefulWindowedBolt#execute(TupleWindow)} method is triggered for each window interval with the list of current events in the
     * window. During initialization of this bolt {@link IStatefulWindowedBolt#initState(State)} is invoked with its previously saved
     * state.
     *
     * @param id               the id of this component. This id is referenced by other components that want to consume this bolt's
     *                         outputs.
     * @param bolt             the stateful windowed bolt
     * @param parallelismHint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process
     *                         somwehere around the cluster.
     * @param <T>              the type of the state (e.g. {@link org.apache.storm.state.KeyValueState})
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends State> BoltDeclarer setBolt(String id, IStatefulWindowedBolt<T> bolt, Number parallelismHint) throws
        IllegalArgumentException {
        hasStatefulBolt = true;
        IStatefulBolt<T> executor;
        if (bolt.isPersistent()) {
            executor = new PersistentWindowedBoltExecutor<>(bolt);
        } else {
            executor = new StatefulWindowedBoltExecutor<T>(bolt);
        }
        return setBolt(id, new StatefulBoltExecutor<T>(executor), parallelismHint);
    }

    /**
     * Define a new bolt in this topology. This defines a lambda basic bolt, which is a simpler to use but more restricted kind of bolt.
     * Basic bolts are intended for non-aggregation processing and automate the anchoring/acking process to achieve proper reliability in
     * the topology.
     *
     * @param id         the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param biConsumer lambda expression that implements tuple processing for this bolt
     * @param fields     fields for tuple that should be emitted to downstream bolts
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, SerializableBiConsumer<Tuple, BasicOutputCollector> biConsumer, String... fields) throws
        IllegalArgumentException {
        return setBolt(id, biConsumer, null, fields);
    }

    /**
     * Define a new bolt in this topology. This defines a lambda basic bolt, which is a simpler to use but more restricted kind of bolt.
     * Basic bolts are intended for non-aggregation processing and automate the anchoring/acking process to achieve proper reliability in
     * the topology.
     *
     * @param id               the id of this component. This id is referenced by other components that want to consume this bolt's
     *                         outputs.
     * @param biConsumer       lambda expression that implements tuple processing for this bolt
     * @param fields           fields for tuple that should be emitted to downstream bolts
     * @param parallelismHint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process
     *                         somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, SerializableBiConsumer<Tuple, BasicOutputCollector> biConsumer, Number parallelismHint,
                                String... fields) throws IllegalArgumentException {
        return setBolt(id, new LambdaBiConsumerBolt(biConsumer, fields), parallelismHint);
    }

    /**
     * Define a new bolt in this topology. This defines a lambda basic bolt, which is a simpler to use but more restricted kind of bolt.
     * Basic bolts are intended for non-aggregation processing and automate the anchoring/acking process to achieve proper reliability in
     * the topology.
     *
     * @param id       the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param consumer lambda expression that implements tuple processing for this bolt
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, SerializableConsumer<Tuple> consumer) throws IllegalArgumentException {
        return setBolt(id, consumer, null);
    }

    /**
     * Define a new bolt in this topology. This defines a lambda basic bolt, which is a simpler to use but more restricted kind of bolt.
     * Basic bolts are intended for non-aggregation processing and automate the anchoring/acking process to achieve proper reliability in
     * the topology.
     *
     * @param id               the id of this component. This id is referenced by other components that want to consume this bolt's
     *                         outputs.
     * @param consumer         lambda expression that implements tuple processing for this bolt
     * @param parallelismHint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process
     *                         somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     *
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, SerializableConsumer<Tuple> consumer, Number parallelismHint) throws IllegalArgumentException {
        return setBolt(id, new LambdaConsumerBolt(consumer), parallelismHint);
    }

    /**
     * Define a new spout in this topology.
     *
     * @param id    the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout) throws IllegalArgumentException {
        return setSpout(id, spout, null);
    }

    /**
     * Define a new spout in this topology with the specified parallelism. If the spout declares itself as non-distributed, the
     * parallelism_hint will be ignored and only one task will be allocated to this component.
     *
     * @param id               the id of this component. This id is referenced by other components that want to consume this spout's
     *                         outputs.
     * @param parallelismHint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a
     *                         process somewhere around the cluster.
     * @param spout            the spout
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) throws IllegalArgumentException {
        validateUnusedId(id);
        initCommon(id, spout, parallelismHint);
        spouts.put(id, spout);
        return new SpoutGetter(id);
    }

    /**
     * Define a new spout in this topology.
     *
     * @param id       the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param supplier lambda expression that implements tuple generating for this spout
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, SerializableSupplier<?> supplier) throws IllegalArgumentException {
        return setSpout(id, supplier, null);
    }

    /**
     * Define a new spout in this topology with the specified parallelism. If the spout declares itself as non-distributed, the
     * parallelism_hint will be ignored and only one task will be allocated to this component.
     *
     * @param id               the id of this component. This id is referenced by other components that want to consume this spout's
     *                         outputs.
     * @param parallelismHint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a
     *                         process somewhere around the cluster.
     * @param supplier         lambda expression that implements tuple generating for this spout
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, SerializableSupplier<?> supplier, Number parallelismHint) throws IllegalArgumentException {
        return setSpout(id, new LambdaSpout(supplier), parallelismHint);
    }

    /**
     * Add a new worker lifecycle hook.
     *
     * @param workerHook the lifecycle hook to add
     */
    public void addWorkerHook(IWorkerHook workerHook) {
        if (null == workerHook) {
            throw new IllegalArgumentException("WorkerHook must not be null.");
        }

        workerHooks.add(ByteBuffer.wrap(Utils.javaSerialize(workerHook)));
    }

    private void validateUnusedId(String id) {
        if (bolts.containsKey(id)) {
            throw new IllegalArgumentException("Bolt has already been declared for id " + id);
        }
        if (spouts.containsKey(id)) {
            throw new IllegalArgumentException("Spout has already been declared for id " + id);
        }
        if (stateSpouts.containsKey(id)) {
            throw new IllegalArgumentException("State spout has already been declared for id " + id);
        }
    }

    /**
     * If the topology has at least one stateful bolt add a {@link CheckpointSpout} component to the topology.
     */
    private void maybeAddCheckpointSpout() {
        if (hasStatefulBolt) {
            setSpout(CHECKPOINT_COMPONENT_ID, new CheckpointSpout(), 1);
        }
    }

    private void maybeAddCheckpointInputs(ComponentCommon common) {
        if (hasStatefulBolt) {
            addCheckPointInputs(common);
        }
    }

    /**
     * If the topology has at least one stateful bolt all the non-stateful bolts are wrapped in {@link CheckpointTupleForwarder} so that the
     * checkpoint tuples can flow through the topology.
     */
    private IRichBolt maybeAddCheckpointTupleForwarder(IRichBolt bolt) {
        if (hasStatefulBolt && !(bolt instanceof StatefulBoltExecutor)) {
            bolt = new CheckpointTupleForwarder(bolt);
        }
        return bolt;
    }

    /**
     * For bolts that has incoming streams from spouts (the root bolts), add checkpoint stream from checkpoint spout to its input. For other
     * bolts, add checkpoint stream from the previous bolt to its input.
     */
    private void addCheckPointInputs(ComponentCommon component) {
        Set<GlobalStreamId> checkPointInputs = new HashSet<>();
        for (GlobalStreamId inputStream : component.get_inputs().keySet()) {
            String sourceId = inputStream.get_componentId();
            if (spouts.containsKey(sourceId)) {
                checkPointInputs.add(new GlobalStreamId(CHECKPOINT_COMPONENT_ID, CHECKPOINT_STREAM_ID));
            } else {
                checkPointInputs.add(new GlobalStreamId(sourceId, CHECKPOINT_STREAM_ID));
            }
        }
        for (GlobalStreamId streamId : checkPointInputs) {
            component.put_to_inputs(streamId, Grouping.all(new NullStruct()));
        }
    }

    private ComponentCommon getComponentCommon(String id, IComponent component) {
        ComponentCommon ret = new ComponentCommon(commons.get(id));
        OutputFieldsGetter getter = new OutputFieldsGetter();
        component.declareOutputFields(getter);
        ret.set_streams(getter.getFieldsDeclaration());
        return ret;
    }

    private void initCommon(String id, IComponent component, Number parallelism) throws IllegalArgumentException {
        ComponentCommon common = new ComponentCommon();
        common.set_inputs(new HashMap<GlobalStreamId, Grouping>());
        if (parallelism != null) {
            int dop = parallelism.intValue();
            if (dop < 1) {
                throw new IllegalArgumentException("Parallelism must be positive.");
            }
            common.set_parallelism_hint(dop);
        }
        Map<String, Object> conf = component.getComponentConfiguration();
        if (conf != null) {
            common.set_json_conf(JSONValue.toJSONString(conf));
        }
        commons.put(id, common);
    }

    protected class ConfigGetter<T extends ComponentConfigurationDeclarer> extends BaseConfigurationDeclarer<T> {
        String id;

        public ConfigGetter(String id) {
            this.id = id;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T addConfigurations(Map<String, Object> conf) {
            if (conf != null) {
                if (conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
                    throw new IllegalArgumentException("Cannot set serializations for a component using fluent API");
                }
                if (!conf.isEmpty()) {
                    String currConf = commons.get(id).get_json_conf();
                    commons.get(id).set_json_conf(mergeIntoJson(parseJson(currConf), conf));
                }
            }
            return (T) this;
        }

        /**
         * return the current component configuration.
         *
         * @return the current configuration.
         */
        @Override
        public Map<String, Object> getComponentConfiguration() {
            return parseJson(commons.get(id).get_json_conf());
        }

        @Override
        public T addResources(Map<String, Double> resources) {
            if (resources != null && !resources.isEmpty()) {
                String currConf = commons.get(id).get_json_conf();
                Map<String, Object> conf = parseJson(currConf);
                Map<String, Double> currentResources =
                    (Map<String, Double>) conf.computeIfAbsent(Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, (k) -> new HashMap<>());
                currentResources.putAll(resources);
                commons.get(id).set_json_conf(JSONValue.toJSONString(conf));
            }
            return (T) this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T addResource(String resourceName, Number resourceValue) {
            Map<String, Object> componentConf = parseJson(commons.get(id).get_json_conf());
            Map<String, Double> resourcesMap = (Map<String, Double>) componentConf.computeIfAbsent(
                Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, (k) -> new HashMap<>());

            resourcesMap.put(resourceName, resourceValue.doubleValue());

            return addConfiguration(Config.TOPOLOGY_COMPONENT_RESOURCES_MAP, resourcesMap);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T addSharedMemory(SharedMemory request) {
            SharedMemory found = sharedMemory.get(request.get_name());
            if (found != null && !found.equals(request)) {
                throw new IllegalArgumentException("Cannot have multiple different shared memory regions with the same name");
            }
            sharedMemory.put(request.get_name(), request);
            Set<String> mems = componentToSharedMemory.computeIfAbsent(id, (k) -> new HashSet<>());
            mems.add(request.get_name());
            return (T) this;
        }
    }

    protected class SpoutGetter extends ConfigGetter<SpoutDeclarer> implements SpoutDeclarer {
        public SpoutGetter(String id) {
            super(id);
        }
    }

    protected class BoltGetter extends ConfigGetter<BoltDeclarer> implements BoltDeclarer {
        private String boltId;

        public BoltGetter(String boltId) {
            super(boltId);
            this.boltId = boltId;
        }

        @Override
        public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
            return fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
        }

        @Override
        public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
            return grouping(componentId, streamId, Grouping.fields(fields.toList()));
        }

        @Override
        public BoltDeclarer globalGrouping(String componentId) {
            return globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        @Override
        public BoltDeclarer globalGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.fields(new ArrayList<String>()));
        }

        @Override
        public BoltDeclarer shuffleGrouping(String componentId) {
            return shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        @Override
        public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.shuffle(new NullStruct()));
        }

        @Override
        public BoltDeclarer localOrShuffleGrouping(String componentId) {
            return localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        @Override
        public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.local_or_shuffle(new NullStruct()));
        }

        @Override
        public BoltDeclarer noneGrouping(String componentId) {
            return noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        @Override
        public BoltDeclarer noneGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.none(new NullStruct()));
        }

        @Override
        public BoltDeclarer allGrouping(String componentId) {
            return allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        @Override
        public BoltDeclarer allGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.all(new NullStruct()));
        }

        @Override
        public BoltDeclarer directGrouping(String componentId) {
            return directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        @Override
        public BoltDeclarer directGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.direct(new NullStruct()));
        }

        private BoltDeclarer grouping(String componentId, String streamId, Grouping grouping) {
            commons.get(boltId).put_to_inputs(new GlobalStreamId(componentId, streamId), grouping);
            return this;
        }

        @Override
        public BoltDeclarer grouping(GlobalStreamId id, Grouping grouping) {
            return grouping(id.get_componentId(), id.get_streamId(), grouping);
        }

        @Override
        public BoltDeclarer partialKeyGrouping(String componentId, Fields fields) {
            return customGrouping(componentId, new PartialKeyGrouping(fields));
        }

        @Override
        public BoltDeclarer partialKeyGrouping(String componentId, String streamId, Fields fields) {
            return customGrouping(componentId, streamId, new PartialKeyGrouping(fields));
        }

        @Override
        public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
            return customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping);
        }

        @Override
        public BoltDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping) {
            return grouping(componentId, streamId, Grouping.custom_serialized(Utils.javaSerialize(grouping)));
        }
    }
}

/**
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
package backtype.storm.topology;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.grouping.PartialKeyGrouping;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONValue;

/**
 * TopologyBuilder exposes the Java API for specifying a topology for Storm
 * to execute. Topologies are Thrift structures in the end, but since the Thrift API
 * is so verbose, TopologyBuilder greatly eases the process of creating topologies.
 * The template for creating and submitting a topology looks something like:
 *
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5, 20);
 * builder.setSpout("2", new TestWordSpout(true), 3, 20);
 * builder.setBolt("3", new TestWordCounter(), 3, 40)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * 
 * StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
 * </pre>
 *
 * Running the exact same topology in local mode (in process), and configuring it to log all tuples
 * emitted, looks like the following. Note that it lets the topology run for 10 seconds
 * before shutting down the local cluster.
 *
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5, 20);
 * builder.setSpout("2", new TestWordSpout(true), 3, 20);
 * builder.setBolt("3", new TestWordCounter(), 3, 40)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * conf.put(Config.TOPOLOGY_DEBUG, true);
 *
 * LocalCluster cluster = new LocalCluster();
 * cluster.submitTopology("mytopology", conf, builder.createTopology());
 * Utils.sleep(10000);
 * cluster.shutdown();
 * </pre>
 *
 * <p>The pattern for TopologyBuilder is to map component ids to components using the setSpout
 * and setBolt methods. Those methods return objects that are then used to declare
 * the inputs for that component.</p>
 */
public class TopologyBuilder {
    private Map<String, IRichBolt> _bolts = new HashMap<String, IRichBolt>();
    private Map<String, IRichSpout> _spouts = new HashMap<String, IRichSpout>();
    private Map<String, ComponentCommon> _commons = new HashMap<String, ComponentCommon>();

//    private Map<String, Map<GlobalStreamId, Grouping>> _inputs = new HashMap<String, Map<GlobalStreamId, Grouping>>();

    private Map<String, StateSpoutSpec> _stateSpouts = new HashMap<String, StateSpoutSpec>();
    
    
    public StormTopology createTopology() {
        Map<String, Bolt> boltSpecs = new HashMap<String, Bolt>();
        Map<String, SpoutSpec> spoutSpecs = new HashMap<String, SpoutSpec>();
        for(String boltId: _bolts.keySet()) {
            IRichBolt bolt = _bolts.get(boltId);
            ComponentCommon common = getComponentCommon(boltId, bolt);
            boltSpecs.put(boltId, new Bolt(ComponentObject.serialized_java(Utils.javaSerialize(bolt)), common));
        }
        for(String spoutId: _spouts.keySet()) {
            IRichSpout spout = _spouts.get(spoutId);
            ComponentCommon common = getComponentCommon(spoutId, spout);
            spoutSpecs.put(spoutId, new SpoutSpec(ComponentObject.serialized_java(Utils.javaSerialize(spout)), common));
            
        }
        return new StormTopology(spoutSpecs,
                                 boltSpecs,
                                 new HashMap<String, StateSpoutSpec>());
    }

    /**
     * Define a new bolt in this topology with parallelism of just one thread and transport batching disabled.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, (Number)null);
    }

    /**
     * Define a new bolt in this topology with the specified amount of parallelism and transport batching disabled.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) throws IllegalArgumentException {
        return setBolt(id, bolt, parallelism_hint, (Number) null);
    }

    /**
     * Define a new bolt in this topology with parallelism of just one thread and transport batch sizes for the specified streams.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @param batch_sizes the number of output tuples that should be assembled into a batch per output stream. Each batch will be transferred to a consumer instance at once.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends Number> BoltDeclarer setBolt(String id, IRichBolt bolt, Map<String,T> batch_sizes) throws IllegalArgumentException {
        return setBolt(id, bolt, null, batch_sizes);
    }
    
    /**
     * Define a new bolt in this topology with the specified amount of parallelism and single transport batch size for all output streams.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @param batch_size the number of output tuples that should be assembled into a batch. Each batch will be transferred to a consumer instance at once.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint, Number batch_size) throws IllegalArgumentException {
        return setBolt(id, bolt, parallelism_hint, applyBatchSizeToDeclaredStreams(bolt, batch_size));
    }

    /**
     * Define a new bolt in this topology with the specified amount of parallelism and transport batch sizes for the specified streams.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @param batch_sizes the number of output tuples that should be assembled into a batch per output stream. Each batch will be transferred to a consumer instance at once.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends Number> BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint, Map<String,T> batch_sizes) throws IllegalArgumentException {
        validateUnusedId(id);
        initCommon(id, bolt, parallelism_hint, convertHashMap(batch_sizes));
        _bolts.put(id, bolt);
        return new BoltGetter(id);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, (Number) null);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) throws IllegalArgumentException {
        return setBolt(id, bolt, parallelism_hint, (Number) null);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @param batch_sizes the number of output tuples that should be assembled into a batch per output stream. Each batch will be transferred to a consumer instance at once.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends Number> BoltDeclarer setBolt(String id, IBasicBolt bolt, Map<String,T> batch_sizes) throws IllegalArgumentException {
        return setBolt(id, bolt, null, batch_sizes);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @param batch_size the number of output tuples that should be assembled into a batch. Each batch will be transferred to a consumer instance at once.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint, Number batch_size) throws IllegalArgumentException {
        return setBolt(id, bolt, parallelism_hint, applyBatchSizeToDeclaredStreams(bolt, batch_size));
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @param batch_sizes the number of output tuples that should be assembled into a batch per output stream. Each batch will be transferred to a consumer instance at once.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends Number> BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint, Map<String,T> batch_sizes) throws IllegalArgumentException {
        return setBolt(id, new BasicBoltExecutor(bolt), parallelism_hint, batch_sizes);
    }

    /**
     * Define a new spout in this topology with parallelism of just one thread and transport batching disabled.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout) throws IllegalArgumentException {
        return setSpout(id, spout, (Number) null);
    }

    /**
     * Define a new spout in this topology with the specified parallelism and transport batching
     * disabled. If the spout declares itself as non-distributed, The parallelism_hint will be
     * ignored and only one task will be allocated to this component.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     * @param parallelism_hint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a process somwehere around the cluster.
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint) throws IllegalArgumentException {
        return setSpout(id, spout, parallelism_hint, (Number) null);
    }

    /**
     * Define a new spout in this topology of just one thread and transport batch sizes for the specified streams.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     * @param batch_sizes the number of output tuples that should be assembled into a batch per output stream. Each batch will be transferred to a consumer instance at once.
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends Number> SpoutDeclarer setSpout(String id, IRichSpout spout, Map<String,T> batch_sizes) throws IllegalArgumentException {
        return setSpout(id, spout, null, batch_sizes);
    }

    /**
     * Define a new spout in this topology with the specified parallelism and single transport
     * batch size for all output streams. If the spout declares itself as non-distributed, the
     * parallelism_hint will be ignored and only one task will be allocated to this component.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     * @param parallelism_hint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a process somwehere around the cluster.
     * @param batch_size the number of output tuples that should be assembled into a batch. Each batch will be transferred to a consumer instance at once.
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint, Number batch_size) throws IllegalArgumentException {
        return setSpout(id, spout, parallelism_hint, applyBatchSizeToDeclaredStreams(spout, batch_size));
    }

    /**
     * Define a new spout in this topology with the specified parallelism and transport batch sizes
     * for the specified streams. If the spout declares itself as non-distributed, the
     * parallelism_hint will be ignored and only one task will be allocated to this component.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     * @param parallelism_hint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a process somwehere around the cluster.
     * @param batch_sizes the number of output tuples that should be assembled into a batch per output stream. Each batch will be transferred to a consumer instance at once.
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends Number> SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint, Map<String,T> batch_sizes) throws IllegalArgumentException {
        validateUnusedId(id);
        initCommon(id, spout, parallelism_hint, convertHashMap(batch_sizes));
        _spouts.put(id, spout);
        return new SpoutGetter(id);
    }

    public void setStateSpout(String id, IRichStateSpout stateSpout) throws IllegalArgumentException {
        setStateSpout(id, stateSpout, (Number) null);
    }

    public void setStateSpout(String id, IRichStateSpout stateSpout, Number parallelism_hint) throws IllegalArgumentException {
        setStateSpout(id, stateSpout, parallelism_hint,(Number) null);
    }

    public <T extends Number> void setStateSpout(String id, IRichStateSpout stateSpout, Map<String,T> batch_sizes) throws IllegalArgumentException {
        setStateSpout(id, stateSpout,  null, batch_sizes);
    }

    public void setStateSpout(String id, IRichStateSpout stateSpout, Number parallelism_hint, Number batch_size) throws IllegalArgumentException {
        setStateSpout(id, stateSpout, parallelism_hint, applyBatchSizeToDeclaredStreams(stateSpout, batch_size));
    }

    public <T extends Number> void setStateSpout(String id, IRichStateSpout stateSpout, Number parallelism_hint, Map<String,T> batch_sizes) throws IllegalArgumentException {
        validateUnusedId(id);
        // TODO: finish
    }


    private void validateUnusedId(String id) {
        if(_bolts.containsKey(id)) {
            throw new IllegalArgumentException("Bolt has already been declared for id " + id);
        }
        if(_spouts.containsKey(id)) {
            throw new IllegalArgumentException("Spout has already been declared for id " + id);
        }
        if(_stateSpouts.containsKey(id)) {
            throw new IllegalArgumentException("State spout has already been declared for id " + id);
        }
    }

    private ComponentCommon getComponentCommon(String id, IComponent component) {
        ComponentCommon ret = new ComponentCommon(_commons.get(id));
        
        OutputFieldsGetter getter = new OutputFieldsGetter();
        component.declareOutputFields(getter);
        ret.set_streams(getter.getFieldsDeclaration());
        return ret;        
    }
    
    private void initCommon(String id, IComponent component, Number parallelism, Map<String,Integer> batch_sizes) throws IllegalArgumentException {
        ComponentCommon common = new ComponentCommon();
        common.set_inputs(new HashMap<GlobalStreamId, Grouping>());
        if(parallelism!=null) {
            int dop = parallelism.intValue();
            if(dop < 1) {
                throw new IllegalArgumentException("Parallelism must be positive.");
            }
            common.set_parallelism_hint(dop);
        }
        if(batch_sizes!=null) common.set_batch_sizes(batch_sizes);
        Map conf = component.getComponentConfiguration();
        if(conf!=null) common.set_json_conf(JSONValue.toJSONString(conf));
        _commons.put(id, common);
    }

    protected class ConfigGetter<T extends ComponentConfigurationDeclarer> extends BaseConfigurationDeclarer<T> {
        String _id;
        
        public ConfigGetter(String id) {
            _id = id;
        }
        
        @Override
        public T addConfigurations(Map<String, Object> conf) {
            if(conf!=null && conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
                throw new IllegalArgumentException("Cannot set serializations for a component using fluent API");
            }
            String currConf = _commons.get(_id).get_json_conf();
            _commons.get(_id).set_json_conf(mergeIntoJson(parseJson(currConf), conf));
            return (T) this;
        }
    }
    
    protected class SpoutGetter extends ConfigGetter<SpoutDeclarer> implements SpoutDeclarer {
        public SpoutGetter(String id) {
            super(id);
        }        
    }
    
    protected class BoltGetter extends ConfigGetter<BoltDeclarer> implements BoltDeclarer {
        private String _boltId;

        public BoltGetter(String boltId) {
            super(boltId);
            _boltId = boltId;
        }

        public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
            return fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
        }

        public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
            return grouping(componentId, streamId, Grouping.fields(fields.toList()));
        }

        public BoltDeclarer globalGrouping(String componentId) {
            return globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer globalGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.fields(new ArrayList<String>()));
        }

        public BoltDeclarer shuffleGrouping(String componentId) {
            return shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.shuffle(new NullStruct()));
        }

        public BoltDeclarer localOrShuffleGrouping(String componentId) {
            return localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.local_or_shuffle(new NullStruct()));
        }
        
        public BoltDeclarer noneGrouping(String componentId) {
            return noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer noneGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.none(new NullStruct()));
        }

        public BoltDeclarer allGrouping(String componentId) {
            return allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer allGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.all(new NullStruct()));
        }

        public BoltDeclarer directGrouping(String componentId) {
            return directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer directGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.direct(new NullStruct()));
        }

        private BoltDeclarer grouping(String componentId, String streamId, Grouping grouping) {
            _commons.get(_boltId).put_to_inputs(new GlobalStreamId(componentId, streamId), grouping);
            return this;
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

        @Override
        public BoltDeclarer grouping(GlobalStreamId id, Grouping grouping) {
            return grouping(id.get_componentId(), id.get_streamId(), grouping);
        }        
    }
    
    private static Map parseJson(String json) {
        if(json==null) return new HashMap();
        else return (Map) JSONValue.parse(json);
    }
    
    private static String mergeIntoJson(Map into, Map newMap) {
        Map res = new HashMap(into);
        if(newMap!=null) res.putAll(newMap);
        return JSONValue.toJSONString(res);
    }
    
    private static HashMap<String, Number> applyBatchSizeToDeclaredStreams(IComponent spout_or_bolt, Number batch_size) {
        HashMap<String, Number> batchSizes = null;
        if (batch_size != null) {
            batchSizes = new HashMap<String, Number>();

            OutputFieldsGetter declarer = new OutputFieldsGetter();
            spout_or_bolt.declareOutputFields(declarer);

            for (String outputStreamId : declarer.getFieldsDeclaration().keySet()) {
                batchSizes.put(outputStreamId, batch_size);
            }
        }
        return batchSizes;
    }

    private static <T extends Number> HashMap<String, Integer> convertHashMap(Map<String, T> batch_sizes) {
        HashMap<String, Integer> batchSizes = null;
        if(batch_sizes != null) {
            batchSizes = new HashMap<String,Integer>();
            for(Map.Entry<String,T> e : batch_sizes.entrySet()) {
                batchSizes.put(e.getKey(), e.getValue().intValue());
            }
        }
        return batchSizes;
    }
}

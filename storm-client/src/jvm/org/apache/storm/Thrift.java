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

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.ComponentObject;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.JavaObject;
import org.apache.storm.generated.JavaObjectArg;
import org.apache.storm.generated.NullStruct;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StormTopology._Fields;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.task.IBolt;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Thrift {
    private static Logger LOG = LoggerFactory.getLogger(Thrift.class);

    private static StormTopology._Fields[] STORM_TOPOLOGY_FIELDS = null;
    private static StormTopology._Fields[] SPOUT_FIELDS =
        { StormTopology._Fields.SPOUTS, StormTopology._Fields.STATE_SPOUTS };

    static {
        Set<_Fields> keys = StormTopology.metaDataMap.keySet();
        keys.toArray(STORM_TOPOLOGY_FIELDS = new StormTopology._Fields[keys.size()]);
    }

    public static StormTopology._Fields[] getTopologyFields() {
        return STORM_TOPOLOGY_FIELDS;
    }

    public static StormTopology._Fields[] getSpoutFields() {
        return SPOUT_FIELDS;
    }

    public static StreamInfo directOutputFields(List<String> fields) {
        return new StreamInfo(fields, true);
    }

    public static StreamInfo outputFields(List<String> fields) {
        return new StreamInfo(fields, false);
    }

    public static Grouping prepareShuffleGrouping() {
        return Grouping.shuffle(new NullStruct());
    }

    public static Grouping prepareLocalOrShuffleGrouping() {
        return Grouping.local_or_shuffle(new NullStruct());
    }

    public static Grouping prepareFieldsGrouping(List<String> fields) {
        return Grouping.fields(fields);
    }

    public static Grouping prepareGlobalGrouping() {
        return prepareFieldsGrouping(new ArrayList<String>());
    }

    public static Grouping prepareDirectGrouping() {
        return Grouping.direct(new NullStruct());
    }

    public static Grouping prepareAllGrouping() {
        return Grouping.all(new NullStruct());
    }

    public static Grouping prepareNoneGrouping() {
        return Grouping.none(new NullStruct());
    }

    public static Grouping prepareCustomStreamGrouping(Object obj) {
        return Grouping.custom_serialized(Utils.javaSerialize(obj));
    }

    public static Grouping prepareCustomJavaObjectGrouping(JavaObject obj) {
        return Grouping.custom_object(obj);
    }

    public static Object instantiateJavaObject(JavaObject obj) {

        List<JavaObjectArg> args = obj.get_args_list();
        Class[] paraTypes = new Class[args.size()];
        Object[] paraValues = new Object[args.size()];
        for (int i = 0; i < args.size(); i++) {
            JavaObjectArg arg = args.get(i);
            paraValues[i] = arg.getFieldValue();

            if (arg.getSetField().equals(JavaObjectArg._Fields.INT_ARG)) {
                paraTypes[i] = Integer.class;
            } else if (arg.getSetField().equals(JavaObjectArg._Fields.LONG_ARG)) {
                paraTypes[i] = Long.class;
            } else if (arg.getSetField().equals(JavaObjectArg._Fields.STRING_ARG)) {
                paraTypes[i] = String.class;
            } else if (arg.getSetField().equals(JavaObjectArg._Fields.BOOL_ARG)) {
                paraTypes[i] = Boolean.class;
            } else if (arg.getSetField().equals(JavaObjectArg._Fields.BINARY_ARG)) {
                paraTypes[i] = ByteBuffer.class;
            } else if (arg.getSetField().equals(JavaObjectArg._Fields.DOUBLE_ARG)) {
                paraTypes[i] = Double.class;
            } else {
                paraTypes[i] = Object.class;
            }
        }

        try {
            Class clazz = Class.forName(obj.get_full_class_name());
            Constructor cons = clazz.getConstructor(paraTypes);
            return cons.newInstance(paraValues);
        } catch (Exception e) {
            LOG.error("java object instantiation failed", e);
        }

        return null;

    }

    public static Grouping._Fields groupingType(Grouping grouping) {
        return grouping.getSetField();
    }

    public static List<String> fieldGrouping(Grouping grouping) {
        if (!Grouping._Fields.FIELDS.equals(groupingType(grouping))) {
            throw new IllegalArgumentException("Tried to get grouping fields from non fields grouping");
        }
        return grouping.get_fields();
    }

    public static boolean isGlobalGrouping(Grouping grouping) {
        if (Grouping._Fields.FIELDS.equals(groupingType(grouping))) {
            return fieldGrouping(grouping).isEmpty();
        }

        return false;
    }

    public static int getParallelismHint(ComponentCommon componentCommon) {
        if (!componentCommon.is_set_parallelism_hint()) {
            return 1;
        } else {
            return componentCommon.get_parallelism_hint();
        }
    }

    public static ComponentObject serializeComponentObject(Object obj) {
        return ComponentObject.serialized_java(Utils.javaSerialize(obj));
    }

    public static Object deserializeComponentObject(ComponentObject obj) {
        if (obj.getSetField() != ComponentObject._Fields.SERIALIZED_JAVA) {
            throw new RuntimeException("Cannot deserialize non-java-serialized object");
        }
        return Utils.javaDeserialize(obj.get_serialized_java(), Serializable.class);
    }

    public static ComponentCommon prepareComponentCommon(Map<GlobalStreamId, Grouping> inputs, Map<String,
        StreamInfo> outputs, Integer parallelismHint) {
        return prepareComponentCommon(inputs, outputs, parallelismHint, null);
    }

    public static ComponentCommon prepareComponentCommon(Map<GlobalStreamId, Grouping> inputs, Map<String, StreamInfo> outputs,
                                                         Integer parallelismHint, Map<String, Object> conf) {
        Map<GlobalStreamId, Grouping> mappedInputs = new HashMap<>();
        Map<String, StreamInfo> mappedOutputs = new HashMap<>();
        if (inputs != null && !inputs.isEmpty()) {
            mappedInputs.putAll(inputs);
        }
        if (outputs != null && !outputs.isEmpty()) {
            mappedOutputs.putAll(outputs);
        }
        ComponentCommon component = new ComponentCommon(mappedInputs, mappedOutputs);
        if (parallelismHint != null) {
            component.set_parallelism_hint(parallelismHint);
        }
        if (conf != null) {
            component.set_json_conf(JSONValue.toJSONString(conf));
        }
        return component;
    }

    public static SpoutSpec prepareSerializedSpoutDetails(IRichSpout spout, Map<String, StreamInfo> outputs) {
        return new SpoutSpec(ComponentObject.serialized_java(Utils.javaSerialize(spout)),
                prepareComponentCommon(new HashMap<>(), outputs, null, null));
    }

    public static Bolt prepareSerializedBoltDetails(Map<GlobalStreamId, Grouping> inputs, IBolt bolt, Map<String, StreamInfo> outputs,
                                                    Integer parallelismHint, Map<String, Object> conf) {
        ComponentCommon common = prepareComponentCommon(inputs, outputs, parallelismHint, conf);
        return new Bolt(ComponentObject.serialized_java(Utils.javaSerialize(bolt)), common);
    }

    public static BoltDetails prepareBoltDetails(Map<GlobalStreamId, Grouping> inputs, Object bolt) {
        return prepareBoltDetails(inputs, bolt, null, null);
    }

    public static BoltDetails prepareBoltDetails(Map<GlobalStreamId, Grouping> inputs, Object bolt,
                                                 Integer parallelismHint) {
        return prepareBoltDetails(inputs, bolt, parallelismHint, null);
    }

    public static BoltDetails prepareBoltDetails(Map<GlobalStreamId, Grouping> inputs, Object bolt,
                                                 Integer parallelismHint, Map<String, Object> conf) {
        BoltDetails details = new BoltDetails(bolt, conf, parallelismHint, inputs);
        return details;
    }

    public static SpoutDetails prepareSpoutDetails(IRichSpout spout) {
        return prepareSpoutDetails(spout, null, null);
    }

    public static SpoutDetails prepareSpoutDetails(IRichSpout spout, Integer parallelismHint) {
        return prepareSpoutDetails(spout, parallelismHint, null);
    }

    public static SpoutDetails prepareSpoutDetails(IRichSpout spout, Integer parallelismHint, Map<String, Object> conf) {
        SpoutDetails details = new SpoutDetails(spout, parallelismHint, conf);
        return details;
    }

    private static void addInputs(BoltDeclarer declarer, Map<GlobalStreamId, Grouping> inputs) {
        for (Entry<GlobalStreamId, Grouping> entry : inputs.entrySet()) {
            declarer.grouping(entry.getKey(), entry.getValue());
        }
    }

    public static StormTopology buildTopology(HashMap<String, SpoutDetails> spoutMap,
                                              HashMap<String, BoltDetails> boltMap, HashMap<String, StateSpoutSpec> stateMap) {
        return buildTopology(spoutMap, boltMap);
    }

    public static StormTopology buildTopology(Map<String, SpoutDetails> spoutMap, Map<String, BoltDetails> boltMap) {
        TopologyBuilder builder = new TopologyBuilder();
        for (Entry<String, SpoutDetails> entry : spoutMap.entrySet()) {
            String spoutId = entry.getKey();
            SpoutDetails spec = entry.getValue();
            SpoutDeclarer spoutDeclarer = builder.setSpout(spoutId, spec.getSpout(), spec.getParallelism());
            spoutDeclarer.addConfigurations(spec.getConf());
        }
        for (Entry<String, BoltDetails> entry : boltMap.entrySet()) {
            String spoutId = entry.getKey();
            BoltDetails spec = entry.getValue();
            BoltDeclarer boltDeclarer = null;
            if (spec.bolt instanceof IRichBolt) {
                boltDeclarer = builder.setBolt(spoutId, (IRichBolt) spec.getBolt(), spec.getParallelism());
            } else {
                boltDeclarer = builder.setBolt(spoutId, (IBasicBolt) spec.getBolt(), spec.getParallelism());
            }
            boltDeclarer.addConfigurations(spec.getConf());
            addInputs(boltDeclarer, spec.getInputs());
        }
        return builder.createTopology();
    }

    public static class SpoutDetails {
        private IRichSpout spout;
        private Integer parallelism;
        private Map<String, Object> conf;

        public SpoutDetails(IRichSpout spout, Integer parallelism, Map<String, Object> conf) {
            this.spout = spout;
            this.parallelism = parallelism;
            this.conf = conf;
        }

        public IRichSpout getSpout() {
            return spout;
        }

        public Integer getParallelism() {
            return parallelism;
        }

        public Map<String, Object> getConf() {
            return conf;
        }
    }

    public static class BoltDetails {
        private Object bolt;
        private Map<String, Object> conf;
        private Integer parallelism;
        private Map<GlobalStreamId, Grouping> inputs;

        public BoltDetails(Object bolt, Map<String, Object> conf, Integer parallelism,
                           Map<GlobalStreamId, Grouping> inputs) {
            this.bolt = bolt;
            this.conf = conf;
            this.parallelism = parallelism;
            this.inputs = inputs;
        }

        public Object getBolt() {
            return bolt;
        }

        public Map<String, Object> getConf() {
            return conf;
        }

        public Map<GlobalStreamId, Grouping> getInputs() {
            return inputs;
        }

        public Integer getParallelism() {
            return parallelism;
        }
    }
}

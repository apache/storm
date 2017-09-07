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

package org.apache.storm.loadgen;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Configuration for a simulated topology.
 */
public class TopologyLoadConf {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyLoadConf.class);
    static final Set<String> IMPORTANT_CONF_KEYS = Collections.unmodifiableSet(new HashSet(Arrays.asList(
        Config.TOPOLOGY_WORKERS,
        Config.TOPOLOGY_ACKER_EXECUTORS,
        Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT,
        Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
        Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
        Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING,
        Config.TOPOLOGY_DEBUG,
        Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,
        Config.TOPOLOGY_ISOLATED_MACHINES,
        Config.TOPOLOGY_MAX_SPOUT_PENDING,
        Config.TOPOLOGY_MAX_TASK_PARALLELISM,
        Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,
        Config.TOPOLOGY_PRIORITY,
        Config.TOPOLOGY_SCHEDULER_STRATEGY,
        Config.TOPOLOGY_SHELLBOLT_MAX_PENDING,
        Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS,
        Config.TOPOLOGY_SPOUT_WAIT_STRATEGY,
        Config.TOPOLOGY_WORKER_CHILDOPTS,
        Config.TOPOLOGY_WORKER_GC_CHILDOPTS,
        Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE,
        Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB
    )));
    private static AtomicInteger topoUniquifier = new AtomicInteger(0);

    public final String name;
    public final Map<String, Object> topoConf;
    public final List<LoadCompConf> spouts;
    public final List<LoadCompConf> bolts;
    public final List<InputStream> streams;
    private final AtomicInteger boltUniquifier = new AtomicInteger(0);
    private final AtomicInteger spoutUniquifier = new AtomicInteger(0);
    private final AtomicInteger streamUniquifier = new AtomicInteger(0);

    /**
     * Parse the TopologyLoadConf from a file in YAML format.
     * @param file the file to read from
     * @return the parsed conf
     * @throws IOException if there is an issue reading the file.
     */
    public static TopologyLoadConf fromConf(File file) throws IOException {
        Yaml yaml = new Yaml(new SafeConstructor());
        Map<String, Object> yamlConf = (Map<String, Object>)yaml.load(new FileReader(file));
        return TopologyLoadConf.fromConf(yamlConf);
    }

    /**
     * Parse the TopologyLoadConf from a config map.
     * @param conf the config with the TopologyLoadConf in it
     * @return the parsed instance.
     */
    public static TopologyLoadConf fromConf(Map<String, Object> conf) {
        Map<String, Object> topoConf = null;
        if (conf.containsKey("config")) {
            topoConf = new HashMap<>((Map<String, Object>)conf.get("config"));
        }

        List<LoadCompConf> spouts = new ArrayList<>();
        for (Map<String, Object> spoutInfo: (List<Map<String, Object>>) conf.get("spouts")) {
            spouts.add(LoadCompConf.fromConf(spoutInfo));
        }

        List<LoadCompConf> bolts = new ArrayList<>();
        List<Map<String, Object>> boltInfos = (List<Map<String, Object>>) conf.get("bolts");
        if (boltInfos != null) {
            for (Map<String, Object> boltInfo : boltInfos) {
                bolts.add(LoadCompConf.fromConf(boltInfo));
            }
        }

        List<InputStream> streams = new ArrayList<>();
        List<Map<String, Object>> streamInfos = (List<Map<String, Object>>) conf.get("streams");
        if (streamInfos != null) {
            for (Map<String, Object> streamInfo: streamInfos) {
                streams.add(InputStream.fromConf(streamInfo));
            }
        }

        return new TopologyLoadConf((String)conf.get("name"), topoConf, spouts, bolts, streams);
    }

    /**
     * Write this out to a file in YAML format.
     * @param file the file to write to.
     * @throws IOException if there is an error writing to the file.
     */
    public void writeTo(File file) throws IOException {
        Yaml yaml = new Yaml(new SafeConstructor());
        try (FileWriter writer = new FileWriter(file)) {
            yaml.dump(toConf(), writer);
        }
    }

    /**
     * Convert this into a YAML String.
     * @return this as a YAML String.
     */
    public String toYamlString() {
        Yaml yaml = new Yaml(new SafeConstructor());
        StringWriter writer = new StringWriter();
        yaml.dump(toConf(), writer);
        return writer.toString();
    }

    /**
     * Covert this into a Map config.
     * @return this as a Map config.
     */
    public Map<String, Object> toConf() {
        Map<String, Object> ret = new HashMap<>();
        if (name != null) {
            ret.put("name", name);
        }
        if (topoConf != null) {
            ret.put("config", topoConf);
        }
        if (spouts != null && !spouts.isEmpty()) {
            ret.put("spouts", spouts.stream().map(LoadCompConf::toConf)
                .collect(Collectors.toList()));
        }

        if (bolts != null && !bolts.isEmpty()) {
            ret.put("bolts", bolts.stream().map(LoadCompConf::toConf)
                .collect(Collectors.toList()));
        }

        if (streams != null && !streams.isEmpty()) {
            ret.put("streams", streams.stream().map(InputStream::toConf)
                .collect(Collectors.toList()));
        }
        return ret;
    }

    /**
     * Constructor.
     * @param name the name of the topology.
     * @param topoConf the config for the topology
     * @param spouts the spouts for the topology
     * @param bolts the bolts for the topology
     * @param streams the streams for the topology
     */
    public TopologyLoadConf(String name, Map<String, Object> topoConf,
                            List<LoadCompConf> spouts, List<LoadCompConf> bolts, List<InputStream> streams) {
        this.name = name;
        this.topoConf = topoConf;
        this.spouts = spouts;
        this.bolts = bolts;
        this.streams = streams;
    }

    private static String getUniqueTopoName() {
        return "topology_" + asCharString(topoUniquifier.getAndIncrement());
    }

    private String getUniqueBoltName() {
        return "bolt_" + asCharString(boltUniquifier.getAndIncrement());
    }

    private String getUniqueSpoutName() {
        return "spout_" + asCharString(spoutUniquifier.getAndIncrement());
    }

    private String getUniqueStreamName() {
        return "stream_" + asCharString(spoutUniquifier.getAndIncrement());
    }

    private static String asCharString(int value) {
        int div = value / 26;
        int remainder = value % 26;
        String ret = "";
        if (div > 0) {
            ret = asCharString(div);
        }
        ret += (char)((int)'a' + remainder);
        return ret;
    }

    public TopologyLoadConf withName(String baseName) {
        return new TopologyLoadConf(baseName, topoConf, spouts, bolts, streams);
    }

    /**
     * The first one that is not null
     * @param rest all the other somethings
     * @param <V> whatever type you want.
     * @return the first one that is not null
     */
    static <V> V or(V...rest) {
        for (V i: rest) {
            if (i != null) {
                return i;
            }
        }
        return null;
    }

    LoadCompConf scaleCompParallel(LoadCompConf comp, double v, Map<String, Double> topoSpecificParallel) {
        LoadCompConf ret = comp;
        double scale = or(topoSpecificParallel.get(name + ":" + comp.id),
            topoSpecificParallel.get(name + ":*"),
            topoSpecificParallel.get("*:" + comp.id),
            v);
        if (scale != 1.0) {
            ret = ret.scaleParallel(scale);
        }
        return ret;
    }

    LoadCompConf scaleCompThroughput(LoadCompConf comp, double v, Map<String, Double> topoSpecificParallel) {
        LoadCompConf ret = comp;
        double scale = or(topoSpecificParallel.get(name + ":" + comp.id),
            topoSpecificParallel.get(name + ":*"),
            topoSpecificParallel.get("*:" + comp.id),
            v);
        if (scale != 1.0) {
            ret = ret.scaleThroughput(scale);
        }
        return ret;
    }

    /**
     * Scale all of the components in the topology by a percentage (but keep the throughput the same).
     * @param v the amount to scale them by.  1.0 is nothing, 0.5 cuts them in half, 2.0 doubles them.
     * @return a copy of this with the needed adjustments made.
     */
    public TopologyLoadConf scaleParallel(double v, Map<String, Double> topoSpecific) {
        if (v == 1.0 && (topoSpecific == null || topoSpecific.isEmpty())) {
            return this;
        }
        List<LoadCompConf> scaledSpouts = spouts.stream().map((s) -> scaleCompParallel(s, v, topoSpecific))
            .collect(Collectors.toList());
        List<LoadCompConf> scaledBolts = bolts.stream().map((s) -> scaleCompParallel(s, v, topoSpecific))
            .collect(Collectors.toList());
        return new TopologyLoadConf(name, topoConf, scaledSpouts, scaledBolts, streams);
    }

    /**
     * Scale the throughput of the entire topology by a percentage.
     * @param v the amount to scale it by 1.0 is nothing 0.5 cuts it in half and 2.0 doubles it.
     * @return a copy of this with the needed adjustments made.
     */
    public TopologyLoadConf scaleThroughput(double v, Map<String, Double> topoSpecific) {
        if (v == 1.0 && (topoSpecific == null || topoSpecific.isEmpty())) {
            return this;
        }
        List<LoadCompConf> scaledSpouts = spouts.stream().map((s) -> scaleCompThroughput(s, v, topoSpecific))
            .collect(Collectors.toList());
        List<LoadCompConf> scaledBolts = bolts.stream().map((s) -> scaleCompThroughput(s, v, topoSpecific))
            .collect(Collectors.toList());
        return new TopologyLoadConf(name, topoConf, scaledSpouts, scaledBolts, streams);
    }

    /**
     * Create a new version of this topology with identifiable information removed.
     * @return the anonymized version of the TopologyLoadConf.
     */
    public TopologyLoadConf anonymize() {
        Map<String, String> remappedComponents = new HashMap<>();
        Map<GlobalStreamId, GlobalStreamId> remappedStreams = new HashMap<>();
        for (LoadCompConf comp: bolts) {
            String newId = getUniqueBoltName();
            remappedComponents.put(comp.id, newId);
            if (comp.streams != null) {
                for (OutputStream out : comp.streams) {
                    GlobalStreamId orig = new GlobalStreamId(comp.id, out.id);
                    GlobalStreamId remapped = new GlobalStreamId(newId, getUniqueStreamName());
                    remappedStreams.put(orig, remapped);
                }
            }
        }

        for (LoadCompConf comp: spouts) {
            remappedComponents.put(comp.id, getUniqueSpoutName());
            String newId = getUniqueSpoutName();
            remappedComponents.put(comp.id, newId);
            if (comp.streams != null) {
                for (OutputStream out : comp.streams) {
                    GlobalStreamId orig = new GlobalStreamId(comp.id, out.id);
                    GlobalStreamId remapped = new GlobalStreamId(newId, getUniqueStreamName());
                    remappedStreams.put(orig, remapped);
                }
            }
        }

        for (InputStream in : streams) {
            if (!remappedComponents.containsKey(in.toComponent)) {
                remappedComponents.put(in.toComponent, getUniqueSpoutName());
            }
            GlobalStreamId orig = in.gsid();
            if (!remappedStreams.containsKey(orig)) {
                //Even if the topology is not valid we still need to remap it all
                String remappedComp = remappedComponents.computeIfAbsent(in.fromComponent, (key) -> {
                    LOG.warn("stream's {} from is not defined {}", in.id, in.fromComponent);
                    return getUniqueBoltName();
                });
                remappedStreams.put(orig, new GlobalStreamId(remappedComp, getUniqueStreamName()));
            }
        }

        //Now we need to map them all back again
        List<LoadCompConf> remappedSpouts = spouts.stream()
            .map((orig) -> orig.remap(remappedComponents, remappedStreams))
            .collect(Collectors.toList());
        List<LoadCompConf> remappedBolts = bolts.stream()
            .map((orig) -> orig.remap(remappedComponents, remappedStreams))
            .collect(Collectors.toList());
        List<InputStream> remappedInputStreams = streams.stream()
            .map((orig) -> orig.remap(remappedComponents, remappedStreams))
            .collect(Collectors.toList());
        return new TopologyLoadConf(getUniqueTopoName(), anonymizeTopoConf(topoConf), remappedSpouts, remappedBolts, remappedInputStreams);
    }

    private static Map<String,Object> anonymizeTopoConf(Map<String, Object> topoConf) {
        //Only keep important conf keys
        Map<String, Object> ret = new HashMap<>();
        for (Map.Entry<String, Object> entry: topoConf.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (IMPORTANT_CONF_KEYS.contains(key)) {
                if (Config.TOPOLOGY_WORKER_CHILDOPTS.equals(key)
                    || Config.TOPOLOGY_WORKER_GC_CHILDOPTS.equals(key)) {
                    value = cleanupChildOpts(value);
                }
                ret.put(key, value);
            }
        }
        return ret;
    }

    private static Object cleanupChildOpts(Object value) {
        if (value instanceof String) {
            String sv = (String) value;
            StringBuffer ret = new StringBuffer();
            for (String part: sv.split("\\s+")) {
                if (part.startsWith("-X")) {
                    ret.append(part).append(" ");
                }
            }
            return ret.toString();
        } else {
            List<String> ret = new ArrayList<>();
            for (String subValue: (Collection<String>)value) {
                ret.add((String)cleanupChildOpts(subValue));
            }
            return ret.stream().filter((item) -> item != null && !item.isEmpty()).collect(Collectors.toList());
        }
    }

    /**
     * Try to see if this looks like a trident topology.
     * NOTE: this will not work for anonymized configs
     * @return true if it does else false.
     */
    public boolean looksLikeTrident() {
        for (LoadCompConf spout: spouts) {
            if (spout.id.startsWith("$mastercoord")) {
                return true;
            }
        }

        for (LoadCompConf bolt: bolts) {
            if (bolt.id.startsWith("$spoutcoord")) {
                return true;
            }
        }

        for (InputStream in: streams) {
            if (in.id.equals("$batch")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the messages emitted per second in aggregate across all streams in the topology.
     * @return messages per second.
     */
    public double getAllEmittedAggregate() {
        double ret = getSpoutEmittedAggregate();
        for (LoadCompConf bolt: bolts) {
            ret += bolt.getAllEmittedAggregate();
        }
        return ret;
    }

    /**
     * Get the messages emitted per second in aggregate for all of the spouts in the topology.
     * @return messages per second.
     */
    public double getSpoutEmittedAggregate() {
        double ret = 0;
        for (LoadCompConf spout: spouts) {
            ret += spout.getAllEmittedAggregate();
        }
        return ret;
    }

    /**
     * Try and guess at the actual number of messages emitted per second by a trident topology, not the number of batches.
     * This does not work on an anonymized conf.
     * @return messages per second or 0 if this does not look like a trident topology.
     */
    public double getTridentEstimatedEmittedAggregate() {
        //In this case we are ignoring the coord stuff, and only looking at
        double ret = 0;
        if (looksLikeTrident()) {
            List<LoadCompConf> all = new ArrayList<>(bolts);
            all.addAll(spouts);
            for (LoadCompConf comp : all) {
                if (comp.id.startsWith("spout-")) {
                    if (comp.streams != null) {
                        for (OutputStream out: comp.streams) {
                            if (!out.id.startsWith("$")
                                && !out.id.startsWith("__")
                                && out.rate != null) {
                                ret += out.rate.mean * comp.parallelism;
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }

    public TopologyLoadConf replaceShuffleWithLocalOrShuffle() {
        List<InputStream> modified = streams.stream().map((in) -> in.replaceShuffleWithLocalOrShuffle()).collect(Collectors.toList());
        return new TopologyLoadConf(name, topoConf, spouts, bolts, modified);
    }
}

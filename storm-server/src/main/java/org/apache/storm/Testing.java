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

package org.apache.storm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.CompletableSpout;
import org.apache.storm.testing.CompleteTopologyParam;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.FixedTupleSpout;
import org.apache.storm.testing.MkClusterParam;
import org.apache.storm.testing.MkTupleParam;
import org.apache.storm.testing.MockedSources;
import org.apache.storm.testing.TestJob;
import org.apache.storm.testing.TrackedTopology;
import org.apache.storm.testing.TupleCaptureBolt;
import org.apache.storm.thrift.TException;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.RegisteredGlobalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Time.SimulatedTime;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility that helps with testing topologies, Bolts and Spouts.
 */
public class Testing {
    /**
     * The default amount of wall time should be spent waiting for
     * specific conditions to happen.  Default is 10 seconds unless
     * the environment variable STORM_TEST_TIMEOUT_MS is set.
     */
    public static final int TEST_TIMEOUT_MS;
    private static final Logger LOG = LoggerFactory.getLogger(Testing.class);

    static {
        int timeout = 10_000;
        try {
            timeout = Integer.parseInt(System.getenv("STORM_TEST_TIMEOUT_MS"));
        } catch (Exception e) {
            //Ignored, will go with default timeout
        }
        TEST_TIMEOUT_MS = timeout;
    }

    /**
     * Continue to execute body repeatedly until condition is true or TEST_TIMEOUT_MS has
     * passed.
     * @param condition what we are waiting for
     * @param body what to run in the loop
     * @throws AssertionError if the loop timed out.
     */
    public static void whileTimeout(Condition condition, Runnable body) {
        whileTimeout(TEST_TIMEOUT_MS, condition, body);
    }

    /**
     * Continue to execute body repeatedly until condition is true or TEST_TIMEOUT_MS has
     * passed.
     * @param timeoutMs the number of ms to wait before timing out.
     * @param condition what we are waiting for
     * @param body what to run in the loop
     * @throws AssertionError if the loop timed out.
     */
    public static void whileTimeout(long timeoutMs, Condition condition, Runnable body) {
        long endTime = System.currentTimeMillis() + timeoutMs;
        LOG.debug("Looping until {}", condition);
        int count = 0;
        while (condition.exec()) {
            count++;
            if (System.currentTimeMillis() > endTime) {
                LOG.info("Condition {} not met in {} ms after calling {} times", condition, timeoutMs, count);
                LOG.info(Utils.threadDump());
                throw new AssertionError("Test timed out (" + timeoutMs + "ms) " + condition);
            }
            body.run();
        }
        LOG.debug("Condition met {}", condition);
    }

    /**
     * Convenience method for data.stream.allMatch(pred).
     */
    public static <T> boolean isEvery(Collection<T> data, Predicate<T> pred) {
        return data.stream().allMatch(pred);
    }

    /**
     * Run with simulated time.
     *
     * @deprecated use ```
     *     try (Time.SimulatedTime time = new Time.SimulatedTime()) {
     *      ...
     *     }
     *     ```
     * @param code what to run
     */
    @Deprecated
    public static void withSimulatedTime(Runnable code) {
        try (SimulatedTime st = new SimulatedTime()) {
            code.run();
        }
    }

    private static LocalCluster cluster(MkClusterParam param, boolean simulated) throws Exception {
        return cluster(param, null, simulated);
    }

    private static LocalCluster cluster(MkClusterParam param) throws Exception {
        return cluster(param, null, false);
    }

    private static LocalCluster cluster(MkClusterParam param, String id, boolean simulated) throws Exception {
        Integer supervisors = param.getSupervisors();
        if (supervisors == null) {
            supervisors = 2;
        }
        Integer ports = param.getPortsPerSupervisor();
        if (ports == null) {
            ports = 3;
        }
        Map<String, Object> conf = param.getDaemonConf();
        if (conf == null) {
            conf = new HashMap<>();
        }
        return new LocalCluster.Builder()
                .withSupervisors(supervisors)
                .withPortsPerSupervisor(ports)
                .withDaemonConf(conf)
                .withNimbusDaemon(param.isNimbusDaemon())
                .withTracked(id)
                .withSimulatedTime(simulated)
                .build();
    }

    /**
     * Run with a local cluster.
     *
     * @deprecated use ```
     *     try (LocalCluster cluster = new LocalCluster()) {
     *      ...
     *     }
     *     ```
     * @param code what to run
     */
    @Deprecated
    public static void withLocalCluster(TestJob code) {
        withLocalCluster(new MkClusterParam(), code);
    }

    /**
     * Run with a local cluster.
     *
     * @deprecated use ```
     *     try (LocalCluster cluster = new LocalCluster.Builder()....build()) {
     *      ...
     *     }
     *     ```
     * @param param configs to set in the cluster
     * @param code what to run
     */
    @Deprecated
    public static void withLocalCluster(MkClusterParam param, TestJob code) {
        try (LocalCluster lc = cluster(param)) {
            code.run(lc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Run with a local cluster.
     *
     * @deprecated use ```
     *     try (LocalCluster cluster = new LocalCluster.Builder()....build()) {
     *      ...
     *     }
     *     ```
     * @param clusterConf some configs to set in the cluster
     */
    @Deprecated
    public static ILocalCluster getLocalCluster(Map<String, Object> clusterConf) {
        @SuppressWarnings("unchecked")
        Map<String, Object> conf = (Map<String, Object>) clusterConf.get("daemon-conf");
        if (conf == null) {
            conf = new HashMap<>();
        }
        Number supervisors = (Number) clusterConf.getOrDefault("supervisors", 2);
        Number ports = (Number) clusterConf.getOrDefault("ports-per-supervisor", 3);
        INimbus inimbus = (INimbus) clusterConf.get("inimbus");
        Number portMin = (Number) clusterConf.getOrDefault("supervisor-slot-port-min", 1024);
        Boolean nimbusDaemon = (Boolean) clusterConf.getOrDefault("nimbus-daemon", false);
        try {
            return new LocalCluster.Builder()
                .withSupervisors(supervisors.intValue())
                .withDaemonConf(conf)
                .withPortsPerSupervisor(ports.intValue())
                .withINimbus(inimbus)
                .withSupervisorSlotPortMin(portMin)
                .withNimbusDaemon(nimbusDaemon)
                .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Run with a local cluster.
     *
     * @deprecated use ```
     *     try (LocalCluster cluster = new LocalCluster.Builder().withSimulatedTime().build()) {
     *      ...
     *     }
     *     ```
     * @param code what to run
     */
    @Deprecated
    public static void withSimulatedTimeLocalCluster(TestJob code) {
        withSimulatedTimeLocalCluster(new MkClusterParam(), code);
    }

    /**
     * Run with a local cluster.
     *
     * @deprecated use ```
     *     try (LocalCluster cluster = new LocalCluster.Builder().withSimulatedTime()....build()) {
     *      ...
     *     }
     *     ```
     * @param param configs to set in the cluster
     * @param code what to run
     */
    @Deprecated
    public static void withSimulatedTimeLocalCluster(MkClusterParam param, TestJob code) {
        try (LocalCluster lc = cluster(param, true)) {
            code.run(lc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Run with a local cluster.
     *
     * @deprecated use ```
     *     try (LocalCluster cluster = new LocalCluster.Builder().withTracked().build()) {
     *      ...
     *     }
     *     ```
     * @param code what to run
     */
    @Deprecated
    public static void withTrackedCluster(TestJob code) {
        withTrackedCluster(new MkClusterParam(), code);
    }

    /**
     * Run with a local tracked cluster.
     *
     * @deprecated use ```
     *     try (LocalCluster cluster = new LocalCluster.Builder().withTracked()....build()) {
     *      ...
     *     }
     *     ```
     * @param param configs to set in the cluster
     * @param code what to run
     */
    @Deprecated
    public static void withTrackedCluster(MkClusterParam param, TestJob code) {
        try (LocalCluster lc = cluster(param, Utils.uuid(), true)) {
            code.run(lc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * In a tracked topology some metrics are tracked.  This provides a way to get those metrics.
     * This is intended mostly for internal testing.
     *
     * @param id the id of the tracked cluster
     * @param key the name of the metric to get.
     * @return the metric
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public static int globalAmt(String id, String key) {
        LOG.warn("Reading tracked metrics for ID {}", id);
        return ((ConcurrentHashMap<String, AtomicInteger>) RegisteredGlobalState.getState(id)).get(key).get();
    }

    /**
     * Track and capture a topology.
     * This is intended mostly for internal testing.
     */
    public static CapturedTopology<TrackedTopology> trackAndCaptureTopology(ILocalCluster cluster, StormTopology topology) {
        CapturedTopology<StormTopology> captured = captureTopology(topology);
        return new CapturedTopology<>(new TrackedTopology(captured.topology, cluster), captured.capturer);
    }

    /**
     * Rewrites a topology so that all the tuples flowing through it are captured.
     * @param topology the topology to rewrite
     * @return the modified topology and a new Bolt that can retrieve the
     *     captured tuples.
     */
    public static CapturedTopology<StormTopology> captureTopology(StormTopology topology) {
        topology = topology.deepCopy(); //Don't modify the original

        TupleCaptureBolt capturer = new TupleCaptureBolt();
        Map<GlobalStreamId, Grouping> captureBoltInputs = new HashMap<>();
        for (Map.Entry<String, SpoutSpec> spoutEntry : topology.get_spouts().entrySet()) {
            String id = spoutEntry.getKey();
            for (Entry<String, StreamInfo> streamEntry : spoutEntry.getValue().get_common().get_streams().entrySet()) {
                String stream = streamEntry.getKey();
                StreamInfo info = streamEntry.getValue();
                if (info.is_direct()) {
                    captureBoltInputs.put(new GlobalStreamId(id, stream), Thrift.prepareDirectGrouping());
                } else {
                    captureBoltInputs.put(new GlobalStreamId(id, stream), Thrift.prepareGlobalGrouping());
                }
            }
        }

        for (Entry<String, Bolt> boltEntry : topology.get_bolts().entrySet()) {
            String id = boltEntry.getKey();
            for (Entry<String, StreamInfo> streamEntry : boltEntry.getValue().get_common().get_streams().entrySet()) {
                String stream = streamEntry.getKey();
                StreamInfo info = streamEntry.getValue();
                if (info.is_direct()) {
                    captureBoltInputs.put(new GlobalStreamId(id, stream), Thrift.prepareDirectGrouping());
                } else {
                    captureBoltInputs.put(new GlobalStreamId(id, stream), Thrift.prepareGlobalGrouping());
                }
            }
        }
        topology.put_to_bolts(Utils.uuid(), new Bolt(Thrift.serializeComponentObject(capturer),
                                                     Thrift.prepareComponentCommon(captureBoltInputs, new HashMap<>(), null)));
        return new CapturedTopology<>(topology, capturer);
    }

    /**
     * Run a topology to completion capturing all of the messages that are emitted.  This only works when all of the spouts are
     * instances of {@link org.apache.storm.testing.CompletableSpout}.
     * @param cluster the cluster to submit the topology to
     * @param topology the topology itself
     * @return a map of the component to the list of tuples it emitted
     * @throws TException on any error from nimbus
     */
    public static Map<String, List<FixedTuple>> completeTopology(ILocalCluster cluster, StormTopology topology) throws InterruptedException,
        TException {
        return completeTopology(cluster, topology, new CompleteTopologyParam());
    }

    /**
     * Run a topology to completion capturing all of the messages that are emitted.  This only works when all of the spouts are
     * instances of {@link org.apache.storm.testing.CompletableSpout} or are overwritten by MockedSources in param
     * @param cluster the cluster to submit the topology to
     * @param topology the topology itself
     * @param param parameters to describe how to complete a topology
     * @return a map of the component to the list of tuples it emitted
     * @throws TException on any error from nimbus.
     */
    public static Map<String, List<FixedTuple>> completeTopology(ILocalCluster cluster, StormTopology topology,
                                                                 CompleteTopologyParam param) throws TException, InterruptedException {
        Map<String, List<FixedTuple>> ret = null;
        CapturedTopology<StormTopology> capTopo = captureTopology(topology);
        topology = capTopo.topology;
        String topoName = param.getTopologyName();
        if (topoName == null) {
            topoName = "topologytest-" + Utils.uuid();
        }

        Map<String, SpoutSpec> spouts = topology.get_spouts();
        MockedSources ms = param.getMockedSources();
        if (ms != null) {
            for (Entry<String, List<FixedTuple>> mocked : ms.getData().entrySet()) {
                FixedTupleSpout newSpout = new FixedTupleSpout(mocked.getValue());
                spouts.get(mocked.getKey()).set_spout_object(Thrift.serializeComponentObject(newSpout));
            }
        }
        List<Object> spoutObjects = spouts.values()
                .stream()
                .map((spec) -> Thrift.deserializeComponentObject(spec.get_spout_object()))
                .collect(Collectors.toList());

        for (Object o : spoutObjects) {
            if (!(o instanceof CompletableSpout)) {
                throw new RuntimeException(
                    "Cannot complete topology unless every spout is a CompletableSpout (or mocked to be); failed by " + o);
            }
        }

        for (Object spout : spoutObjects) {
            ((CompletableSpout) spout).startup();
        }

        cluster.submitTopology(topoName, param.getStormConf(), topology);

        if (Time.isSimulating()) {
            cluster.advanceClusterTime(11);
        }

        IStormClusterState state = cluster.getClusterState();
        String topoId = state.getTopoId(topoName).get();
        //Give the topology time to come up without using it to wait for the spouts to complete
        simulateWait(cluster);
        Integer timeoutMs = param.getTimeoutMs();
        if (timeoutMs == null) {
            timeoutMs = TEST_TIMEOUT_MS;
        }
        whileTimeout(timeoutMs,
            () -> !isEvery(spoutObjects, (o) -> ((CompletableSpout) o).isExhausted()),
            () -> {
                try {
                    simulateWait(cluster);
                } catch (Exception e) {
                    throw new RuntimeException();
                }
            });

        KillOptions killOpts = new KillOptions();
        killOpts.set_wait_secs(0);
        cluster.killTopologyWithOpts(topoName, killOpts);

        whileTimeout(timeoutMs,
            () -> state.assignmentInfo(topoId, null) != null,
            () -> {
                try {
                    simulateWait(cluster);
                } catch (Exception e) {
                    throw new RuntimeException();
                }
            });

        if (param.getCleanupState()) {
            for (Object o : spoutObjects) {
                ((CompletableSpout) o).clean();
            }
            ret = capTopo.capturer.getAndRemoveResults();
        } else {
            ret = capTopo.capturer.getAndClearResults();
        }

        return ret;
    }

    /**
     * If using simulated time simulate waiting for 10 seconds. This is intended for internal testing only.
     */
    public static void simulateWait(ILocalCluster cluster) throws InterruptedException {
        if (Time.isSimulating()) {
            cluster.advanceClusterTime(10);
            Thread.sleep(100);
        }
    }

    /**
     * Get all of the tuples from a given component on the default stream.
     * @param results the results of running a completed topology
     * @param componentId the id of the component to look at
     * @return a list of the tuple values.
     */
    public static List<List<Object>> readTuples(Map<String, List<FixedTuple>> results, String componentId) {
        return readTuples(results, componentId, Utils.DEFAULT_STREAM_ID);
    }

    /**
     * Get all of the tuples from a given component on a given stream.
     * @param results the results of running a completed topology
     * @param componentId the id of the component to look at
     * @param streamId the id of the stream to look for.
     * @return a list of the tuple values.
     */
    public static List<List<Object>> readTuples(Map<String, List<FixedTuple>> results, String componentId, String streamId) {
        List<List<Object>> ret = new ArrayList<>();
        List<FixedTuple> streamResult = results.get(componentId);
        if (streamResult != null) {
            for (FixedTuple tuple : streamResult) {
                if (streamId.equals(tuple.stream)) {
                    ret.add(tuple.values);
                }
            }
        }
        return ret;
    }

    /**
     * Create a tracked topology.
     * @deprecated use {@link org.apache.storm.testing.TrackedTopology} directly.
     */
    @Deprecated
    public static TrackedTopology mkTrackedTopology(ILocalCluster cluster, StormTopology topology) {
        return new TrackedTopology(topology, cluster);
    }

    /**
     * Simulated time wait for a tracked topology.  This is intended for internal testing.
     */
    public static void trackedWait(CapturedTopology<TrackedTopology> topo) {
        topo.topology.trackedWait();
    }

    /**
     * Simulated time wait for a tracked topology.  This is intended for internal testing.
     */
    public static void trackedWait(CapturedTopology<TrackedTopology> topo, Integer amt) {
        topo.topology.trackedWait(amt);
    }

    /**
     * Simulated time wait for a tracked topology.  This is intended for internal testing.
     */
    public static void trackedWait(CapturedTopology<TrackedTopology> topo, Integer amt, Integer timeoutMs) {
        topo.topology.trackedWait(amt, timeoutMs);
    }

    /**
     * Simulated time wait for a tracked topology.  This is intended for internal testing.
     */
    public static void trackedWait(TrackedTopology topo) {
        topo.trackedWait();
    }

    /**
     * Simulated time wait for a tracked topology.  This is intended for internal testing.
     */
    public static void trackedWait(TrackedTopology topo, Integer amt) {
        topo.trackedWait(amt);
    }

    /**
     * Simulated time wait for a tracked topology.  This is intended for internal testing.
     */
    public static void trackedWait(TrackedTopology topo, Integer amt, Integer timeoutMs) {
        topo.trackedWait(amt, timeoutMs);
    }

    /**
     * Simulated time wait for a cluster.  This is intended for internal testing.
     */
    public static void advanceClusterTime(ILocalCluster cluster, Integer secs) throws InterruptedException {
        advanceClusterTime(cluster, secs, 1);
    }

    /**
     * Simulated time wait for a cluster.  This is intended for internal testing.
     */
    public static void advanceClusterTime(ILocalCluster cluster, Integer secs, Integer step) throws InterruptedException {
        cluster.advanceClusterTime(secs, step);
    }

    /**
     * Count how many times each element appears in the Collection.
     * @param c a collection of values
     * @return a map of the unique values in c to the count of those values.
     */
    public static <T> Map<T, Integer> multiset(Collection<T> c) {
        Map<T, Integer> ret = new HashMap<T, Integer>();
        for (T t : c) {
            Integer i = ret.get(t);
            if (i == null) {
                i = new Integer(0);
            }
            i += 1;
            ret.put(t, i);
        }
        return ret;
    }

    private static void printRec(Object o, String prefix) {
        if (o instanceof Collection) {
            LOG.info("{} {} ({}) [", prefix, o, o.getClass());
            for (Object sub : (Collection) o) {
                printRec(sub, prefix + "  ");
            }
            LOG.info("{} ]", prefix);
        } else if (o instanceof Map) {
            Map<?, ?> m = (Map<?, ?>) o;
            LOG.info("{} {} ({}) {", prefix, o, o.getClass());
            for (Map.Entry<?, ?> entry : m.entrySet()) {
                printRec(entry.getKey(), prefix + "  ");
                LOG.info("{} ->", prefix);
                printRec(entry.getValue(), prefix + "  ");
            }
            LOG.info("{} }", prefix);
        } else {
            LOG.info("{} {} ({})", prefix, o, o.getClass());
        }
    }

    /**
     * Check if two collections are equivalent ignoring the order of elements.
     */
    public static <T> boolean multiseteq(Collection<T> a, Collection<T> b) {
        boolean ret = multiset(a).equals(multiset(b));
        if (!ret) {
            printRec(multiset(a), "MS-A:");
            printRec(multiset(b), "MS-B:");
        }
        return ret;
    }

    /**
     * Create a {@link org.apache.storm.tuple.Tuple} for use with testing.
     * @param values the values to appear in the tuple
     */
    public static Tuple testTuple(List<Object> values) {
        return testTuple(values, new MkTupleParam());
    }

    /**
     * Create a {@link org.apache.storm.tuple.Tuple} for use with testing.
     * @param values the values to appear in the tuple
     * @param param parametrs describing more details about the tuple
     */
    public static Tuple testTuple(List<Object> values, MkTupleParam param) {
        String stream = param.getStream();
        if (stream == null) {
            stream = Utils.DEFAULT_STREAM_ID;
        }

        String component = param.getComponent();
        if (component == null) {
            component = "component";
        }

        int task = 1;

        List<String> fields = param.getFields();
        if (fields == null) {
            fields = new ArrayList<>(values.size());
            for (int i = 1; i <= values.size(); i++) {
                fields.add("field" + i);
            }
        }

        Map<Integer, String> taskToComp = new HashMap<>();
        taskToComp.put(task, component);
        Map<String, Map<String, Fields>> compToStreamToFields = new HashMap<>();
        Map<String, Fields> streamToFields = new HashMap<>();
        streamToFields.put(stream, new Fields(fields));
        compToStreamToFields.put(component, streamToFields);

        TopologyContext context = new TopologyContext(null,
            ConfigUtils.readStormConfig(),
            taskToComp,
            null,
            compToStreamToFields,
            null,
            "test-storm-id",
            null,
            null,
            1,
            null,
            null,
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            new HashMap<>(),
            new AtomicBoolean(false),
            null);
        return new TupleImpl(context, values, component, 1, stream);
    }

    /**
     * Simply produces a boolean to see if a specific state is true or false.
     */
    public interface Condition {
        boolean exec();
    }

    /**
     * A topology that has all messages captured and can be read later on.
     * This is intended mostly for internal testing.
     * @param <T> the topology (tracked or regular)
     */
    public static final class CapturedTopology<T> {
        public final T topology;
        /**
         * a Bolt that will hold all of the captured data.
         */
        public final TupleCaptureBolt capturer;

        public CapturedTopology(T topology, TupleCaptureBolt capturer) {
            this.topology = topology;
            this.capturer = capturer;
        }
    }
}

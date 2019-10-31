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

package org.apache.storm.starter;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.SharedOffHeapWithinNode;
import org.apache.storm.topology.SharedOnHeap;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ResourceAwareExampleTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //A topology can set resources in terms of CPU and Memory for each component
        // These can be chained (like with setting the CPU requirement)
        SpoutDeclarer spout = builder.setSpout("word", new TestWordSpout(), 10).setCPULoad(20);
        // Or done separately like with setting the
        // onheap and offheap memory requirement
        spout.setMemoryLoad(64, 16);
        //On heap memory is used to help calculate the heap of the java process for the worker
        // off heap memory is for things like JNI memory allocated off heap, or when using the
        // ShellBolt or ShellSpout.  In this case the 16 MB of off heap is just as an example
        // as we are not using it.

        // Some times a Bolt or Spout will have some memory that is shared between the instances
        // These are typically caches, but could be anything like a static database that is memory
        // mapped into the processes. These can be declared separately and added to the bolts and
        // spouts that use them.  Or if only one uses it they can be created inline with the add
        SharedOnHeap exclaimCache = new SharedOnHeap(100, "exclaim-cache");
        SharedOffHeapWithinNode notImplementedButJustAnExample =
            new SharedOffHeapWithinNode(500, "not-implemented-node-level-cache");

        //If CPU or memory is not set the values stored in topology.component.resources.onheap.memory.mb,
        // topology.component.resources.offheap.memory.mb and topology.component.cpu.pcore.percent
        // will be used instead
        builder
            .setBolt("exclaim1", new ExclamationBolt(), 3)
            .shuffleGrouping("word")
            .addSharedMemory(exclaimCache);

        builder
            .setBolt("exclaim2", new ExclamationBolt(), 2)
            .shuffleGrouping("exclaim1")
            .setMemoryLoad(100)
            .addSharedMemory(exclaimCache)
            .addSharedMemory(notImplementedButJustAnExample);

        Config conf = new Config();
        conf.setDebug(true);

        //Under RAS the number of workers is determined by the scheduler and the settings in the conf are ignored
        //conf.setNumWorkers(3);

        //Instead the scheduler lets you set the maximum heap size for any worker.
        conf.setTopologyWorkerMaxHeapSize(1024.0);
        //The scheduler generally will try to pack executors into workers until the max heap size is met, but
        // this can vary depending on the specific scheduling strategy selected.
        // The reason for this is to try and balance the maximum pause time GC might take (which is larger for larger heaps)
        // against better performance because of not needing to serialize/deserialize tuples.

        //The priority of a topology describes the importance of the topology in decreasing importance
        // starting from 0 (i.e. 0 is the highest priority and the priority importance decreases as the priority number increases).
        //Recommended range of 0-29 but no hard limit set.
        // If there are not enough resources in a cluster the priority in combination with how far over a guarantees
        // a user is will decide which topologies are run and which ones are not.
        conf.setTopologyPriority(29);

        //set to use the default resource aware strategy when using the MultitenantResourceAwareBridgeScheduler
        conf.setTopologyStrategy(
            "org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy");

        String topoName = "test";
        if (args != null && args.length > 0) {
            topoName = args[0];
        }

        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
    }

    public static class ExclamationBolt extends BaseRichBolt {
        //Have a crummy cache to show off shared memory accounting
        private static final ConcurrentHashMap<String, String> myCrummyCache =
            new ConcurrentHashMap<>();
        private static final int CACHE_SIZE = 100_000;
        OutputCollector collector;

        protected static String getFromCache(String key) {
            return myCrummyCache.get(key);
        }

        protected static void addToCache(String key, String value) {
            myCrummyCache.putIfAbsent(key, value);
            int numToRemove = myCrummyCache.size() - CACHE_SIZE;
            if (numToRemove > 0) {
                //Remove something randomly...
                Iterator<Entry<String, String>> it = myCrummyCache.entrySet().iterator();
                for (; numToRemove > 0 && it.hasNext(); numToRemove--) {
                    it.next();
                    it.remove();
                }
            }
        }

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            String orig = tuple.getString(0);
            String ret = getFromCache(orig);
            if (ret == null) {
                ret = orig + "!!!";
                addToCache(orig, ret);
            }
            collector.emit(tuple, new Values(ret));
            collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }
}

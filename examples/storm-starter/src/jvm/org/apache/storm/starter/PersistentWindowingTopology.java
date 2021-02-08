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

package org.apache.storm.starter;

import static org.apache.storm.topology.base.BaseWindowedBolt.Duration;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.starter.spout.RandomIntegerSpout;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.streams.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example that demonstrates the usage of {@link org.apache.storm.topology.IStatefulWindowedBolt} with window persistence.
 * <p>
 * The framework automatically checkpoints the tuples in the window along with the bolt's state and restores the same during restarts.
 * </p>
 *
 * <p>
 * This topology uses 'redis' for state persistence, so you should also start a redis instance before deploying. If you are running in local
 * mode you can just start a redis server locally which will be used for storing the state. The default RedisKeyValueStateProvider
 * parameters can be overridden by setting {@link Config#TOPOLOGY_STATE_PROVIDER_CONFIG}, for e.g.
 * <pre>
 * {
 *   "jedisPoolConfig": {
 *     "host": "redis-server-host",
 *     "port": 6379,
 *     "timeout": 2000,
 *     "database": 0,
 *     "password": "xyz"
 *   }
 * }
 * </pre>
 * </p>
 */
public class PersistentWindowingTopology {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentWindowingTopology.class);

    /**
     * Create and deploy the topology.
     *
     * @param args args
     * @throws Exception exception
     */
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        // generate random numbers
        builder.setSpout("spout", new RandomIntegerSpout());

        // emits sliding window and global averages
        builder.setBolt("avgbolt", new AvgBolt()
            .withWindow(new Duration(10, TimeUnit.SECONDS), new Duration(2, TimeUnit.SECONDS))
            // persist the window in state
            .withPersistence()
            // max number of events to be cached in memory
            .withMaxEventsInMemory(25000), 1)
               .shuffleGrouping("spout");

        // print the values to stdout
        builder.setBolt("printer", (x, y) -> System.out.println(x.getValue(0)), 1).shuffleGrouping("avgbolt");

        Config conf = new Config();
        conf.setDebug(false);

        // checkpoint the state every 5 seconds
        conf.put(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL, 5000);

        // use redis for state persistence
        conf.put(Config.TOPOLOGY_STATE_PROVIDER, "org.apache.storm.redis.state.RedisKeyValueStateProvider");

        String topoName = "test";
        if (args != null && args.length > 0) {
            topoName = args[0];
        }
        conf.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
    }

    // wrapper to hold global and window averages
    private static class Averages {
        private final double global;
        private final double window;

        Averages(double global, double window) {
            this.global = global;
            this.window = window;
        }

        @Override
        public String toString() {
            return "Averages{" + "global=" + String.format("%.2f", global) + ", window=" + String.format("%.2f", window) + '}';
        }
    }

    /**
     * A bolt that uses stateful persistence to store the windows along with the state (global avg).
     */
    private static class AvgBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Pair<Long, Long>>> {
        private static final String STATE_KEY = "avg";

        private OutputCollector collector;
        private KeyValueState<String, Pair<Long, Long>> state;
        private Pair<Long, Long> globalAvg;

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void initState(KeyValueState<String, Pair<Long, Long>> state) {
            this.state = state;
            globalAvg = state.get(STATE_KEY, Pair.of(0L, 0L));
            LOG.info("initState with global avg [" + (double) globalAvg.getFirst() / globalAvg.getSecond() + "]");
        }

        @Override
        public void execute(TupleWindow window) {
            int sum = 0;
            int count = 0;
            // iterate over tuples in the current window
            Iterator<Tuple> it = window.getIter();
            while (it.hasNext()) {
                Tuple tuple = it.next();
                sum += tuple.getInteger(0);
                ++count;
            }
            LOG.debug("Count : {}", count);
            globalAvg = Pair.of(globalAvg.getFirst() + sum, globalAvg.getSecond() + count);
            // update the value in state
            state.put(STATE_KEY, globalAvg);
            // emit the averages downstream
            collector.emit(new Values(new Averages((double) globalAvg.getFirst() / globalAvg.getSecond(), (double) sum / count)));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("avg"));
        }
    }

}

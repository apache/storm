/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.utils;

import java.util.HashSet;
import java.util.Map;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GetInfoOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NumErrorsChoice;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologySummary;

public class Monitor {
    private static final String WATCH_TRANSFERRED = "transferred";
    private static final String WATCH_EMITTED = "emitted";

    private int interval = 4;
    private String topology;
    private String component;
    private String stream;
    private String watch;

    private HashSet<String> getComponents(Nimbus.Iface client, String topology) throws Exception {
        HashSet<String> components = new HashSet<>();
        GetInfoOptions getInfoOpts = new GetInfoOptions();
        getInfoOpts.set_num_err_choice(NumErrorsChoice.NONE);
        TopologyInfo info = client.getTopologyInfoByNameWithOpts(topology, getInfoOpts);
        for (ExecutorSummary es : info.get_executors()) {
            components.add(es.get_component_id());
        }
        return components;
    }

    public void metrics(Nimbus.Iface client) throws Exception {
        if (interval <= 0) {
            throw new IllegalArgumentException("poll interval must be positive");
        }

        if (topology == null || topology.isEmpty()) {
            throw new IllegalArgumentException("topology name must be something");
        }

        if (component == null || component.isEmpty()) {
            HashSet<String> components = getComponents(client, topology);
            System.out.println("Available components for " + topology + " :");
            System.out.println("------------------");
            for (String comp : components) {
                System.out.println(comp);
            }
            System.out.println("------------------");
            System.out.println("Please use -m to specify one component");
            return;
        }

        if (stream == null || stream.isEmpty()) {
            throw new IllegalArgumentException("stream name must be something");
        }

        if (!WATCH_TRANSFERRED.equals(watch) && !WATCH_EMITTED.equals(watch)) {
            throw new IllegalArgumentException("watch item must either be transferred or emitted");
        }
        System.out.println("topology\tcomponent\tparallelism\tstream\ttime-diff ms\t" + watch + "\tthroughput (Kt/s)");

        long pollMs = interval * 1000;
        long now = System.currentTimeMillis();
        MetricsState state = new MetricsState(now, 0);
        Poller poller = new Poller(now, pollMs);

        do {
            metrics(client, now, state);
            try {
                now = poller.nextPoll();
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        } while (true);
    }

    public void metrics(Nimbus.Iface client, long now, MetricsState state) throws Exception {
        long totalStatted = 0;
        int componentParallelism = 0;
        boolean streamFound = false;
        GetInfoOptions getInfoOpts = new GetInfoOptions();
        getInfoOpts.set_num_err_choice(NumErrorsChoice.NONE);
        TopologyInfo info = client.getTopologyInfoByNameWithOpts(topology, getInfoOpts);
        for (ExecutorSummary es : info.get_executors()) {
            if (component.equals(es.get_component_id())) {
                componentParallelism++;
                ExecutorStats stats = es.get_stats();
                if (stats != null) {
                    Map<String, Map<String, Long>> statted = WATCH_EMITTED.equals(watch) ? stats.get_emitted() : stats.get_transferred();
                    if (statted != null) {
                        Map<String, Long> e2 = statted.get(":all-time");
                        if (e2 != null) {
                            Long stream = e2.get(this.stream);
                            if (stream != null) {
                                streamFound = true;
                                totalStatted += stream;
                            }
                        }
                    }
                }
            }
        }

        if (componentParallelism <= 0) {
            HashSet<String> components = getComponents(client, topology);
            System.out.println("Available components for " + topology + " :");
            System.out.println("------------------");
            for (String comp : components) {
                System.out.println(comp);
            }
            System.out.println("------------------");
            throw new IllegalArgumentException("component: " + component + " not found");
        }

        if (!streamFound) {
            throw new IllegalArgumentException("stream: " + stream + " not found");
        }
        long timeDelta = now - state.getLastTime();
        long stattedDelta = totalStatted - state.getLastStatted();
        state.setLastTime(now);
        state.setLastStatted(totalStatted);
        double throughput = (stattedDelta == 0 || timeDelta == 0) ? 0.0 : ((double) stattedDelta / (double) timeDelta);
        System.out.println(topology + "\t"
                    + component + "\t"
                    + componentParallelism + "\t"
                    + stream + "\t"
                    + timeDelta + "\t"
                    + stattedDelta + "\t"
                    + throughput);
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public void setTopology(String topology) {
        this.topology = topology;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public void setWatch(String watch) {
        this.watch = watch;
    }

    private static class MetricsState {
        private long lastTime = 0;
        private long lastStatted = 0;

        private MetricsState(long lastTime, long lastStatted) {
            this.lastTime = lastTime;
            this.lastStatted = lastStatted;
        }

        public long getLastStatted() {
            return lastStatted;
        }

        public void setLastStatted(long lastStatted) {
            this.lastStatted = lastStatted;
        }

        public long getLastTime() {
            return lastTime;
        }

        public void setLastTime(long lastTime) {
            this.lastTime = lastTime;
        }
    }

    private static class Poller {
        private long startTime = 0;
        private long pollMs = 0;

        private Poller(long startTime, long pollMs) {
            this.startTime = startTime;
            this.pollMs = pollMs;
        }

        public long nextPoll() throws InterruptedException {
            long now = System.currentTimeMillis();
            long cycle = (now - startTime) / pollMs;
            long wakeupTime = startTime + (pollMs * (cycle + 1));
            long sleepTime = wakeupTime - now;
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
            now = System.currentTimeMillis();
            return now;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getPollMs() {
            return pollMs;
        }

        public void setPollMs(long pollMs) {
            this.pollMs = pollMs;
        }
    }
}

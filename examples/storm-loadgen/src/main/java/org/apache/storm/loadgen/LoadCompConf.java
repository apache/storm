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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.utils.ObjectReader;

/**
 * Configuration for a simulated spout.
 */
public class LoadCompConf {
    public final String id;
    public final int parallelism;
    public final List<OutputStream> streams;
    public final double cpuLoad;
    public final double memoryLoad;

    /**
     * Parse the LoadCompConf from a config Map.
     * @param conf the map holding the config for a LoadCompConf.
     * @return the parsed object.
     */
    public static LoadCompConf fromConf(Map<String, Object> conf) {
        String id = (String) conf.get("id");
        int parallelism = ObjectReader.getInt(conf.get("parallelism"), 1);
        List<OutputStream> streams = new ArrayList<>();
        List<Map<String, Object>> streamData = (List<Map<String, Object>>) conf.get("streams");
        if (streamData != null) {
            for (Map<String, Object> streamInfo: streamData) {
                streams.add(OutputStream.fromConf(streamInfo));
            }
        }
        double memoryMb = ObjectReader.getDouble(conf.get("memoryLoad"), 0.0);
        double cpuPercent = ObjectReader.getDouble(conf.get("cpuLoad"), 0.0);

        return new LoadCompConf(id, parallelism, streams, memoryMb, cpuPercent);
    }

    /**
     * Build a config map for this object.
     * @return the config map.
     */
    public Map<String, Object> toConf() {
        Map<String, Object> ret = new HashMap<>();
        ret.put("id", id);
        ret.put("parallelism", parallelism);
        if (memoryLoad > 0) {
            ret.put("memoryLoad", memoryLoad);
        }
        if (cpuLoad > 0) {
            ret.put("cpuLoad", cpuLoad);
        }

        if (streams != null) {
            List<Map<String, Object>> streamData = new ArrayList<>();
            for (OutputStream out : streams) {
                streamData.add(out.toConf());
            }
            ret.put("streams", streamData);
        }
        return ret;
    }

    /**
     * Chenge the name of components and streams according to the parameters passed in.
     * @param remappedComponents original component name to new component name.
     * @param remappedStreams original stream id to new stream id.
     * @return a copy of this with the values remapped.
     */
    public LoadCompConf remap(Map<String, String> remappedComponents, Map<GlobalStreamId, GlobalStreamId> remappedStreams) {
        String remappedId = remappedComponents.get(id);
        List<OutputStream> remappedOutStreams = (streams == null) ? null :
            streams.stream()
                .map((orig) -> orig.remap(id, remappedStreams))
                .collect(Collectors.toList());

        return new LoadCompConf(remappedId, parallelism, remappedOutStreams, cpuLoad, memoryLoad);
    }

    /**
     * Scale the parallelism of this component by v.  The aggregate throughput will be the same.
     * The parallelism will be rounded up to the next largest whole number.  Parallelism will always be at least 1.
     * @param v 1.0 is not change 0.5 is drop the parallelism by half.
     * @return a copy of this with the parallelism adjusted.
     */
    public LoadCompConf scaleParallel(double v) {
        return setParallel(Math.max(1, (int)Math.ceil(parallelism * v)));
    }

    /**
     * Set the parallelism of this component, and adjust the throughput so in aggregate it stays the same.
     * @param newParallelism the new parallelism to set.
     * @return a copy of this with the adjustments made.
     */
    public LoadCompConf setParallel(int newParallelism) {
        //We need to adjust the throughput accordingly (so that it stays the same in aggregate)
        double throughputAdjustment = ((double)parallelism) / newParallelism;
        return new LoadCompConf(id, newParallelism, streams, cpuLoad, memoryLoad).scaleThroughput(throughputAdjustment);
    }

    /**
     * Scale the throughput of this component.
     * @param v 1.0 is unchanged 0.5 will cut the throughput in half.
     * @return a copu of this with the adjustments made.
     */
    public LoadCompConf scaleThroughput(double v) {
        if (streams != null) {
            List<OutputStream> newStreams = streams.stream().map((s) -> s.scaleThroughput(v)).collect(Collectors.toList());
            return new LoadCompConf(id, parallelism, newStreams, cpuLoad, memoryLoad);
        } else {
            return this;
        }
    }

    /**
     * Compute the total amount of all messages emitted in all streams per second.
     * @return the sum of all messages emitted per second.
     */
    public double getAllEmittedAggregate() {
        double ret = 0;
        if (streams != null) {
            for (OutputStream out: streams) {
                if (out.rate != null) {
                    ret += out.rate.mean * parallelism;
                }
            }
        }
        return ret;
    }

    public static class Builder {
        private String id;
        private int parallelism = 1;
        private List<OutputStream> streams;
        private double cpuLoad = 0.0;
        private double memoryLoad = 0.0;

        public String getId() {
            return id;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public int getParallelism() {
            return parallelism;
        }

        public Builder withParallelism(int parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        public List<OutputStream> getStreams() {
            return streams;
        }

        /**
         * Add in a single OutputStream to this component.
         * @param stream the stream to add
         * @return this
         */
        public Builder withStream(OutputStream stream) {
            if (streams == null) {
                streams = new ArrayList<>();
            }
            streams.add(stream);
            return this;
        }

        public Builder withStreams(List<OutputStream> streams) {
            this.streams = streams;
            return this;
        }

        public Builder withCpuLoad(double cpuLoad) {
            this.cpuLoad = cpuLoad;
            return this;
        }

        public Builder withMemoryLoad(double memoryLoad) {
            this.memoryLoad = memoryLoad;
            return this;
        }

        public LoadCompConf build() {
            return new LoadCompConf(id, parallelism, streams, cpuLoad, memoryLoad);
        }
    }

    /**
     * Create a new LoadCompConf with the given values.
     * @param id the id of the component.
     * @param parallelism tha parallelism of the component.
     * @param streams the output streams of the component.
     */
    public LoadCompConf(String id, int parallelism, List<OutputStream> streams, double cpuLoad, double memoryLoad) {
        this.id = id;
        if (id == null) {
            throw new IllegalArgumentException("A spout ID cannot be null");
        }
        this.parallelism = parallelism;
        this.streams = streams;
        this.cpuLoad = cpuLoad;
        this.memoryLoad = memoryLoad;
    }
}

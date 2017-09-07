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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import jdk.nashorn.internal.objects.Global;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.loadgen.NormalDistStats;

/**
 * A set of measurements about a stream so we can statistically reproduce it.
 */
public class OutputStream implements Serializable {
    //The global stream id is this + the from component it must be a part of.
    public final String id;
    public final NormalDistStats rate;
    public final boolean areKeysSkewed;

    /**
     * Create an output stream from a config.
     * @param conf the config to read from.
     * @return the read OutputStream.
     */
    public static OutputStream fromConf(Map<String, Object> conf) {
        String streamId = (String) conf.getOrDefault("streamId", "default");
        NormalDistStats rate = NormalDistStats.fromConf((Map<String, Object>) conf.get("rate"));
        boolean areKeysSkewed = (Boolean) conf.getOrDefault("areKeysSkewed", false);
        return new OutputStream(streamId, rate, areKeysSkewed);
    }

    /**
     * Convert this to a conf.
     * @return the conf.
     */
    public Map<String, Object> toConf() {
        Map<String, Object> ret = new HashMap<>();
        ret.put("streamId", id);
        ret.put("rate", rate.toConf());
        ret.put("areKeysSkewed", areKeysSkewed);
        return ret;
    }

    public OutputStream remap(String origId, Map<GlobalStreamId, GlobalStreamId> remappedStreams) {
        GlobalStreamId remapped = remappedStreams.get(new GlobalStreamId(origId, id));
        return new OutputStream(remapped.get_streamId(), rate, areKeysSkewed);
    }

    public OutputStream scaleThroughput(double v) {
        return new OutputStream(id, rate.scaleBy(v), areKeysSkewed);
    }

    public static class Builder {
        private String id;
        private NormalDistStats rate;
        private boolean areKeysSkewed;

        public String getId() {
            return id;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public NormalDistStats getRate() {
            return rate;
        }

        public Builder withRate(NormalDistStats rate) {
            this.rate = rate;
            return this;
        }

        public boolean isAreKeysSkewed() {
            return areKeysSkewed;
        }

        public Builder withAreKeysSkewed(boolean areKeysSkewed) {
            this.areKeysSkewed = areKeysSkewed;
            return this;
        }

        public OutputStream build() {
            return new OutputStream(id, rate, areKeysSkewed);
        }
    }

    /**
     * Create a new stream with stats
     * @param id the id of the stream
     * @param rate the rate of tuples being emitted on this stream
     * @param areKeysSkewed true if keys are skewed else false.  For skewed keys
     *     we only simulate it by using a gaussian distribution to the keys instead
     *     of an even distribution.  Tere is no effort made right not to measure the
     *     skewness and reproduce it.
     */
    public OutputStream(String id, NormalDistStats rate, boolean areKeysSkewed) {
        this.id = id;
        this.rate = rate;
        this.areKeysSkewed = areKeysSkewed;
    }
}

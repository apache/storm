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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.grouping.PartialKeyGrouping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of measurements about a stream so we can statistically reproduce it.
 */
public class InputStream implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(InputStream.class);
    public final String fromComponent;
    public final String toComponent;
    public final String id;
    public final NormalDistStats execTime;
    public final NormalDistStats processTime;
    public final GroupingType groupingType;
    //Cached GlobalStreamId
    private GlobalStreamId gsid = null;

    /**
     * Create an output stream from a config.
     * @param conf the config to read from.
     * @return the read OutputStream.
     */
    public static InputStream fromConf(Map<String, Object> conf) {
        String component = (String) conf.get("from");
        String toComp = (String) conf.get("to");
        NormalDistStats execTime = NormalDistStats.fromConf((Map<String, Object>) conf.get("execTime"));
        NormalDistStats processTime = NormalDistStats.fromConf((Map<String, Object>) conf.get("processTime"));
        Map<String, Object> grouping = (Map<String, Object>) conf.get("grouping");
        GroupingType groupingType = GroupingType.fromConf((String) grouping.get("type"));
        String streamId = (String) grouping.getOrDefault("streamId", "default");
        return new InputStream(component, toComp, streamId, execTime, processTime, groupingType);
    }

    /**
     * Convert this to a conf.
     * @return the conf.
     */
    public Map<String, Object> toConf() {
        Map<String, Object> ret = new HashMap<>();
        ret.put("from", fromComponent);
        ret.put("to", toComponent);
        ret.put("execTime", execTime.toConf());
        ret.put("processTime", processTime.toConf());

        Map<String, Object> grouping = new HashMap<>();
        grouping.put("streamId", id);
        grouping.put("type", groupingType.toConf());
        ret.put("grouping", grouping);

        return ret;
    }

    public static class Builder {
        private String fromComponent;
        private String toComponent;
        private String id;
        private NormalDistStats execTime;
        private NormalDistStats processTime;
        private GroupingType groupingType = GroupingType.SHUFFLE;

        public String getFromComponent() {
            return fromComponent;
        }

        public Builder withFromComponent(String fromComponent) {
            this.fromComponent = fromComponent;
            return this;
        }

        public String getToComponent() {
            return toComponent;
        }

        public Builder withToComponent(String toComponent) {
            this.toComponent = toComponent;
            return this;
        }

        public String getId() {
            return id;
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public NormalDistStats getExecTime() {
            return execTime;
        }

        public Builder withExecTime(NormalDistStats execTime) {
            this.execTime = execTime;
            return this;
        }

        public NormalDistStats getProcessTime() {
            return processTime;
        }

        public Builder withProcessTime(NormalDistStats processTime) {
            this.processTime = processTime;
            return this;
        }

        public GroupingType getGroupingType() {
            return groupingType;
        }

        public Builder withGroupingType(GroupingType groupingType) {
            this.groupingType = groupingType;
            return this;
        }

        /**
         * Add the grouping type based off of the thrift Grouping class.
         * @param grouping the Grouping to extract the grouping type from
         * @return this
         */
        @SuppressWarnings("checkstyle:FallThrough")
        public Builder withGroupingType(Grouping grouping) {
            GroupingType group = GroupingType.SHUFFLE;
            Grouping._Fields thriftType = grouping.getSetField();

            switch (thriftType) {
                case FIELDS:
                    //Global Grouping is fields with an empty list
                    if (grouping.get_fields().isEmpty()) {
                        group = GroupingType.GLOBAL;
                    } else {
                        group = GroupingType.FIELDS;
                    }
                    break;
                case ALL:
                    group = GroupingType.ALL;
                    break;
                case NONE:
                    group = GroupingType.NONE;
                    break;
                case SHUFFLE:
                    group = GroupingType.SHUFFLE;
                    break;
                case LOCAL_OR_SHUFFLE:
                    group = GroupingType.LOCAL_OR_SHUFFLE;
                    break;
                case CUSTOM_SERIALIZED:
                    //This might be a partial key grouping..
                    byte[] data = grouping.get_custom_serialized();
                    try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                         ObjectInputStream ois = new ObjectInputStream(bis);) {
                        Object cg = ois.readObject();
                        if (cg instanceof PartialKeyGrouping) {
                            group = GroupingType.PARTIAL_KEY;
                            break;
                        }
                    } catch (Exception e) {
                        //ignored
                    }
                    //Fall through if not supported
                default:
                    LOG.warn("{} is not supported for replay of a topology.  Using SHUFFLE", thriftType);
                    break;
            }
            return withGroupingType(group);
        }

        public InputStream build() {
            return new InputStream(fromComponent, toComponent, id, execTime, processTime, groupingType);
        }
    }

    /**
     * Create a new input stream to a bolt.
     * @param fromComponent the source component of the stream.
     * @param id the id of the stream
     * @param execTime exec time stats
     * @param processTime process time stats
     */
    public InputStream(String fromComponent, String toComponent, String id, NormalDistStats execTime,
                       NormalDistStats processTime, GroupingType groupingType) {
        this.fromComponent = fromComponent;
        this.toComponent = toComponent;
        if (fromComponent == null) {
            throw new IllegalArgumentException("from cannot be null");
        }
        if (toComponent == null) {
            throw new IllegalArgumentException("to cannot be null");
        }
        this.id = id;
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        this.execTime = execTime;
        this.processTime = processTime;
        this.groupingType = groupingType;
        if (groupingType == null) {
            throw new IllegalArgumentException("grouping type cannot be null");
        }
    }

    /**
     * Get the global stream id for this input stream.
     * @return the GlobalStreamId for this input stream.
     */
    public synchronized GlobalStreamId gsid() {
        if (gsid == null) {
            gsid = new GlobalStreamId(fromComponent, id);
        }
        return gsid;
    }

    /**
     * Remap the names of components.
     * @param remappedComponents old name to new name of components.
     * @param remappedStreams old ID to new ID of streams.
     * @return a modified version of this with names remapped.
     */
    public InputStream remap(Map<String, String> remappedComponents, Map<GlobalStreamId, GlobalStreamId> remappedStreams) {
        String remapTo = remappedComponents.get(toComponent);
        String remapFrom = remappedComponents.get(fromComponent);
        GlobalStreamId remapStreamId = remappedStreams.get(gsid());
        return new InputStream(remapFrom, remapTo, remapStreamId.get_streamId(), execTime, processTime, groupingType);
    }

    /**
     * Replace all SHUFFLE groupings with LOCAL_OR_SHUFFLE.
     * @return a modified copy of this
     */
    public InputStream replaceShuffleWithLocalOrShuffle() {
        if (groupingType != GroupingType.SHUFFLE) {
            return this;
        }
        return new InputStream(fromComponent, toComponent, id, execTime, processTime, GroupingType.LOCAL_OR_SHUFFLE);
    }
}

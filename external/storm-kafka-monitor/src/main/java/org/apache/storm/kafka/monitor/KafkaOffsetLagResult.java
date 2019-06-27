/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.monitor;

import org.json.simple.JSONAware;

/**
 * Class representing the log head offsets, spout offsets and the lag for a topic.
 */
public class KafkaOffsetLagResult implements JSONAware {
    private String topic;
    private int partition;
    private long consumerCommittedOffset;
    private long logHeadOffset;
    private long lag;

    public KafkaOffsetLagResult(String topic, int parition, long consumerCommittedOffset, long logHeadOffset) {
        this.topic = topic;
        this.partition = parition;
        this.consumerCommittedOffset = consumerCommittedOffset;
        this.logHeadOffset = logHeadOffset;
        this.lag = this.logHeadOffset - this.consumerCommittedOffset;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getConsumerCommittedOffset() {
        return consumerCommittedOffset;
    }

    public long getLogHeadOffset() {
        return logHeadOffset;
    }

    public long getLag() {
        return lag;
    }

    @Override
    public String toString() {
        return "KafkaOffsetLagResult{"
                + "topic='" + topic + '\''
                + ", partition=" + partition
                + ", consumerCommittedOffset=" + consumerCommittedOffset
                + ", logHeadOffset=" + logHeadOffset
                + ", lag=" + lag
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KafkaOffsetLagResult that = (KafkaOffsetLagResult) o;

        if (partition != that.partition) {
            return false;
        }
        if (consumerCommittedOffset != that.consumerCommittedOffset) {
            return false;
        }
        if (logHeadOffset != that.logHeadOffset) {
            return false;
        }
        if (lag != that.lag) {
            return false;
        }
        return !(topic != null ? !topic.equals(that.topic) : that.topic != null);

    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + partition;
        result = 31 * result + (int) (consumerCommittedOffset ^ (consumerCommittedOffset >>> 32));
        result = 31 * result + (int) (logHeadOffset ^ (logHeadOffset >>> 32));
        result = 31 * result + (int) (lag ^ (lag >>> 32));
        return result;
    }

    @Override
    public String toJSONString() {
        return "{\"topic\":\"" + topic
                + "\",\"partition\":" + partition
                + ",\"consumerCommittedOffset\":" + consumerCommittedOffset
                + ",\"logHeadOffset\":" + logHeadOffset
                + ",\"lag\":" + lag
                + "}";
    }
}

/*******************************************************************************
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
 *******************************************************************************/

package org.apache.storm.kafka.monitor;

public class KafkaPartitionOffsetLag {
    private long consumerCommittedOffset;
    private long logHeadOffset;
    private long lag;

    public KafkaPartitionOffsetLag(long consumerCommittedOffset, long logHeadOffset) {
        this.consumerCommittedOffset = consumerCommittedOffset;
        this.logHeadOffset = logHeadOffset;
        this.lag = logHeadOffset - consumerCommittedOffset;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KafkaPartitionOffsetLag)) {
            return false;
        }

        KafkaPartitionOffsetLag that = (KafkaPartitionOffsetLag) o;

        if (getConsumerCommittedOffset() != that.getConsumerCommittedOffset()) {
            return false;
        }
        if (getLogHeadOffset() != that.getLogHeadOffset()) {
            return false;
        }
        return getLag() == that.getLag();

    }

    @Override
    public int hashCode() {
        int result = (int) (getConsumerCommittedOffset() ^ (getConsumerCommittedOffset() >>> 32));
        result = 31 * result + (int) (getLogHeadOffset() ^ (getLogHeadOffset() >>> 32));
        result = 31 * result + (int) (getLag() ^ (getLag() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        // JSONAware not working for nested element on Map so write JSON format from here
        return "{\"consumerCommittedOffset\": " + consumerCommittedOffset + ", "
                + "\"logHeadOffset\": " + logHeadOffset + ", "
                + "\"lag\": " + lag + "}";
    }
}

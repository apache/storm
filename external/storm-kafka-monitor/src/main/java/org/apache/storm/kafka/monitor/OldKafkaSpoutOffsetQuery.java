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

/**
 * Class representing information for querying kafka for log head offsets, spout offsets and the difference for old kafka spout
 */
public class OldKafkaSpoutOffsetQuery {
    private final String topic; //single topic or a wildcard topic
    private final String zkServers; //comma separated list of zk servers and port e.g. hostname1:2181, hostname2:2181
    private final String zkPath; //zk node prefix without topic/partition where committed offsets are stored
    private final boolean isWildCardTopic; //if the topic is a wildcard
    private final String brokersZkPath; //zk node prefix where kafka stores all broker information
    private final String partitions; //comma separated list of partitions corresponding to leaders below (for StaticHosts)
    private final String leaders; //comma separated list of leader brokers and port corresponding to the partitions above (for StaticHosts) e.g.
    // hostname1:9092,hostname2:9092

    public OldKafkaSpoutOffsetQuery(String topic, String zkServers, String zkPath, boolean isWildCardTopic, String brokersZkPath) {
        this(topic, zkServers, zkPath, isWildCardTopic, brokersZkPath, null, null);
    }

    public OldKafkaSpoutOffsetQuery(String topic, String zkServers, String zkPath, String partitions, String leaders) {
        this(topic, zkServers, zkPath, false, null, partitions, leaders);

    }

    private OldKafkaSpoutOffsetQuery(String topic, String zkServers, String zkPath, boolean isWildCardTopic, String brokersZkPath, String partitions, String
            leaders) {
        this.topic = topic;
        this.zkServers = zkServers;
        this.zkPath = zkPath;
        this.isWildCardTopic = isWildCardTopic;
        this.brokersZkPath = brokersZkPath;
        this.partitions = partitions;
        this.leaders = leaders;
    }

    @Override
    public String toString() {
        return "OldKafkaSpoutOffsetQuery{" +
                "topic='" + topic + '\'' +
                ", zkServers='" + zkServers + '\'' +
                ", zkPath='" + zkPath + '\'' +
                ", isWildCardTopic=" + isWildCardTopic +
                ", brokersZkPath='" + brokersZkPath + '\'' +
                ", partitions='" + partitions + '\'' +
                ", leaders='" + leaders + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OldKafkaSpoutOffsetQuery that = (OldKafkaSpoutOffsetQuery) o;

        if (isWildCardTopic != that.isWildCardTopic) return false;
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
        if (zkServers != null ? !zkServers.equals(that.zkServers) : that.zkServers != null) return false;
        if (zkPath != null ? !zkPath.equals(that.zkPath) : that.zkPath != null) return false;
        if (brokersZkPath != null ? !brokersZkPath.equals(that.brokersZkPath) : that.brokersZkPath != null) return false;
        if (partitions != null ? !partitions.equals(that.partitions) : that.partitions != null) return false;
        return !(leaders != null ? !leaders.equals(that.leaders) : that.leaders != null);

    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + (zkServers != null ? zkServers.hashCode() : 0);
        result = 31 * result + (zkPath != null ? zkPath.hashCode() : 0);
        result = 31 * result + (isWildCardTopic ? 1 : 0);
        result = 31 * result + (brokersZkPath != null ? brokersZkPath.hashCode() : 0);
        result = 31 * result + (partitions != null ? partitions.hashCode() : 0);
        result = 31 * result + (leaders != null ? leaders.hashCode() : 0);
        return result;
    }

    public String getTopic() {

        return topic;
    }

    public String getZkServers() {
        return zkServers;
    }

    public String getZkPath() {
        return zkPath;
    }

    public boolean isWildCardTopic() {
        return isWildCardTopic;
    }

    public String getBrokersZkPath() {
        return brokersZkPath;
    }

    public String getPartitions() {
        return partitions;
    }

    public String getLeaders() {
        return leaders;
    }

}

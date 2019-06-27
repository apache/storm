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
 * Class representing information for querying kafka for log head offsets, consumer offsets and the difference for new
 * kafka spout using new consumer api.
 */
public class NewKafkaSpoutOffsetQuery {
    private final String topics; // comma separated list of topics
    private final String consumerGroupId; // consumer group id for which the offset needs to be calculated
    private final String bootStrapBrokers; // bootstrap brokers
    private final String securityProtocol; // security protocol to connect to kafka
    private final String consumerPropertiesFileName; // properties file containing additional kafka consumer configs

    public NewKafkaSpoutOffsetQuery(String topics, String bootstrapBrokers, String consumerGroupId, String securityProtocol,
        String consumerPropertiesFileName) {
        this.topics = topics;
        this.bootStrapBrokers = bootstrapBrokers;
        this.consumerGroupId = consumerGroupId;
        this.securityProtocol = securityProtocol;
        this.consumerPropertiesFileName = consumerPropertiesFileName;
    }

    public String getTopics() {
        return topics;
    }

    public String getBootStrapBrokers() {
        return bootStrapBrokers;
    }

    public String getConsumerGroupId() {
        return this.consumerGroupId;
    }

    public String getSecurityProtocol() {
        return this.securityProtocol;
    }

    public String getConsumerPropertiesFileName() {
        return this.consumerPropertiesFileName;
    }

    @Override
    public String toString() {
        return "NewKafkaSpoutOffsetQuery{"
                + "topics='" + topics + '\''
                + ", consumerGroupId='" + consumerGroupId + '\''
                + ", bootStrapBrokers='" + bootStrapBrokers + '\''
                + ", securityProtocol='" + securityProtocol + '\''
                + ", consumerPropertiesFileName='" + consumerPropertiesFileName + '\''
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

        NewKafkaSpoutOffsetQuery that = (NewKafkaSpoutOffsetQuery) o;

        if (topics != null ? !topics.equals(that.topics) : that.topics != null) {
            return false;
        }
        if (consumerGroupId != null ? !consumerGroupId.equals(that.consumerGroupId)
                                    : that.consumerGroupId != null) {
            return false;
        }
        if (bootStrapBrokers != null ? !bootStrapBrokers.equals(that.bootStrapBrokers)
                                     : that.bootStrapBrokers != null) {
            return false;
        }
        if (securityProtocol != null ? !securityProtocol.equals(that.securityProtocol)
                                     : that.securityProtocol != null) {
            return false;
        }
        return consumerPropertiesFileName != null ? consumerPropertiesFileName
            .equals(that.consumerPropertiesFileName) : that.consumerPropertiesFileName == null;
    }

    @Override
    public int hashCode() {
        int result = topics != null ? topics.hashCode() : 0;
        result = 31 * result + (consumerGroupId != null ? consumerGroupId.hashCode() : 0);
        result = 31 * result + (bootStrapBrokers != null ? bootStrapBrokers.hashCode() : 0);
        result = 31 * result + (securityProtocol != null ? securityProtocol.hashCode() : 0);
        result = 31 * result + (consumerPropertiesFileName != null ? consumerPropertiesFileName.hashCode() : 0);
        return result;
    }
}

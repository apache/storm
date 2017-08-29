/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.elasticsearch.common;

import org.elasticsearch.common.settings.Settings;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * @since 0.11
 */
public class EsConfig implements Serializable {

    private final String clusterName;
    private final String[] nodes;
    private final Map<String, String> additionalConfiguration;

    /**
     * EsConfig Constructor to be used in EsIndexBolt, EsPercolateBolt and EsStateFactory
     *
     * @param clusterName Elasticsearch cluster name
     * @param nodes       Elasticsearch addresses in host:port pattern string array
     * @throws IllegalArgumentException if nodes are empty
     * @throws NullPointerException     on any of the fields being null
     */
    public EsConfig(String clusterName, String[] nodes) {
        this(clusterName, nodes, Collections.<String, String>emptyMap());
    }

    /**
     * EsConfig Constructor to be used in EsIndexBolt, EsPercolateBolt and EsStateFactory
     *
     * @param clusterName             Elasticsearch cluster name
     * @param nodes                   Elasticsearch addresses in host:port pattern string array
     * @param additionalConfiguration Additional Elasticsearch configuration
     * @throws IllegalArgumentException if nodes are empty
     * @throws NullPointerException     on any of the fields being null
     */
    public EsConfig(String clusterName, String[] nodes, Map<String, String> additionalConfiguration) {
        this.clusterName = checkNotNull(clusterName,"clusterName must not be null");
        this.additionalConfiguration = new HashMap<>(checkNotNull(additionalConfiguration,"additional configuration must not be null"));
        this.nodes = checkNotNull(nodes,"nodes must not be null");
        checkArgument(this.nodes.length != 0, "nodes must not be empty");
    }

    TransportAddresses getTransportAddresses() {
        return new TransportAddresses(nodes);
    }

    Settings toBasicSettings() {
        return Settings.settingsBuilder()
                                .put("cluster.name", clusterName)
                                .put(additionalConfiguration)
                                .build();
    }
}

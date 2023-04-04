/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.cassandra.client;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.session.Session;
import java.util.Map;
import org.apache.storm.cassandra.context.BaseBeanFactory;

/**
 * Default interface to build cassandra Cluster from the a Storm Topology configuration.
 */
public class ClusterFactory extends BaseBeanFactory<CqlSession> {

    CqlSessionBuilder cqlSessionBuilder;

    /**
     * Creates a new Cluster based on the specified configuration.
     * @param topoConf the storm configuration.
     * @return a new a new {@link Session} instance.
     */
    @Override
    protected CqlSession make(Map<String, Object> topoConf) {
        cqlSessionBuilder = new CqlSessionBuilderFactory().make(topoConf);
        return cqlSessionBuilder.build();
    }

    protected CqlSessionBuilder getCqlSessionBuilder() {
        return cqlSessionBuilder; // only available after make() call
    }
}

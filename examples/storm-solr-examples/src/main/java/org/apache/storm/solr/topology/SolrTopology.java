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

package org.apache.storm.solr.topology;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.solr.config.SolrCommitStrategy;
import org.apache.storm.solr.config.SolrConfig;

public abstract class SolrTopology {
    protected static String COLLECTION = "gettingstarted";

    public void run(String[] args) throws Exception {
        final StormTopology topology = getTopology();
        final Config config = getConfig();

        String topoName = "test";
        if (args.length > 0) {
            topoName = args[0];
        }
        submitTopologyRemoteCluster(topoName, topology, config);
    }

    protected abstract StormTopology getTopology() throws IOException;

    protected void submitTopologyRemoteCluster(String arg, StormTopology topology, Config config) throws Exception {
        StormSubmitter.submitTopology(arg, config, topology);
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

    protected SolrCommitStrategy getSolrCommitStgy() {
        return null;                          // To Commit to Solr and Ack every tuple
    }

    protected static SolrConfig getSolrConfig() {
        String zkHostString = "127.0.0.1:9983";  // zkHostString for Solr gettingstarted example
        return new SolrConfig(zkHostString);
    }

    protected static SolrClient getSolrClient() {
        String zkHostString = "127.0.0.1:9983";  // zkHostString for Solr gettingstarted example
        return new CloudSolrClient(zkHostString);
    }

}

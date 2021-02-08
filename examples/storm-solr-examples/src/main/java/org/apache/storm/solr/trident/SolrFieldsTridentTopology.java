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

package org.apache.storm.solr.trident;

import java.io.IOException;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.solr.config.SolrConfig;
import org.apache.storm.solr.spout.SolrFieldsSpout;
import org.apache.storm.solr.topology.SolrFieldsTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;

public class SolrFieldsTridentTopology extends SolrFieldsTopology {
    public static void main(String[] args) throws Exception {
        SolrFieldsTridentTopology solrFieldsTridentTopology = new SolrFieldsTridentTopology();
        solrFieldsTridentTopology.run(args);
    }

    @Override
    protected StormTopology getTopology() throws IOException {
        final TridentTopology tridentTopology = new TridentTopology();
        final SolrFieldsSpout spout = new SolrFieldsSpout();
        final Stream stream = tridentTopology.newStream("SolrFieldsSpout", spout);
        SolrConfig solrConfig = getSolrConfig();
        final StateFactory solrStateFactory = new SolrStateFactory(solrConfig, getSolrMapper(solrConfig));
        stream.partitionPersist(solrStateFactory, spout.getOutputFields(),  new SolrUpdater(), new Fields());
        return tridentTopology.build();
    }
}

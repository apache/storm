/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.nimbus;

import java.util.Map;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTopologyValidator implements ITopologyValidator {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultTopologyValidator.class);

    @Override
    public void prepare(Map<String, Object> stormConf) {
    }

    @Override
    public void validate(String topologyName, Map topologyConf, StormTopology topology) throws InvalidTopologyException {
        if (topologyName.contains(".")) {
            LOG.warn("Metrics for topology name '{}' will be reported as '{}'.", topologyName, topologyName.replace('.', '_'));
        }
        Map<String, SpoutSpec> spouts = topology.get_spouts();
        for (String spoutName : spouts.keySet()) {
            if (spoutName.contains(".")) {
                LOG.warn("Metrics for spout name '{}' will be reported as '{}'.", spoutName, spoutName.replace('.', '_'));
            }
            SpoutSpec spoutSpec = spouts.get(spoutName);
            for (String streamName : spoutSpec.get_common().get_streams().keySet()) {
                if (streamName.contains(".")) {
                    LOG.warn("Metrics for stream name '{}' will be reported as '{}'.", streamName, streamName.replace('.', '_'));
                }
            }
        }

        Map<String, Bolt> bolts = topology.get_bolts();
        for (String boltName : bolts.keySet()) {
            if (boltName.contains(".")) {
                LOG.warn("Metrics for bolt name '{}' will be reported as '{}'.", boltName, boltName.replace('.', '_'));
            }
            Bolt bolt = bolts.get(boltName);
            for (String streamName : bolt.get_common().get_streams().keySet()) {
                if (streamName.contains(".")) {
                    LOG.warn("Metrics for stream name '{}' will be reported as '{}'.", streamName, streamName.replace('.', '_'));
                }
            }
        }
    }
}

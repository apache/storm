/*
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

package org.apache.storm;

import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.topology.TopologyBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestStormSubmitter {

    @Test
    public void invalidTopologyWithoutSpout() {
        String expectedExceptionMsgFragment = "does not have any spout";
        TopologyBuilder tb = new TopologyBuilder();
        tb.setBolt("bolt1", new TestWordCounter(), 10).shuffleGrouping("spout1");
        tb.setBolt("bolt11", new TestWordCounter(), 10).shuffleGrouping("bolt1");
        tb.setBolt("bolt12", new TestWordCounter(), 10).shuffleGrouping("bolt1");
        StormTopology topology = tb.createTopology();
        Map<String, Object> topoConf = null;
        SubmitOptions opts = new SubmitOptions(TopologyInitialStatus.INACTIVE);

        try {
            StormSubmitter.submitTopologyAs("test-topo-without-spout", topoConf, topology, opts, null, "none");
            Assert.fail("Topology without spout should fail in submission");
        } catch (InvalidTopologyException ex) {
            if (!ex.getMessage().contains(expectedExceptionMsgFragment)) {
                String err = String.format("Topology submit failure should contain string \"%s\", but is \"%s\"",
                        expectedExceptionMsgFragment, ex.getMessage());
                Assert.fail(err);
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
            Assert.fail("Unexpected exception submitting topology without spout: " + ex);
        }
    }
}

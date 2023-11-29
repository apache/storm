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
package org.apache.storm.flux;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.flux.model.ExecutionContext;
import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.apache.storm.flux.test.TestBolt;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import java.util.Collections;
import java.util.Properties;

public class TCKTest {
    
    @Test
    public void testTCK() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/tck.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testShellComponents() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/shell_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testBadShellComponents() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/bad_shell_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);

        IllegalArgumentException expectedException = assertThrows(IllegalArgumentException.class, () -> FluxBuilder.buildTopology(context));
        assertTrue(expectedException.getMessage().contains("Unable to find configuration method"));
    }

    @Test
    public void testKafkaSpoutConfig() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/kafka_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testLoadFromResource() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/kafka_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }


    @Test
    public void testHdfs() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/hdfs_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testDiamondTopology() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/diamond-topology.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testBadHbase() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/bad_hbase.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);

        IllegalArgumentException expectedException = assertThrows(IllegalArgumentException.class, () -> FluxBuilder.buildTopology(context));
        assertTrue(expectedException.getMessage().contains("Couldn't find a suitable constructor"));
    }

    @Test
    public void testIncludes() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/include_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        assertTrue(topologyDef.getName().equals("include-topology"));
        assertTrue(topologyDef.getBolts().size() > 0);
        assertTrue(topologyDef.getSpouts().size() > 0);
        topology.validate();
    }

    @Test
    public void testTopologySource() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testTopologySourceWithReflection() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology-reflection.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testTopologySourceWithConfigParam() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology-reflection-config.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testTopologySourceWithMethodName() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology-method-override.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }


    @Test
    public void testTridentTopologySource() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology-trident.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    public void testInvalidTopologySource() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/invalid-existing-topology.yaml", false, true, null, false);
        assertFalse(topologyDef.validate(), "Topology config is invalid.");
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        assertThrows(IllegalArgumentException.class, () -> FluxBuilder.buildTopology(context));
    }


    @Test
    public void testTopologySourceWithGetMethodName() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/existing-topology-reflection.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();
    }

    @Test
    public void testTopologySourceWithConfigMethods() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/config-methods-test.yaml", false, true, null, false);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();

        // make sure the property was actually set
        TestBolt bolt = (TestBolt)context.getBolt("bolt-1");
        assertTrue(bolt.getFoo().equals("foo"));
        assertTrue(bolt.getBar().equals("bar"));
        assertTrue(bolt.getFooBar().equals("foobar"));

        assertNotNull(context.getBolt("bolt-2"));
        assertNotNull(context.getBolt("bolt-3"));
        assertNotNull(context.getBolt("bolt-4"));
        assertArrayEquals(new TestBolt.TestClass[] {new TestBolt.TestClass("foo"), new TestBolt.TestClass("bar"), new TestBolt.TestClass("baz")}, bolt.getClasses());
    }

    @Test
    public void testVariableSubstitution() throws Exception {
        Properties properties = FluxParser.parseProperties("/configs/test.properties", true);
        TopologyDef topologyDef = FluxParser.parseResource("/configs/substitution-test.yaml", false, true, properties, true);
        assertTrue(topologyDef.validate());
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        topology.validate();

        // test basic substitution
        assertEquals("substitution-topology",
                context.getTopologyDef().getName(), "Property not replaced.");

        // test environment variable substitution
        // $PATH should be defined on most systems
        String envPath = System.getenv().get("PATH");
        assertEquals(envPath,
                context.getTopologyDef().getConfig().get("test.env.value"), "ENV variable not replaced.");
        
        //Test substitution where the target type is List
        assertThat("List property is not replaced by the expected value",
               Collections.singletonList("A string list"),
               is(context.getTopologyDef().getConfig().get("list.property.target")));

        //Test substitution where the target type is a List element
        assertThat("List element property is not replaced by the expected value",
                "A string list",
                is(context.getTopologyDef().getConfig().get("list.element.property.target")));

    }
    
    @Test
    public void testTopologyWithInvalidStaticFactoryArgument() throws Exception {
        //STORM-3087.
        TopologyDef topologyDef = FluxParser.parseResource("/configs/bad_static_factory_test.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);

        IllegalArgumentException expectedException = assertThrows(IllegalArgumentException.class, () -> FluxBuilder.buildTopology(context));
        assertTrue(expectedException.getMessage().contains("Couldn't find a suitable static method"));
    }

    @Test
    public void testTopologyWithWorkerHook() throws Exception {
        TopologyDef topologyDef = FluxParser.parseResource("/configs/worker_hook.yaml", false, true, null, false);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        assertNotNull(topology);
        assertTrue(topologyDef.getName().equals("worker-hook-topology"));
        assertTrue(topologyDef.getWorkerHooks().size() > 0);
        assertTrue(topology.get_worker_hooks_size() > 0);
        topology.validate();
    }
}

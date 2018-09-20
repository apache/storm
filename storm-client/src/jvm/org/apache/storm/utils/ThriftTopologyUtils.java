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

package org.apache.storm.utils;

import java.util.HashSet;
import java.util.Set;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;

public class ThriftTopologyUtils {
    public static boolean isWorkerHook(StormTopology._Fields f) {
        return f.equals(StormTopology._Fields.WORKER_HOOKS);
    }

    public static boolean isDependencies(StormTopology._Fields f) {
        return f.equals(StormTopology._Fields.DEPENDENCY_JARS) || f.equals(StormTopology._Fields.DEPENDENCY_ARTIFACTS);
    }

    public static Set<String> getComponentIds(StormTopology topology) {
        Set<String> ret = new HashSet<>();
        ret.addAll(topology.get_bolts().keySet());
        ret.addAll(topology.get_spouts().keySet());
        ret.addAll(topology.get_state_spouts().keySet());
        return ret;
    }

    public static ComponentCommon getComponentCommon(StormTopology topology, String componentId) {
        Bolt b = topology.get_bolts().get(componentId);
        if (b != null) {
            return b.get_common();
        }
        SpoutSpec s = topology.get_spouts().get(componentId);
        if (s != null) {
            return s.get_common();
        }
        StateSpoutSpec ss = topology.get_state_spouts().get(componentId);
        if (ss != null) {
            return ss.get_common();
        }
        throw new IllegalArgumentException("Could not find component common for " + componentId);
    }
}

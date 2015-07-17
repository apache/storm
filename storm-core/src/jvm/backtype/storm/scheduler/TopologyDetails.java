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
package backtype.storm.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.StormTopology;


public class TopologyDetails {
    String topologyId;
    Map topologyConf;
    StormTopology topology;
    Map<ExecutorInfo, String> executorToComponent;
    int numWorkers;
 
    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology, int numWorkers) {
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.topology = topology;
        this.numWorkers = numWorkers;
    }
    
    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology, int numWorkers, Map<ExecutorInfo, String> executorToComponents) {
        this(topologyId, topologyConf, topology, numWorkers);
        this.executorToComponent = new HashMap<ExecutorInfo, String>(0);
        if (executorToComponents != null) {
            this.executorToComponent.putAll(executorToComponents);
        }
    }
    
    public String getId() {
        return topologyId;
    }
    
    public String getName() {
        return (String)this.topologyConf.get(Config.TOPOLOGY_NAME);
    }
    
    public Map getConf() {
        return topologyConf;
    }
    
    public int getNumWorkers() {
        return numWorkers;
    }
    
    public StormTopology getTopology() {
        return topology;
    }

    public Map<ExecutorInfo, String> getExecutorToComponent() {
        return this.executorToComponent;
    }

    public Map<ExecutorInfo, String> selectExecutorToComponent(Collection<ExecutorInfo> executors) {
        Map<ExecutorInfo, String> ret = new HashMap<ExecutorInfo, String>(executors.size());
        for (ExecutorInfo executor : executors) {
            String compId = this.executorToComponent.get(executor);
            if (compId != null) {
                ret.put(executor, compId);
            }
        }
        
        return ret;
    }
    
    public Collection<ExecutorInfo> getExecutors() {
        return this.executorToComponent.keySet();
    }
}

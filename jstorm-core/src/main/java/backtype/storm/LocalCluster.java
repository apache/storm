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
package backtype.storm;

import java.util.Map;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Credentials;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.utils.JStormUtils;

public class LocalCluster implements ILocalCluster {
    
    public static Logger LOG = LoggerFactory.getLogger(LocalCluster.class);
    
    private LocalClusterMap state;
    
    protected void setLogger() {
        // the code is for log4j
        // boolean needReset = true;
        // Logger rootLogger = Logger.getRootLogger();
        // if (rootLogger != null) {
        // Enumeration appenders = rootLogger.getAllAppenders();
        // if (appenders.hasMoreElements() == true) {
        // needReset = false;
        // }
        // }
        //
        // if (needReset == true) {
        // BasicConfigurator.configure();
        // rootLogger.setLevel(Level.INFO);
        // }
        
    }
    
    // this is easy to debug
    protected static LocalCluster instance = null;
    
    public static LocalCluster getInstance() {
        return instance;
    }
    
    public LocalCluster() {
        synchronized (LocalCluster.class) {
            if (instance != null) {
                throw new RuntimeException("LocalCluster should be single");
            }
            setLogger();
            
            // fix in zk occur Address family not supported by protocol family:
            // connect
            System.setProperty("java.net.preferIPv4Stack", "true");
            
            this.state = LocalUtils.prepareLocalCluster();
            if (this.state == null)
                throw new RuntimeException("prepareLocalCluster error");
            
            instance = this;
        }
    }
    
    @Override
    public void submitTopology(String topologyName, Map conf, StormTopology topology) {
        submitTopologyWithOpts(topologyName, conf, topology, null);
    }
    
    @Override
    public void submitTopologyWithOpts(String topologyName, Map conf, StormTopology topology, SubmitOptions submitOpts) {
        // TODO Auto-generated method stub
        if (!Utils.isValidConf(conf))
            throw new RuntimeException("Topology conf is not json-serializable");
        JStormUtils.setLocalMode(true);
        
        try {
            if (submitOpts == null) {
                state.getNimbus().submitTopology(topologyName, null, Utils.to_json(conf), topology);
            } else {
                state.getNimbus().submitTopologyWithOpts(topologyName, null, Utils.to_json(conf), topology, submitOpts);
            }
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Failed to submit topology " + topologyName, e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void killTopology(String topologyName) {
        // TODO Auto-generated method stub
        try {
            // kill topology quickly
            KillOptions killOps = new KillOptions();
            killOps.set_wait_secs(0);
            state.getNimbus().killTopologyWithOpts(topologyName, killOps);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("fail to kill Topology " + topologyName, e);
        }
    }
    
    @Override
    public void killTopologyWithOpts(String name, KillOptions options) throws NotAliveException {
        // TODO Auto-generated method stub
        try {
            state.getNimbus().killTopologyWithOpts(name, options);
        } catch (TException e) {
            // TODO Auto-generated catch block
            LOG.error("fail to kill Topology " + name, e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void activate(String topologyName) {
        // TODO Auto-generated method stub
        try {
            state.getNimbus().activate(topologyName);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("fail to activate " + topologyName, e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void deactivate(String topologyName) {
        // TODO Auto-generated method stub
        try {
            state.getNimbus().deactivate(topologyName);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("fail to deactivate " + topologyName, e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void rebalance(String name, RebalanceOptions options) {
        // TODO Auto-generated method stub
        try {
            state.getNimbus().rebalance(name, options);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("fail to rebalance " + name, e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public void shutdown() {
        // TODO Auto-generated method stub
        // in order to avoid kill topology's command competition
        // it take 10 seconds to remove topology's node
        JStormUtils.sleepMs(10 * 1000);
        this.state.clean();
    }
    
    @Override
    public String getTopologyConf(String id) {
        // TODO Auto-generated method stub
        try {
            return state.getNimbus().getTopologyConf(id);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("fail to get topology Conf of topologId: " + id, e);
        }
        return null;
    }
    
    @Override
    public StormTopology getTopology(String id) {
        // TODO Auto-generated method stub
        try {
            return state.getNimbus().getTopology(id);
        } catch (NotAliveException e) {
            // TODO Auto-generated catch block
            LOG.error("fail to get topology of topologId: " + id, e);
        } catch (TException e) {
            // TODO Auto-generated catch block
            LOG.error("fail to get topology of topologId: " + id, e);
        }
        return null;
    }
    
    @Override
    public ClusterSummary getClusterInfo() {
        // TODO Auto-generated method stub
        try {
            return state.getNimbus().getClusterInfo();
        } catch (TException e) {
            // TODO Auto-generated catch block
            LOG.error("fail to get cluster info", e);
        }
        return null;
    }
    
    @Override
    public TopologyInfo getTopologyInfo(String id) {
        // TODO Auto-generated method stub
        try {
            return state.getNimbus().getTopologyInfo(id);
        } catch (NotAliveException e) {
            // TODO Auto-generated catch block
            LOG.error("fail to get topology info of topologyId: " + id, e);
        } catch (TException e) {
            // TODO Auto-generated catch block
            LOG.error("fail to get topology info of topologyId: " + id, e);
        }
        return null;
    }
    
    /***
     * You should use getLocalClusterMap() to instead.This function will always return null
     * */
    @Deprecated
    @Override
    public Map getState() {
        // TODO Auto-generated method stub
        return null;
    }
    
    public LocalClusterMap getLocalClusterMap() {
        return state;
    }
    
    public static void main(String[] args) throws Exception {
        LocalCluster localCluster = null;
        try {
            localCluster = new LocalCluster();
        } finally {
            if (localCluster != null) {
                localCluster.shutdown();
            }
        }
    }

    @Override
    public void uploadNewCredentials(String topologyName, Credentials creds) {
        // TODO Auto-generated method stub
        try {
            state.getNimbus().uploadNewCredentials(topologyName, creds);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("fail to uploadNewCredentials of topologyId: " + topologyName, e);
        } 
    }
    
}

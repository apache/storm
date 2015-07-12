package com.dianping.cosmos.monitor.topology;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.dianping.cosmos.monitor.HttpCatClient;
import com.dianping.cosmos.util.Constants;
import com.dianping.cosmos.util.TupleHelpers;

@SuppressWarnings({ "rawtypes", "unchecked"})
public class ClusterInfoBolt  extends BaseRichBolt{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInfoBolt.class);

    private static final long serialVersionUID = 1L;
    private transient Client client;
    private transient NimbusClient nimbusClient;
    private OutputCollector collector;
    private Map configMap = null;
   
    @Override
    public void prepare(Map map, TopologyContext topologycontext,
            OutputCollector outputcollector) {
        this.collector = outputcollector;
        this.configMap = map;
        initClient(configMap);
    }
    private void initClient(Map map) {
        nimbusClient = NimbusClient.getConfiguredClient(map);
        client = nimbusClient.getClient();
    }
    @Override
    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            if(nimbusClient == null){
                initClient(configMap);
            }
            getClusterInfo(client);
            collector.emit(new Values(tuple));
        }        
    }
    
    private void getClusterInfo(Client client) {
        try {
            ClusterSummary clusterSummary = client.getClusterInfo();
            List<SupervisorSummary> supervisorSummaryList = clusterSummary.get_supervisors();
            int totalWorkers = 0;
            int usedWorkers = 0;
            for(SupervisorSummary summary : supervisorSummaryList){
                totalWorkers += summary.get_num_workers() ;
                usedWorkers += summary.get_num_used_workers();
            }
            int freeWorkers = totalWorkers - usedWorkers;
            LOGGER.info("cluster totalWorkers = " + totalWorkers 
                    + ", usedWorkers = " + usedWorkers 
                    + ", freeWorkers  = " +  freeWorkers);
            
            HttpCatClient.sendMetric("ClusterMonitor", "freeSlots", "avg", String.valueOf(freeWorkers));
            HttpCatClient.sendMetric("ClusterMonitor", "totalSlots", "avg", String.valueOf(totalWorkers));
            
            List<TopologySummary> topologySummaryList = clusterSummary.get_topologies();
            long clusterTPS = 0l;
            for(TopologySummary topology : topologySummaryList){
                long topologyTPS = getTopologyTPS(topology, client);
                clusterTPS += topologyTPS;
                if(topology.get_name().startsWith("ClusterMonitor")){
                    continue;
                }
                HttpCatClient.sendMetric(topology.get_name(), topology.get_name() + "-TPS", "avg", String.valueOf(topologyTPS));
            }
            HttpCatClient.sendMetric("ClusterMonitor", "ClusterEmitTPS", "avg", String.valueOf(clusterTPS));
            
        } catch (TException e) {
            initClient(configMap);
            LOGGER.error("get client info error.", e);
        }
        catch(NotAliveException nae){
            LOGGER.warn("topology is dead.", nae);
        }
    }
    
    protected long getTopologyTPS(TopologySummary topology, Client client) throws NotAliveException, TException{
        long topologyTps = 0l;
        String topologyId = topology.get_id();
        if(topologyId.startsWith("ClusterMonitor")){
            return topologyTps;
        }
        TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
        if(topologyInfo == null){
            return topologyTps;
        }
        List<ExecutorSummary> executorSummaryList = topologyInfo.get_executors();
        for(ExecutorSummary executor : executorSummaryList){
            topologyTps += getComponentTPS(executor);
        }
        LOGGER.info("topology = " + topology.get_name() + ", tps = " + topologyTps);
        return topologyTps;
    }
    
    private long getComponentTPS(ExecutorSummary executor) {
        long componentTps = 0l;
        if(executor == null){
            return componentTps;
        }
        String componentId = executor.get_component_id();
        
        if(Utils.isSystemId(componentId)){
            return componentTps;
        }
        if(executor.get_stats() == null){
            return componentTps;
        }

        Map<String, Map<String, Long>> emittedMap = executor.get_stats().get_emitted();
        Map<String, Long> minutesEmitted = emittedMap.get("600");
        if(minutesEmitted == null){
            return componentTps;
        }
        for(Map.Entry<String, Long> emittedEntry : minutesEmitted.entrySet()){
            if(Utils.isSystemId(emittedEntry.getKey())){
                continue;
            }
            if(executor.get_uptime_secs() >= 600){
                componentTps += emittedEntry.getValue() / 600;
            }
            if(executor.get_uptime_secs() >= 10 && executor.get_uptime_secs() < 600){
                componentTps += emittedEntry.getValue() / executor.get_uptime_secs();
            }   
        }
        LOGGER.debug("component = " + componentId + ", tps = " + componentTps);
        return componentTps;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputfieldsdeclarer) {        
        outputfieldsdeclarer.declare(new Fields("monitor"));
    }
    
    @Override
    public Map getComponentConfiguration(){
         Map<String, Object> conf = new HashMap<String, Object>();
         conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Constants.TPS_COUNTER_FREQUENCY_IN_SECONDS);
         return conf;
    }
        
}

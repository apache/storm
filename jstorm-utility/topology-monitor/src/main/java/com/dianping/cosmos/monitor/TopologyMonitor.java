package com.dianping.cosmos.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.IMetricsConsumer.DataPoint;
import backtype.storm.metric.api.IMetricsConsumer.TaskInfo;

public class TopologyMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyMonitor.class);

    private static Map<Integer, SpoutCounter> spoutCounterMap = new ConcurrentHashMap<Integer, SpoutCounter>();

    public void monitorStatus(String stormId, TaskInfo taskInfo, DataPoint p) {
        SpoutCounter counter = spoutCounterMap.get(taskInfo.srcTaskId);
        if(counter == null){
            counter = new SpoutCounter();
            spoutCounterMap.put(taskInfo.srcTaskId, counter);
        }
        counter.incrRepeatCounter();
        String value = String.valueOf(p.value);
        long increment = Long.parseLong(value);
        counter.incrTupleCounter(increment);
        //连续1分钟
        if(counter.getRepeatCounter() >= 12){
            //数据量少于某个记录
            LOGGER.info("last minute tuple = " + counter.getTupleCounter());
             if(counter.getTupleCounter() <= 10000){
                LOGGER.error("spout has problem, restar topology....");
                //restartTopology(stormId);
            }
            spoutCounterMap.clear();
        }
    }
    
    /**
     * stromId: MobileUV_7-212-1409657868
     * @param stormId
     */
    public void restartTopology(String stormId){
        String currentTopology = StringUtils.substringBefore(stormId, "-");
        String topologyPrefix = StringUtils.substringBefore(currentTopology, "_");
        String topologyIndex =  StringUtils.substringAfter(currentTopology, "_");
        int newIndex = Integer.parseInt(topologyIndex) + 1;
        String newTopologyName = topologyPrefix + "_" + newIndex;
        LOGGER.info("new topology name = " + newTopologyName);
        execStartCommand(newTopologyName);
        LOGGER.info("execStartCommand finish ..");
        execShutdownCommand(currentTopology);
        LOGGER.info("execShutdownCommand finish ..");
    }
    
    public void execStartCommand(String topologyName){
        Process process;
        try {
            process = Runtime.getRuntime().exec(new String[]{
                    "/usr/local/storm/bin/storm",  
                    "jar", 
                    "/home/hadoop/topology/meteor-traffic-0.0.1.jar", 
                    "com.dianping.data.warehouse.traffic.mobile.MobileUVTopology",  
                    topologyName});
                process.waitFor();
        } catch (Exception e) {
            LOGGER.error("", e);
        }  
    }
    
    public void execShutdownCommand(String topologyName){
        Process process;
        try {
            process = Runtime.getRuntime().exec(new String[]{
                    "/usr/local/storm/bin/storm",  
                    "kill", 
                    topologyName,
                    "10"});
                process.waitFor();
        } catch (Exception e) {
            LOGGER.error("", e);
        }  
    }
    
    public static void main(String[] args){
        TopologyMonitor monitor = new TopologyMonitor();
        monitor.restartTopology("MobileUV_7-212-1409657868");
    }
}

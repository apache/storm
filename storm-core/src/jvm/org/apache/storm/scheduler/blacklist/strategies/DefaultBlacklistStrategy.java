package org.apache.storm.scheduler.blacklist.strategies;

import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.blacklist.CircularBuffer;
import org.apache.storm.scheduler.blacklist.reporters.IReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by howard.li on 2016/7/13.
 */
public class DefaultBlacklistStrategy implements IBlacklistStrategy{

    private static Logger LOG = LoggerFactory.getLogger(DefaultBlacklistStrategy.class);

    private IReporter _reporter;

    private int _toleranceTime;
    private int _toleranceCount;
    private int _resumeTime;
    private int _nimbusMonitorFreqSecs;

    private TreeMap<String,Integer> blacklist;

    @Override
    public void prepare(IReporter reporter,int toleranceTime, int toleranceCount, int resumeTime,int nimbusMonitorFreqSecs) {
        _reporter=reporter;

        _toleranceTime=toleranceTime;
        _toleranceCount=toleranceCount;
        _resumeTime=resumeTime;
        _nimbusMonitorFreqSecs=nimbusMonitorFreqSecs;
        blacklist=new TreeMap<>();
    }

    @Override
    public Set<String> getBlacklist(CircularBuffer<HashMap<String,Set<Integer>>> toleranceBuffer,Cluster cluster, Topologies topologies){
        //Set<String> blacklist=new HashSet<String>();
        Map<String,Integer> countMap=new HashMap<String,Integer>();

        for(Map<String,Set<Integer>> item : toleranceBuffer){
            Set<String> supervisors=item.keySet();
            for(String supervisor :supervisors){
                int supervisorCount=0;
                if(countMap.containsKey(supervisor)){
                    supervisorCount=countMap.get(supervisor);
                }
                countMap.put(supervisor, supervisorCount + 1);
            }
        }
        for(Map.Entry<String,Integer> entry:countMap.entrySet()){
            String supervisor=entry.getKey();
            int count=entry.getValue();
            if(count>=_toleranceCount){
                if(!blacklist.containsKey(supervisor)){// if not in blacklist then add it and set the resume time according to config
                    LOG.info("add supervisor {} to blacklist",supervisor);
                    LOG.info("toleranceBuffer : {}",toleranceBuffer);
                    _reporter.reportBlacklist(supervisor, toleranceBuffer);
                    blacklist.put(supervisor,_resumeTime/_nimbusMonitorFreqSecs);
                }
            }
        }
        releaseBlacklistWhenNeeded(cluster,topologies);
        return blacklist.keySet();
    }

    public void resumeFromBlacklist(){
        Set<String> readyToRemove=new HashSet<String>();
        for(Map.Entry<String,Integer> entry:blacklist.entrySet()){
            String key=entry.getKey();
            int value=entry.getValue()-1;
            if (value == 0) {
                readyToRemove.add(key);
            }else{
                blacklist.put(key,value);
            }
        }
        for(String key : readyToRemove){
            blacklist.remove(key);
            LOG.info("supervisor {} reach the resume time ,removed from blacklist",key);
        }
    }

    public void releaseBlacklistWhenNeeded(Cluster cluster, Topologies topologies) {
        if(blacklist.size()>0){
            int totalNeedNumWorkers=0;
            List<TopologyDetails> needSchedulingTopologies=cluster.needsSchedulingTopologies(topologies);
            for(TopologyDetails topologyDetails:needSchedulingTopologies){
                int numWorkers=topologyDetails.getNumWorkers();
                int assignedNumWorkers=cluster.getAssignedNumWorkers(topologyDetails);
                int unAssignedNumWorkers=numWorkers-assignedNumWorkers;
                totalNeedNumWorkers+=unAssignedNumWorkers;
            }
            Map<String, SupervisorDetails> availableSupervisors=cluster.getSupervisors();
            List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
            int availableSlotsNotInBlacklistCount=0;
            for(WorkerSlot slot:availableSlots){
                if(!blacklist.containsKey(slot.getNodeId())){
                    availableSlotsNotInBlacklistCount+=1;
                }
            }
            int shortage=totalNeedNumWorkers-availableSlotsNotInBlacklistCount;

            if(shortage>0){
                LOG.info("total needed num of workers :{}, available num of slots not in blacklist :{},num blacklist :{}, will release some blacklist."
                        ,totalNeedNumWorkers,availableSlotsNotInBlacklistCount,blacklist.size());
                //release earliest blacklist
                Set<String> readyToRemove=new HashSet<String>();
                for(String supervisor:blacklist.keySet()){//blacklist is treeMap sorted by value, value minimum meas earliest
                    if(availableSupervisors.containsKey(supervisor)){
                        Set<Integer> ports=cluster.getAvailablePorts(availableSupervisors.get(supervisor));
                        readyToRemove.add(supervisor);
                        shortage-=ports.size();
                        if(shortage<=0){//released enough supervisor
                            break;
                        }
                    }
                }
                for(String key : readyToRemove){
                    blacklist.remove(key);
                    LOG.info("release supervisor {} for shortage of worker slots.",key);
                }
            }
        }
    }
}

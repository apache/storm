package org.apache.storm.scheduler.blacklist;

import org.apache.storm.Config;
import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.blacklist.reporter.IReporter;
import org.apache.storm.scheduler.blacklist.strategies.IBlacklistStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by howard.li on 2016/6/29.
 */
public class BlacklistScheduler implements IScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(BlacklistScheduler.class);
    DefaultScheduler defaultScheduler;
    @SuppressWarnings("rawtypes")
    private Map _conf;

    public static final String BLACKLIST_ENABLE="topology.scheduler.blacklist.enable";

    public static final String TOLERANCE_TIME="storm.scheduler.blacklist.tolerance.time.secs";
    public static final String TOLERANCE_COUNT="storm.scheduler.blacklist.tolerance.count";
    public static final String RESUME_TIME="storm.scheduler.blacklist.resume.time.secs";
    public static final String REPORTER ="storm.scheduler.blacklist.reporter";
    public static final String STRATEGY ="storm.scheduler.blacklist.reporter";

    public static final int DEFAULT_TOLERANCE_TIME=5*60;
    public static final int DEFAULT_TOLERANCE_COUNT=3;
    public static final int DEFAULT_RESUME_TIME=30*60;
    public static final String DEFAULT_REPORTER="org.apache.storm.scheduler.blacklist.reporter.LogReporter";
    public static final String DEFAULT_STRATEGY="org.apache.storm.scheduler.blacklist.strategies.DefaultBlacklistStrategy";

    private int toleranceTime=DEFAULT_TOLERANCE_TIME;
    private int toleranceCount=DEFAULT_TOLERANCE_COUNT;
    private int resumeTime=DEFAULT_RESUME_TIME;
    private IReporter reporter;
    private IBlacklistStrategy blacklistStrategy;

    private int nimbusMonitorFreqSecs;


    private Map<String,Set<Integer>> cachedSupervisors;

    //key is supervisor key ,value is supervisor ports
    private CircularBuffer<HashMap<String,Set<Integer>>> toleranceBuffer;
    private Set<WorkerSlot> slotsInBlacklistTopology;

    @Override
    public void prepare(Map conf) {
        LOG.info("prepare black list scheduler");
        LOG.info(conf.toString());
        defaultScheduler=new DefaultScheduler();
        defaultScheduler.prepare(conf);
        _conf=conf;
        if(_conf.containsKey(TOLERANCE_TIME)){
            toleranceTime=(Integer)_conf.get(TOLERANCE_TIME);
        }
        if(_conf.containsKey(TOLERANCE_COUNT)){
            toleranceCount=(Integer)_conf.get(TOLERANCE_COUNT);
        }
        if(_conf.containsKey(RESUME_TIME)){
            resumeTime=(Integer)_conf.get(RESUME_TIME);
        }
        String reporterClassName=_conf.containsKey(REPORTER)?(String)_conf.get(REPORTER):DEFAULT_REPORTER;
        try {
            reporter=(IReporter)Class.forName(reporterClassName).newInstance();
        } catch (ClassNotFoundException e) {
            LOG.error("Can't find blacklist reporter for name {}",reporterClassName);
        } catch (InstantiationException e) {
            LOG.error("Throw InstantiationException blacklist reporter for name {}", reporterClassName);
        } catch (IllegalAccessException e) {
            LOG.error("Throw illegalAccessException blacklist reporter for name {}", reporterClassName);
        }

        String strategyClassName=_conf.containsKey(STRATEGY)?(String)_conf.get(STRATEGY):DEFAULT_STRATEGY;
        try {
            blacklistStrategy=(IBlacklistStrategy)Class.forName(strategyClassName).newInstance();
        } catch (ClassNotFoundException e) {
            LOG.error("Can't find blacklist strategy for name {}",reporterClassName);
        } catch (InstantiationException e) {
            LOG.error("Throw InstantiationException blacklist strategy for name {}", reporterClassName);
        } catch (IllegalAccessException e) {
            LOG.error("Throw illegalAccessException blacklist strategy for name {}", reporterClassName);
        }

        nimbusMonitorFreqSecs=(Integer)_conf.get(Config.NIMBUS_MONITOR_FREQ_SECS);
        blacklistStrategy.prepare(reporter,toleranceTime,toleranceCount,resumeTime,nimbusMonitorFreqSecs);

        toleranceBuffer=new CircularBuffer<HashMap<String,Set<Integer>>>(toleranceTime/nimbusMonitorFreqSecs);
        cachedSupervisors=new HashMap<>();


    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.info("running Black List scheduler");
        Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();
        for(Map.Entry<String,SupervisorDetails> entry:supervisors.entrySet()){
            SupervisorDetails supervisorDetails=entry.getValue();
            String supervisorName=entry.getKey();
            Set<Integer> ports=supervisorDetails.getAllPorts();
            LOG.info("supervisor: "+supervisorDetails.getHost()+" ports"+ports);
        }
        LOG.info( "AssignableSlots: "+cluster.getAssignableSlots());
        LOG.info("AvailableSlots: "+cluster.getAvailableSlots());
        LOG.info("UsedSlots: "+cluster.getUsedSlots());

        blacklistStrategy.resumeFromBlacklist();
        //blacklistStrategy.releaseBlacklistWhenNeeded(cluster,topologies);
        badSupervisors(supervisors);
        cluster.setBlacklistedHosts(getBlacklistHosts(cluster,topologies));
        removeLongTimeDisappearFromCache();

        defaultScheduler.schedule(topologies,cluster);

        //add to slotsInBlacklistTopology after all Assignment. So it can be used in next time assignment
        slotsInBlacklistTopology =new HashSet<>();
        for(TopologyDetails topologyDetails :topologies.getTopologies()){
            cluster.needsScheduling(topologyDetails);
            Map config=topologyDetails.getConf();
            if(config.containsKey(BLACKLIST_ENABLE) ? (boolean)config.get(BLACKLIST_ENABLE) : (boolean)_conf.get(BLACKLIST_ENABLE)){
                LOG.info("topology {} enable blacklist scheduler",topologyDetails.getName() );
                Collection<WorkerSlot> usedSlots = cluster.getUsedSlotsByTopologyId(topologyDetails.getId());
                if(usedSlots==null){
                    LOG.error("default scheduler doesn't assign anything for topology {} ,should never happened.",topologyDetails.getName());
                }else{
                    LOG.info("topology {} uses slots: {}",topologyDetails.getName(),usedSlots);
                    slotsInBlacklistTopology.addAll(usedSlots);
                }
            }
        }
    }

    private void badSupervisors(Map<String, SupervisorDetails> supervisors){
        Set<String> cachedSupervisorsKeySet=cachedSupervisors.keySet();
        Set<String> supervisorsKeySet=supervisors.keySet();

        Set<String> missSupervisorKeys=Sets.difference(cachedSupervisorsKeySet,supervisorsKeySet);//cached supervisor doesn't show up
        HashMap<String,Set<Integer>> missSupervisors=new HashMap<String,Set<Integer>>();
        for(String key:missSupervisorKeys){
            missSupervisors.put(key,cachedSupervisors.get(key));
        }

        for(Map.Entry<String,SupervisorDetails> entry :supervisors.entrySet()){
            String key=entry.getKey();
            SupervisorDetails supervisorDetails=entry.getValue();
            if(cachedSupervisors.containsKey(key)){
                Set<Integer> badSlots=badSlots(supervisors,key);
                if(badSlots.size()>0){//supervisor contains bad slots
                    missSupervisors.put(key, badSlots);
                }
            }else{
                cachedSupervisors.put(key,supervisorDetails.getAllPorts());//new supervisor to cache
            }
        }

        toleranceBuffer.add(missSupervisors);
    }

    private Set<Integer> badSlots(Map<String, SupervisorDetails> supervisors, String supervisorKey){
        SupervisorDetails supervisor=supervisors.get(supervisorKey);
        Set<Integer> cachedSupervisorPorts=cachedSupervisors.get(supervisorKey);
        Set<Integer> supervisorPorts=supervisor.getAllPorts();

        Set<Integer> newPorts=Sets.difference(supervisorPorts,cachedSupervisorPorts);
        if(newPorts.size()>0){
            cachedSupervisors.put(supervisorKey,Sets.union(newPorts, supervisor.getAllPorts()));
        }

        Set<Integer> difference=Sets.difference(cachedSupervisorPorts,supervisorPorts);
        Set<Integer> badSlots=new HashSet<>();
        for(int port :difference){
            if(slotsInBlacklistTopology.contains(new WorkerSlot(supervisorKey,port))){
                badSlots.add(port);
            }
        }
        return badSlots;
    }

    public Set<String> getBlacklistHosts(Cluster cluster,Topologies topologies){
        Set<String> blacklist=blacklistStrategy.getBlacklist(toleranceBuffer,cluster,topologies);
        Set<String> blacklistHost=new HashSet<>();
        for(String supervisor:blacklist){
            String host=cluster.getHost(supervisor);
            //LOG.info("add supervisor {} to blacklist ,host is :{}",supervisor,host);
            if(host!=null){
                blacklistHost.add(host);
            }else{
                LOG.info("supervisor {} is not alive know, do not need to add to blacklist.", supervisor);
            }
        }
        return blacklistHost;
    }

    //supervisor or port never exits once in tolerance time will be removed from cache
    private void removeLongTimeDisappearFromCache(){

        Map<String,Integer> supervisorCountMap=new HashMap<String,Integer>();
        Map<WorkerSlot,Integer> slotCountMap=new HashMap<WorkerSlot,Integer>();

        for(Map<String,Set<Integer>> item : toleranceBuffer){
            Set<String> supervisors=item.keySet();
            for(String supervisor :supervisors){
                int supervisorCount=0;
                if(supervisorCountMap.containsKey(supervisor)){
                    supervisorCount=supervisorCountMap.get(supervisor);
                }
                supervisorCountMap.put(supervisor, supervisorCount+1);
                for(Integer slot:item.get(supervisor)){
                    int slotCount=0;
                    WorkerSlot workerSlot=new WorkerSlot(supervisor,slot);
                    if(slotCountMap.containsKey(workerSlot)){
                        slotCount=slotCountMap.get(workerSlot);
                    }
                    slotCountMap.put(workerSlot,slotCount+1);
                }
            }
        }

        int windowSize=toleranceBuffer.capacity();
        for(Map.Entry<String,Integer> entry:supervisorCountMap.entrySet()){
            String key=entry.getKey();
            int value=entry.getValue();
            if(value==windowSize){//supervisor never exits once in tolerance time will be removed from cache
                cachedSupervisors.remove(key);
                LOG.info("supervisor {} has never exited once during tolerance time, proberbly be dead forever, removed from cache.",key);
            }
        }

        for(Map.Entry<WorkerSlot,Integer> entry:slotCountMap.entrySet()){
            WorkerSlot workerSlot=entry.getKey();
            String supervisorKey=workerSlot.getNodeId();
            Integer slot=workerSlot.getPort();
            int value=entry.getValue();
            if(value==windowSize){//port never exits once in tolerance time will be removed from cache
                Set<Integer> slots=cachedSupervisors.get(supervisorKey);
                if(slots!=null){//slots will be null while supervisor has been removed from cached supervisors
                    slots.remove(slot);
                }
                cachedSupervisors.put(supervisorKey, slots);
                LOG.info("slot {} has never exited once during tolerance time, proberbly be dead forever, removed from cache.",workerSlot);
            }
        }
    }
}
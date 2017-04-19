package org.apache.storm.scheduler.blacklist;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.storm.Config;
import org.apache.storm.scheduler.*;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by howard.li on 2016/7/11.
 */
public class TestBlacklistScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(TestBlacklistScheduler.class);

    private static int currentTime = 1468216504;

    @Test
    public void TestBadSupervisor(){
        INimbus iNimbus=new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap= TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(BlacklistScheduler.TOLERANCE_TIME,200);
        config.put(BlacklistScheduler.TOLERANCE_COUNT,2);
        config.put(BlacklistScheduler.RESUME_TIME,300);

        config.put(BlacklistScheduler.BLACKLIST_ENABLE,true);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        //TopologyDetails topo2 = TestUtilsForBlacklistScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8,true);
        //TopologyDetails topo3 = TestUtilsForBlacklistScheduler.getTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16,true);
        topoMap.put(topo1.getId(), topo1);
        //topoMap.put(topo2.getId(), topo2);
        //topoMap.put(topo3.getId(), topo3);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        Topologies topologies = new Topologies(topoMap);
        BlacklistScheduler bs=new BlacklistScheduler();
        bs.prepare(config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        Set<String> hosts=new HashSet<>();
        hosts.add("host-0");
        Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
    }

    @Test
    public void TestBadSlot(){
        INimbus iNimbus=new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap= TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(BlacklistScheduler.TOLERANCE_TIME,200);
        config.put(BlacklistScheduler.TOLERANCE_COUNT,2);
        config.put(BlacklistScheduler.RESUME_TIME,300);

        config.put(BlacklistScheduler.BLACKLIST_ENABLE,true);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        //TopologyDetails topo2 = TestUtilsForBlacklistScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8,true);
        //TopologyDetails topo3 = TestUtilsForBlacklistScheduler.getTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16,true);
        topoMap.put(topo1.getId(), topo1);
        //topoMap.put(topo2.getId(), topo2);
        //topoMap.put(topo3.getId(), topo3);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        Topologies topologies = new Topologies(topoMap);
        BlacklistScheduler bs=new BlacklistScheduler();
        bs.prepare(config);
        bs.schedule(topologies,cluster);

        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removePortFromSupervisors(supMap, "sup-0", 0), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removePortFromSupervisors(supMap, "sup-0", 0), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        bs.schedule(topologies,cluster);
        Set<String> hosts=new HashSet<>();
        hosts.add("host-0");
        Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
    }

    @Test
    public void TestResumeBlacklist(){
        INimbus iNimbus=new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap= TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(BlacklistScheduler.TOLERANCE_TIME,40);
        config.put(BlacklistScheduler.TOLERANCE_COUNT,2);
        config.put(BlacklistScheduler.RESUME_TIME,300);

        config.put(BlacklistScheduler.BLACKLIST_ENABLE,true);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        Topologies topologies = new Topologies(topoMap);
        BlacklistScheduler bs=new BlacklistScheduler();
        bs.prepare(config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        bs.schedule(topologies,cluster);
        Set<String> hosts=new HashSet<>();
        hosts.add("host-0");
        Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
        for(int i=0;i<300/10-2;i++){
            bs.schedule(topologies,cluster);
        }
        Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
        bs.schedule(topologies,cluster);
        hosts.clear();
        Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
    }

    @Test
    public void TestReleaseBlacklist(){
        INimbus iNimbus=new TestUtilsForBlacklistScheduler.INimbusTest();

        Map<String, SupervisorDetails> supMap= TestUtilsForBlacklistScheduler.genSupervisors(3, 4);

        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(BlacklistScheduler.TOLERANCE_TIME,200);
        config.put(BlacklistScheduler.TOLERANCE_COUNT,2);
        config.put(BlacklistScheduler.RESUME_TIME,300);

        config.put(BlacklistScheduler.BLACKLIST_ENABLE,true);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();

        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        TopologyDetails topo2 = TestUtilsForBlacklistScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, true);
        TopologyDetails topo3 = TestUtilsForBlacklistScheduler.getTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16, true);
        TopologyDetails topo4 = TestUtilsForBlacklistScheduler.getTopology("topo-4", config, 5, 15, 1, 1, currentTime - 32, true);
        topoMap.put(topo1.getId(), topo1);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        Topologies topologies = new Topologies(topoMap);
        BlacklistScheduler bs=new BlacklistScheduler();
        bs.prepare(config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supMap, "sup-0"), TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        cluster = new Cluster(iNimbus, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        Set<String> hosts=new HashSet<>();
        hosts.add("host-0");
        Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topologies = new Topologies(topoMap);
        cluster = new Cluster(iNimbus, supMap, TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments()), config);
        bs.schedule(topologies,cluster);
        hosts.clear();
        Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
    }

    @Test
    public void TestList(){
        INimbus iNimbus=new TestUtilsForBlacklistScheduler.INimbusTest();
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(BlacklistScheduler.TOLERANCE_TIME,200);
        config.put(BlacklistScheduler.TOLERANCE_COUNT,2);
        config.put(BlacklistScheduler.RESUME_TIME,300);
        config.put(BlacklistScheduler.BLACKLIST_ENABLE,true);
        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        TopologyDetails topo1 = TestUtilsForBlacklistScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, true);
        TopologyDetails topo2 = TestUtilsForBlacklistScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 2, true);
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        Topologies topologies = new Topologies(topoMap);
        BlacklistScheduler bs=new BlacklistScheduler();
        bs.prepare(config);

        List<Map<Integer,List<Integer>>> faultList=new ArrayList<>();

        faultList.add(new HashMap<Integer, List<Integer>>());
        faultList.add((Map)ImmutableMap.of(0, ImmutableList.of(0,1)));
        faultList.add((Map) ImmutableMap.of(0, new ArrayList<>()));
        for(int i=0;i<17;i++){
            faultList.add(new HashMap<Integer, List<Integer>>());
        }
        faultList.add((Map)ImmutableMap.of(0, ImmutableList.of(0,1)));
        faultList.add((Map)ImmutableMap.of(1, ImmutableList.of(1)));
        for(int i=0;i<8;i++){
            faultList.add(new HashMap<Integer, List<Integer>>());
        }
        faultList.add((Map)ImmutableMap.of(0, ImmutableList.of(1)));
        faultList.add((Map)ImmutableMap.of(1, ImmutableList.of(1)));
        for(int i=0;i<30;i++){
            faultList.add(new HashMap<Integer, List<Integer>>());
        }

        List<Map<String, SupervisorDetails>> supervisorsList = FaultGenerateUtils.getSupervisorsList(3, 4, faultList);
        Cluster cluster = null;
        int count=0;
        for(Map<String,SupervisorDetails> supervisors:supervisorsList){
            cluster = FaultGenerateUtils.nextCluster(cluster, supervisors, iNimbus, config);
            bs.schedule(topologies,cluster);
            if(count==0){
                Set<String> hosts=new HashSet<>();
                Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
            }else if(count==2){
                Set<String> hosts=new HashSet<>();
                Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
            }else if(count==3){
                Set<String> hosts=new HashSet<>();
                hosts.add("host-0");
                Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
            }else if (count==30){
                Set<String> hosts=new HashSet<>();
                hosts.add("host-0");
                Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
            }else if (count==31){
                Set<String> hosts=new HashSet<>();
                hosts.add("host-0");
                hosts.add("host-1");
                Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
            }else if(count==32){
                Set<String> hosts=new HashSet<>();
                hosts.add("host-0");
                hosts.add("host-1");
                Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
            }else if(count==60){
                Set<String> hosts=new HashSet<>();
                hosts.add("host-0");
                hosts.add("host-1");
                Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
            }else if(count==61){
                Set<String> hosts=new HashSet<>();
                hosts.add("host-0");
                Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
            }else if(count==62){
                Set<String> hosts=new HashSet<>();
                Assert.assertEquals("blacklist",hosts, cluster.getBlacklistedHosts());
            }
            count++;
        }

    }
}

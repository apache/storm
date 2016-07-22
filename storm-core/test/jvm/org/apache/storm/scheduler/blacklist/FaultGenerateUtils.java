package org.apache.storm.scheduler.blacklist;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by howard.li on 2016/7/15.
 */
public class FaultGenerateUtils {

    public static List<Map<String, SupervisorDetails>> getSupervisorsList(int supervisorCount,int slotCount,int [][][]faults){
        List<Map<String,SupervisorDetails>> supervisorsList=new ArrayList<>(faults.length);
        for(int [][]fault:faults){
            Map<String,SupervisorDetails> supervisors= TestUtilsForBlacklistScheduler.genSupervisors(supervisorCount, slotCount);
            if(fault.length==1 && fault[0][0]==-1){
                TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supervisors, "sup-0");
            }
        }
        return supervisorsList;
    }

    public static List<Map<String, SupervisorDetails>> getSupervisorsList(int supervisorCount,int slotCount,List<Map<Integer,List<Integer>>> faultList){
        List<Map<String,SupervisorDetails>> supervisorsList=new ArrayList<>(faultList.size());
        for(Map<Integer,List<Integer>> faults:faultList){
            Map<String,SupervisorDetails> supervisors= TestUtilsForBlacklistScheduler.genSupervisors(supervisorCount, slotCount);
            for(Map.Entry<Integer,List<Integer>> fault:faults.entrySet()){
                int supervisor=fault.getKey();
                List<Integer> slots=fault.getValue();
                if(slots.isEmpty()){
                    supervisors= TestUtilsForBlacklistScheduler.removeSupervisorFromSupervisors(supervisors, "sup-" + supervisor);
                }else{
                    for(int slot:slots){
                        supervisors= TestUtilsForBlacklistScheduler.removePortFromSupervisors(supervisors, "sup-" + supervisor, slot);
                    }
                }
            }
            supervisorsList.add(supervisors);
        }
        return supervisorsList;
    }

    public static Cluster nextCluster(Cluster cluster,Map<String,SupervisorDetails> supervisors,INimbus iNimbus,Map config){
        Map<String, SchedulerAssignmentImpl> assigment;
        if(cluster==null) {
            assigment=new HashMap<String, SchedulerAssignmentImpl>();
        }else{
            assigment = TestUtilsForBlacklistScheduler.assignmentMapToImpl(cluster.getAssignments());
        }
        return new Cluster(iNimbus,supervisors,assigment,config);
    }
}

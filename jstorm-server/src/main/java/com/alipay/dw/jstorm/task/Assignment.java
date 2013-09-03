package com.alipay.dw.jstorm.task;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alipay.dw.jstorm.common.NodePort;
import com.alipay.dw.jstorm.common.JStormUtils;

/**
 * Assignment of one Toplogy, stored in /ZK-DIR/assignments/{topologyid}
 * nodeHost {supervisorid: hostname} -- assigned supervisor Map
 * taskStartTimeSecs: {taskid, taskStartSeconds} masterCodeDir: topology source
 * code's dir in Nimbus taskToNodeport: {taskid, nodeport}
 * 
 * @author Lixin/Longda
 */
public class Assignment implements Serializable {
    
    private static final long      serialVersionUID = 6087667851333314069L;
    
    private Map<String, String>    nodeHost;
    private Map<Integer, Integer>  taskStartTimeSecs;
    private String                 masterCodeDir;
    private Map<Integer, NodePort> taskToNodeport;
    
    public Assignment(String masterCodeDir,
            Map<Integer, NodePort> taskToNodeport,
            Map<String, String> nodeHost,
            Map<Integer, Integer> taskStartTimeSecs) {
        this.taskToNodeport = taskToNodeport;
        this.nodeHost = nodeHost;
        this.taskStartTimeSecs = taskStartTimeSecs;
        this.masterCodeDir = masterCodeDir;
    }
    
    public Map<String, String> getNodeHost() {
        return nodeHost;
    }
    
    public void setNodePorts(Map<String, String> nodeHost) {
        this.nodeHost = nodeHost;
    }
    
    public Map<Integer, Integer> getTaskStartTimeSecs() {
        return taskStartTimeSecs;
    }
    
    public void setTaskStartTimeSecs(Map<Integer, Integer> taskStartTimeSecs) {
        this.taskStartTimeSecs = taskStartTimeSecs;
    }
    
    public String getMasterCodeDir() {
        return masterCodeDir;
    }
    
    public void setMasterCodeDir(String masterCodeDir) {
        this.masterCodeDir = masterCodeDir;
    }
    
    public Map<Integer, NodePort> getTaskToNodeport() {
        return taskToNodeport;
    }
    
    public void setTaskToNodeport(Map<Integer, NodePort> taskToNodeport) {
        this.taskToNodeport = taskToNodeport;
    }
    
    /**
     * find taskToNodeport for every supervisorId (node)
     * 
     * @param supervisorId
     * @return Map<Integer, NodePort>
     */
    public Map<Integer, NodePort> getTaskToPortbyNode(String supervisorId) {
        
        Map<Integer, NodePort> taskToPortbyNode = new HashMap<Integer, NodePort>();
        if (taskToNodeport == null) {
            return null;
        }
        for (Entry<Integer, NodePort> entry : taskToNodeport.entrySet()) {
            String node = entry.getValue().getNode();
            if (node.equals(supervisorId)) {
                taskToPortbyNode.put(entry.getKey(), entry.getValue());
            }
        }
        return taskToPortbyNode;
    }
    
    @Override
    public boolean equals(Object assignment) {
        if (assignment instanceof Assignment
                && ((Assignment) assignment).getNodeHost().equals(nodeHost)
                && ((Assignment) assignment).getTaskStartTimeSecs().equals(
                        taskStartTimeSecs)
                && ((Assignment) assignment).getMasterCodeDir().equals(
                        masterCodeDir)
                && ((Assignment) assignment).getTaskToNodeport().equals(
                        taskToNodeport)) {
            return true;
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return this.nodeHost.hashCode() + this.taskStartTimeSecs.hashCode()
                + this.masterCodeDir.hashCode()
                + this.taskToNodeport.hashCode();
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }
}

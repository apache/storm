package com.alipay.dw.jstorm.common;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Worker's node and port
 */
public class NodePort implements Serializable, Comparable<NodePort> {
    
    private static final long serialVersionUID = 5768277681792471500L;
    
    private String            node;                                   // supervisorId
    private Integer           port;
    
    public NodePort(String node, Integer port) {
        this.node = node;
        this.port = port;
    }
    
    public String getNode() {
        return node;
    }
    
    public void setNode(String node) {
        this.node = node;
    }
    
    public Integer getPort() {
        return port;
    }
    
    public void setPort(Integer port) {
        this.port = port;
    }
    
    @Override
    public boolean equals(Object obj) {
        if ((obj instanceof NodePort) == false) {
            return false;
        }
        
        NodePort other = (NodePort)obj;
        if (node.equals(other.getNode()) == false) {
            return false;
        }
        
        if (port.equals(other.getPort()) == false) {
            return false;
        }
        
        return true;
    }
    
    @Override
    public int hashCode() {
        return this.node.hashCode() + this.port.hashCode();
    }
    
    @Override
    public int compareTo(NodePort other) {
        int result = node.compareTo(other.getNode());
        if (result != 0) {
            return result;
        }
        
        result = port.compareTo(other.getPort());
        if (result != 0) {
            return result;
        }
        
        return 0;
    }

    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
package com.alipay.dw.jstorm.cluster;

import java.util.List;
import java.util.UUID;

/**
 * All ZK interface
 * 
 * @author yannian
 * 
 */
public interface ClusterState {
    public void set_ephemeral_node(String path, byte[] data) throws Exception;
    
    public void delete_node(String path) throws Exception;
    
    public void set_data(String path, byte[] data) throws Exception;
    
    public byte[] get_data(String path, boolean watch) throws Exception;
    
    public List<String> get_children(String path, boolean watch)
            throws Exception;
    
    public void mkdirs(String path) throws Exception;
    
    public void close();
    
    public UUID register(ClusterStateCallback callback);
    
    public ClusterStateCallback unregister(UUID id);
}

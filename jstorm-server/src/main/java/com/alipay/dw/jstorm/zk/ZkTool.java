package com.alipay.dw.jstorm.zk;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.alipay.dw.jstorm.cluster.DistributedClusterState;
import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.common.JStormUtils;

public class ZkTool {
    private static Logger      LOG      = Logger.getLogger(ZkTool.class);
    
    public static final String READ_CMD = "read";
    
    public static void usage() {
        LOG.info(ZkTool.class.getName() + " read zkpath");
    }
    
    public static String getData(DistributedClusterState zkClusterState,
            String path) throws Exception {
        byte[] data = zkClusterState.get_data(path, false);
        if (data == null || data.length == 0) {
            return null;
        }
        
        Object obj = Utils.deserialize(data);
        
        return obj.toString();
    }
    
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        
        if (args.length < 2) {
            LOG.info("Invalid parameter");
            usage();
            return;
        }
        
        Map conf = Utils.readStormConfig();
        conf.put(Config.STORM_ZOOKEEPER_ROOT, "/");
        
        DistributedClusterState zkClusterState = new DistributedClusterState(
                conf);
        
        try {
            if (args[0].equalsIgnoreCase(READ_CMD)) {
                String path = args[1];
                
                String data = getData(zkClusterState, path);
                if (data == null) {
                    LOG.info("No data of " + path);
                }
                
                StringBuilder sb = new StringBuilder();
                
                sb.append("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
                sb.append("Zk node " + path + "\n");
                sb.append("Readable data:" + data + "\n");
                sb.append("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n");
                
                LOG.info(sb.toString());
                
            }
        } finally {
            zkClusterState.close();
        }
    }
    
}

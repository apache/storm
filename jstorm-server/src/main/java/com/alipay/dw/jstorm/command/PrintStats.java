package com.alipay.dw.jstorm.command;

import java.util.Map;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

import backtype.storm.Config;
import backtype.storm.generated.Nimbus;

import com.alipay.dw.jstorm.cluster.ClusterState;
import com.alipay.dw.jstorm.cluster.DistributedClusterState;
import com.alipay.dw.jstorm.cluster.StormClusterState;
import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.cluster.StormZkClusterState;
import com.alipay.dw.jstorm.task.error.TaskError;
import com.alipay.dw.jstorm.common.JStormUtils;

public class PrintStats {
    public static void main(String[] args) throws Exception {
        
        Map conf = StormConfig.read_storm_config();
        String host = String.valueOf(conf.get(Config.NIMBUS_HOST));
        Integer port = JStormUtils
                .parseInt(conf.get(Config.NIMBUS_THRIFT_PORT));
        TFramedTransport transport = new TFramedTransport(new TSocket(host,
                port));
        TBinaryProtocol prot = new TBinaryProtocol(transport);
        Nimbus.Client client = new Nimbus.Client(prot);
        transport.open();
        try {
            if (args[0].equals("cluster")) {
                System.out.println(client.getClusterInfo().toString());
                
            } else {
                String stromId = args[0];
                System.out.println(client.getTopologyInfo(stromId));
                
                ClusterState cluster_state = new DistributedClusterState(conf);
                StormClusterState zk = new StormZkClusterState(cluster_state);
                for (Integer taskid : zk.task_ids(stromId)) {
                    System.out.println("########" + taskid);
                    for (TaskError err : zk.task_errors(stromId, taskid)) {
                        System.out.println(err.getError());
                    }
                    
                }
                System.out.println("disconnect");
                
                zk.disconnect();
                
            }
        } finally {
            transport.close();
        }
        
    }
}

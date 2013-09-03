package com.alipay.dw.jstorm.command;

import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

import backtype.storm.Config;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;

import com.alipay.dw.jstorm.cluster.StormConfig;
import com.alipay.dw.jstorm.common.JStormUtils;

/**
 * kill topology client
 * 
 * Will be call by python client
 * 
 * @author yannian
 * 
 */
public class KillTopology {
    public static void main(String[] args) throws NotAliveException, TException {
        
        String name = args[0];
        KillOptions ops = new KillOptions();
        if (args.length > 1) {
            Integer wait = 0;
            wait = JStormUtils.parseInt(args[1]);
            ops.set_wait_secs(wait);
        }
        
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
            client.killTopologyWithOpts(name, ops);
        } finally {
            transport.close();
        }
        
    }
}

package com.alipay.dw.jstorm.task.comm;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.common.JStormUtils;
import com.alipay.dw.jstorm.task.TaskStatus;
import com.alipay.dw.jstorm.utils.RunCounter;
import com.alipay.dw.jstorm.zeroMq.IRecvConnection;
import com.alipay.dw.jstorm.zeroMq.ISendConnection;
import com.alipay.dw.jstorm.zeroMq.MQContext;
import com.alipay.dw.jstorm.zeroMq.PacketPair;

/**
 * Message dispatcher
 * 
 * @author yannian/Longda
 * 
 */
public class VirtualPortDispatch extends RunnableCallback {
    private final static Logger           LOG         = Logger.getLogger(VirtualPortDispatch.class);
    
    private MQContext                     mqContext;
    private IRecvConnection               recvConn;
    private Set<Integer>                  valid_ports = null;
    private Map<Integer, ISendConnection> sendConns;
    
    private RunCounter                    runCounter = new RunCounter("VirtualPortDispatch", VirtualPortDispatch.class);
    
    public VirtualPortDispatch(MQContext mqContext, IRecvConnection recvConn,
            Set<Integer> valid_ports) {
        this.mqContext = mqContext;
        this.recvConn = recvConn;
        this.valid_ports = valid_ports;
        
        sendConns = new HashMap<Integer, ISendConnection>();
    }
    
    public void cleanup() {
        LOG.info("Virtual port  received shutdown notice");
        byte shutdownCmd[] = {TaskStatus.SHUTDOWN};
        for (Entry<Integer, ISendConnection> entry : sendConns.entrySet()) {
            ISendConnection sendConn = entry.getValue();
            sendConn.send(shutdownCmd);
            sendConn.close();
        }
        
        recvConn.close();
        
        recvConn = null;
    }
    
    @Override
    public void run() {
        boolean hasTuple = false;
        
        while (true) {
            byte[] data = recvConn.recv();
            if (data == null || data.length == 0) {
                if (hasTuple == false) {
                    JStormUtils.sleepMs(1);
                }
                return ;
            }
            hasTuple = true;
            
            long before = System.currentTimeMillis();
            
            
            PacketPair packet = PacketPair.parse_packet(data);
            if (packet.getPort() == -1) {
                // shutdown message
                cleanup();
                
                return;
            }
            
            Integer port = (int)packet.getPort();
            //LOG.info("Get message to port " + port);
            if (valid_ports == null || valid_ports.contains(port)) {
                ISendConnection sendConn = sendConns.get(port);
                if (sendConn == null) {
                    sendConn = mqContext.connect(false, "localhost", port);
                    sendConns.put(port, sendConn);
                }
                sendConn.send(packet.getMessage());
                
            } else {
                LOG.warn("Received invalid message directed at port "
                        + packet.getPort() + ". Dropping...");
            }   
            
            long after = System.currentTimeMillis();
            runCounter.count(after - before);
        }
        
    }
    
    @Override
    public Object getResult() {
        return recvConn == null ? -1 : 0;
    }
}

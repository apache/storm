package com.alipay.dw.jstorm.daemon.worker;

import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.Tuple;

import com.alipay.dw.jstorm.zeroMq.PacketPair;

/**
 * Sending entrance
 * 
 * Task sending all tuples through this Object
 * 
 * Serialize the Tuple and put the serialized data to the sending queue
 * 
 * @author yannian
 * 
 */
public class WorkerTransfer {
    private LinkedBlockingQueue<TransferData> transferQueue;
    private KryoTupleSerializer               serializer;
    
    public WorkerTransfer(KryoTupleSerializer serializer,
            LinkedBlockingQueue<TransferData> _transfer_queue) {
        this.transferQueue = _transfer_queue;
        this.serializer = serializer;
    }
    
    public void transfer(int taskid, Tuple tuple) {
        byte [] tupleMessage = serializer.serialize(tuple);
        byte [] sendMessage = PacketPair.mk_packet((short)taskid, tupleMessage);
        TransferData tData = new TransferData(taskid, sendMessage);
        transferQueue.offer(tData);
    }
    
}

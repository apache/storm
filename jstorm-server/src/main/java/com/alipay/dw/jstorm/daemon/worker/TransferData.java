package com.alipay.dw.jstorm.daemon.worker;

public class TransferData {
    private int    taskid;
    private byte[] data;
    
    public TransferData(int taskid, byte[] data) {
        this.taskid = taskid;
        this.data = data;
    }
    
    public int getTaskid() {
        return taskid;
    }
    
    public byte[] getData() {
        return data;
    }
}

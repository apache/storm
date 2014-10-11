package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

import org.apache.log4j.Logger;

import backtype.storm.generated.WorkerMetricData;

import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.utils.JStormUtils;

public class WorkerMetrics implements Serializable{
	/**  */
	private static final long serialVersionUID = -5610156437643520186L;

	private static final Logger LOG = Logger.getLogger(WorkerMetrics.class);
	
    String hostName;
    int    port;
    double usedCpu;
    double usedMem;
    double nettyservDecodeTime;
    double nettyservMsgTransTime;
    double dispatchTime;
    double dispatchQueue;
    double batchTupleTime;
    double batchTupleQueue;
    double nettycliSendTime;
    double nettycliBatchSize;
    double nettycliSendPending;
    double nettycliSyncBatchQueue;
    double nettycliSyncDisrpQueue;
    
    public WorkerMetrics() {
    }

    public String getHostName() {
    	return hostName;
    }
    
    public void setHostName(String hostName) {
    	this.hostName = hostName;
    }
    
    public int getPort() {
    	return port;
    }
    
    public void setPort(int port) {
    	this.port = port;
    }
    
    public double getNettyservDecodeTime() {
    	return nettyservDecodeTime;
    }
    
    public void setNettyservDecodeTime(double value) {
    	this.nettyservDecodeTime = value;
    }
    
    public double getNettyservMsgTransTime() {
    	return nettyservMsgTransTime;
    }
    
    public void setNettyservMsgTransTime(double value) {
    	this.nettyservMsgTransTime = value;
    }
    
    public double getDispatchTime() {
        return 	dispatchTime;
    }
    
    public void setDispatchTime(double value) {
    	this.dispatchTime = value;
    }
    
    public double getDispatchQueue() {
    	return dispatchQueue;
    }
    
    public void setDispatchQueue(double value) {
    	this.dispatchQueue = value;
    }
    
    public double getBatchTupleTime() {
    	return batchTupleTime;
    }
    
    public void setBatchTupleTime(double value) {
    	this.batchTupleTime = value;
    }
    
    public double getBatchTupleQueue() {
    	return batchTupleQueue;
    }
    
    public void setBatchTupleQueue(double value) {
    	this.batchTupleQueue = value;
    }
    
    public double getNettycliSendTime() {
    	return nettycliSendTime;
    }
    
    public void setNettycliSendTime(double value) {
    	this.nettycliSendTime = value;
    }
    
    public double getNettycliBatchSize() {
    	return nettycliBatchSize;
    }
    
    public void setNettycliBatchSize(double value) {
    	this.nettycliBatchSize = value;
    }
    
    public double getNettycliSendPending() {
    	return nettycliSendPending;
    }
    
    public void setNettycliSendPending(double value) {
    	this.nettycliSendPending = value;
    }
    
    public double getNettycliSyncBatchQueue() {
    	return nettycliSyncBatchQueue;
    }
    
    public void setNettycliSyncBatchQueue(double value) {
    	this.nettycliSyncBatchQueue = value;
    }
    
    public double getNettycliSyncDisrpQueue() {
    	return nettycliSyncDisrpQueue;
    }
    
    public void setNettycliSyncDisrpQueue(double value) {
    	this.nettycliSyncDisrpQueue = value;
    }
    
    public double getUsedCpu() {
    	return usedCpu;
    }
    
    public double getusedMem() {
    	return usedMem;
    }

    public void updateWorkerMetricData(WorkerMetricData metricData) {
		hostName = metricData.get_hostname();
		port = metricData.get_port();
		
		usedCpu = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.CPU_USED_RATIO));
		usedMem = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.MEMORY_USED));
		usedMem = JStormUtils.formatDoubleDecPoint2(usedMem/(1024*1204));
		
		batchTupleQueue = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.BATCH_TUPLE_QUEUE));
		batchTupleTime = UIUtils.getDoubleValue(
				metricData.get_timer().get(MetricDef.BATCH_TUPLE_TIME));
		dispatchQueue = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.DISPATCH_QUEUE));
		dispatchTime = UIUtils.getDoubleValue(
				metricData.get_timer().get(MetricDef.DISPATCH_TIME));
		nettycliBatchSize = UIUtils.getDoubleValue(
				metricData.get_histogram().get(MetricDef.NETTY_CLI_BATCH_SIZE));
		nettycliSendTime = UIUtils.getDoubleValue(
				metricData.get_timer().get(MetricDef.NETTY_CLI_SEND_TIME));
		nettyservDecodeTime = UIUtils.getDoubleValue(
				metricData.get_timer().get(MetricDef.NETTY_SERV_DECODE_TIME));
		nettyservMsgTransTime = UIUtils.getDoubleValue(
				metricData.get_histogram().get(MetricDef.NETWORK_MSG_TRANS_TIME));
		nettycliSendPending = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.NETTY_CLI_SEND_PENDING));
		nettycliSyncBatchQueue = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.NETTY_CLI_SYNC_BATCH_QUEUE));
		nettycliSyncDisrpQueue = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.NETTY_CLI_SYNC_DISR_QUEUE));
    }
}
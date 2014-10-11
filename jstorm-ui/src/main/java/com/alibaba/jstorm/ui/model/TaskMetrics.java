package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.ui.UIUtils;

import backtype.storm.generated.*;

public class TaskMetrics implements Serializable{
    /**  */
	private static final long serialVersionUID = -113082870281288187L;
	
	int    taskId;
    String componentId;
    double deserializeQueue;
    double deserializeTime;
	double executorQueue;
	double executorTime;
	double serializeQueue;
	double serializeTime;
	double ackerTime;
	double emptyCpuRatio;
	double pendingNum;
	double emitTime;
	
	public TaskMetrics() {
	}
	
	public int getTaskId() {
		return taskId;
	}
	
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}
	
	public String getComponentId() {
		return componentId;
	}
	
	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}
	
	public double getDeserializeQueue() {
		return deserializeQueue;
	}
	
	public void setDeserializeQueue(double value) {
		this.deserializeQueue = value;
	}
	
	public double getDeserializeTime() {
		return deserializeTime;
	}
	
	public void setDeserializeTime(double value) {
		this.deserializeTime = value;
	}
	
	public double getExecutorQueue() {
		return executorQueue;
	}
	
	public void setExecutorQueue(double value) {
		this.executorQueue = value;
	}
	
	public double getExecutorTime() {
		return executorTime;
	}
	
	public void setExecutorTime(double value) {
		this.executorTime = value;
	}
	
	public double getSerializeQueue() {
		return serializeQueue;
	}
	
	public void setSerializeQueue(double value) {
		this.serializeQueue = value;
	}
	
	public double getSerializeTime() {
		return serializeTime;
	}

	public void setSerializeTime(double value) {
		this.serializeTime = value;
	}
	
	public double getAckerTime() {
		return ackerTime;
	}
	
	public void setAckerTime(double value) {
		this.ackerTime = value;
	}
	
	public double getEmptyCpuRatio() {
		return emptyCpuRatio;
	}
	
	public void setEmptyCpuRatio(double value) {
		this.emptyCpuRatio = value;
	}
	
	public double getPendingNum() {
		return pendingNum;
	}
	
	public void setPendingNum(double value) {
		this.pendingNum = value;
	}
	
	public double getEmitTime() {
		return emitTime;
	}
	
	public void setEmitTime(double value) {
		this.emitTime = value;
	}
	
	public void updateTaskMetricData(TaskMetricData metricData) {
        taskId = metricData.get_task_id();
        componentId = metricData.get_component_id();	
		deserializeQueue = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.DESERIALIZE_QUEUE));	
		deserializeTime = UIUtils.getDoubleValue(
				metricData.get_timer().get(MetricDef.DESERIALIZE_TIME));	
		executorQueue = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.EXECUTE_QUEUE));	
		executorTime = UIUtils.getDoubleValue(
				metricData.get_timer().get(MetricDef.EXECUTE_TIME));	
		serializeQueue = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.SERIALIZE_QUEUE));	
		serializeTime = UIUtils.getDoubleValue(
				metricData.get_timer().get(MetricDef.SERIALIZE_TIME));
		ackerTime = UIUtils.getDoubleValue(
				metricData.get_timer().get(MetricDef.ACKER_TIME));	
		emitTime = UIUtils.getDoubleValue(
				metricData.get_timer().get(MetricDef.EMIT_TIME));	
		emptyCpuRatio = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.EMPTY_CPU_RATIO));	
		pendingNum = UIUtils.getDoubleValue(
				metricData.get_gauge().get(MetricDef.PENDING_MAP));
	}
	
}
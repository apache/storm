/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.jstorm.ui.model;

import java.io.Serializable;

import backtype.storm.generated.GlobalStreamId;

import com.alibaba.jstorm.utils.JStormUtils;

/**
 * 
 * @author xin.zhou/Longda
 */
public class ComponentInput implements Serializable {

	private static final long serialVersionUID = -1415620236852453926L;

	private String componentId;
	private String stream;
	private String recvTps;
	private String process;
	private String acked;
	private String failed;

	public String getComponentId() {
		return componentId;
	}

	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}

	public String getStream() {
		return stream;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

	public String getRecvTps() {
		return recvTps;
	}

	public void setRecvTps(String recvTps) {
		this.recvTps = recvTps;
	}

	public String getProcess() {
		return process;
	}

	public void setProcess(String process) {
		this.process = process;
	}

	public String getAcked() {
		return acked;
	}

	public void setAcked(String acked) {
		this.acked = acked;
	}

	public String getFailed() {
		return failed;
	}

	public void setFailed(String failed) {
		this.failed = failed;
	}

	public void setValues(GlobalStreamId gstreamId, Double recvTps,
			Double process, Long acked, Long failed) {
		this.componentId = gstreamId.get_componentId();
		this.stream = gstreamId.get_streamId();
		this.recvTps = JStormUtils.formatValue(recvTps);
		this.acked = JStormUtils.formatValue(acked);
		this.failed = JStormUtils.formatValue(failed);
		this.process = JStormUtils.formatValue(process);

	}

}

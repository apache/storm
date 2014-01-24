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
public class SpoutOutput implements Serializable {

	/**
     * 
     */
	private static final long serialVersionUID = -5631649054937247856L;

	private String stream;
	private String emitted;
	private String sendTps;
	private String process;
	private String acked;
	private String failed;

	public String getEmitted() {
		return emitted;
	}

	public void setEmitted(String emitted) {
		this.emitted = emitted;
	}

	public String getSendTps() {
		return sendTps;
	}

	public void setSendTps(String sendTps) {
		this.sendTps = sendTps;
	}

	public String getStream() {
		return stream;
	}

	public void setStream(String stream) {
		this.stream = stream;
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

	public void setValues(String stream, Long emitted, Double sendTps,
			Double process, Long acked, Long failed) {
		this.stream = stream;
		this.emitted = JStormUtils.formatValue(emitted);
		this.sendTps = JStormUtils.formatValue(sendTps);
		this.acked = JStormUtils.formatValue(acked);
		this.failed = JStormUtils.formatValue(failed);
		this.process = JStormUtils.formatValue(process);

	}

}

package com.alibaba.jstorm.ui.model;

/**
 * componentpage:ComponentSummary
 *
 * @author xin.zhou
 *
 */
import java.io.Serializable;
import java.util.Map;

import com.alibaba.jstorm.common.stats.StaticsType;
import com.alibaba.jstorm.utils.JStormUtils;

public class ComponentStats implements Serializable {

	private static final long serialVersionUID = 2183846733736949858L;
	private String emitted;
	private String sendTps;
	private String recvTps;
	private String acked;
	private String failed;
	private String process;

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

	public String getRecvTps() {
		return recvTps;
	}

	public void setRecvTps(String recvTps) {
		this.recvTps = recvTps;
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

	public String getProcess() {
		return process;
	}

	public void setProcess(String process) {
		this.process = process;
	}

	public void setValues(Map<StaticsType, Object> staticsType) {
		emitted = JStormUtils.formatValue(staticsType.get(StaticsType.emitted));
		sendTps = JStormUtils
				.formatValue(staticsType.get(StaticsType.send_tps));
		recvTps = JStormUtils
				.formatValue(staticsType.get(StaticsType.recv_tps));
		acked = JStormUtils.formatValue(staticsType.get(StaticsType.acked));
		failed = JStormUtils.formatValue(staticsType.get(StaticsType.failed));
		process = JStormUtils.formatValue(staticsType
				.get(StaticsType.process_latencies));
	}

}

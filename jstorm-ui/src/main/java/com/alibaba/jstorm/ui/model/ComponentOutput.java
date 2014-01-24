package com.alibaba.jstorm.ui.model;

/**
 * componentpage:ComponentOutput
 *
 * @author xin.zhou
 *
 */
import java.io.Serializable;

import com.alibaba.jstorm.utils.JStormUtils;

public class ComponentOutput implements Serializable {

	private static final long serialVersionUID = 5607257248459397567L;

	private String stream;
	private String emitted;
	private String sendTps;

	public String getStream() {
		return stream;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

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

	public void setValues(String stream, Long emitted, Double sendTps) {
		this.stream = stream;
		this.emitted = JStormUtils.formatValue(emitted);
		this.sendTps = JStormUtils.formatValue(sendTps);
	}

}
package com.alibaba.jstorm.ui.model;

/**
 * topologypage:TopologyStats
 * 
 * @author xin.zhou
 * 
 */
import java.io.Serializable;

public class WinComponentStats extends ComponentStats implements Serializable {

	private static final long serialVersionUID = 4819784268512595428L;
	private String window;

	public String getWindow() {
		return window;
	}

	public void setWindow(String window) {
		this.window = window;
	}

}

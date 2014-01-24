package com.alibaba.jstorm.ui.model;

/**
 * topologypage:Spouts/Bolts
 * 
 * @author xin.zhou
 * 
 */
import java.io.Serializable;

public class Components extends ComponentStats implements Serializable {

	private static final long serialVersionUID = -5697993689701474L;

	private String type; // spout/bolt
	private String componetId;
	private String parallelism;
	private String lastError;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getComponetId() {
		return componetId;
	}

	public void setComponetId(String componetId) {
		this.componetId = componetId;
	}

	public String getParallelism() {
		return parallelism;
	}

	public void setParallelism(String parallelism) {
		this.parallelism = parallelism;
	}

	public String getLastError() {
		return lastError;
	}

	public void setLastError(String lastError) {
		this.lastError = lastError;
	}

}

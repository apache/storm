package com.alibaba.jstorm.ui.model;

/**
 * taskpage:ErrorSummary
 *
 * @author xin.zhou
 *
 */
import java.io.Serializable;

public class ErrorSummary implements Serializable {

	private static final long serialVersionUID = -4762717099406678507L;
	private String time;
	private String error;

	public ErrorSummary(String time, String error) {
		this.time = time;
		this.error = error;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}
}

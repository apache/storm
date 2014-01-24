package com.alibaba.jstorm.stats.rolling;

public class UpdateParams {
	private Object[] args;
	private Object curr;

	public Object[] getArgs() {
		return args;
	}

	public void setArgs(Object[] args) {
		this.args = args;
	}

	public Object getCurr() {
		return curr;
	}

	public void setCurr(Object curr) {
		this.curr = curr;
	}

}

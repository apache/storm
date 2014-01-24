package com.alibaba.jstorm.daemon.worker;

/**
 * Worker's status
 * 
 * @author chenjun
 * 
 */
public enum State {
	valid, disallowed, notStarted, timedOut;
}

package com.alibaba.jstorm.client.spout;

import java.util.List;

/**
 * This interface will list emit values when tuple success
 * 
 * if spout implement this interface, 
 * spout won't call ISpout.ack() when tuple success
 * 
 * @author longda
 */
public interface IAckValueSpout {
	void ack(Object msgId, List<Object> values);
}

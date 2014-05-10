package com.alibaba.jstorm.client.spout;

import java.util.List;

/**
 * This interface will list emit values when tuple fails
 * 
 * if spout implement this interface, 
 * spout won't call ISpout.fail() when tuple fail
 * 
 * @author longda
 */
public interface IFailValueSpout {
	void fail(Object msgId, List<Object> values);
}

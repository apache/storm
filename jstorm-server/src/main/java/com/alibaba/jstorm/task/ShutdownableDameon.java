package com.alibaba.jstorm.task;

import backtype.storm.daemon.Shutdownable;

import com.alibaba.jstorm.cluster.DaemonCommon;

public interface ShutdownableDameon extends Shutdownable, DaemonCommon,
		Runnable {

}

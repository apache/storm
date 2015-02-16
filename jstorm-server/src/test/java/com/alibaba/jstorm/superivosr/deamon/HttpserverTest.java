package com.alibaba.jstorm.superivosr.deamon;

import java.util.HashMap;

import org.junit.Test;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.daemon.supervisor.Httpserver;
import com.google.common.collect.Maps;

public class HttpserverTest {
	
	@Test
	public void test_httpserver() {
		int port = ConfigExtension.getSupervisorDeamonHttpserverPort(Maps.newHashMap());
		Httpserver httpserver = new Httpserver(port, new HashMap<String, Object>());
		httpserver.start();
		System.out.println("start....");
		
		httpserver.shutdown();
	}
	
	
	
}

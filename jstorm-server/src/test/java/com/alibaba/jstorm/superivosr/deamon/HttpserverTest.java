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
	}
	
	enum ViewMode {
		LAST_4K, LAST_8K, ALL;
		
//		final String mode;
//
//		ViewMode(String mode) {
//			this.mode = mode;
//		}
//		
//		String getMode() {
//			return mode;
//		}
	}
	
	@Test
	public void test() {
//		System.out.println(ViewMode.LAST_4K.getMode());
		System.out.println(Enum.valueOf(ViewMode.class, "LAST_4K"));
	}
	
}

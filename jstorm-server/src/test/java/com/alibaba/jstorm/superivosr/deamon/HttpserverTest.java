package com.alibaba.jstorm.superivosr.deamon;

import org.junit.Test;

import com.alibaba.jstorm.daemon.supervisor.Httpserver;
import com.google.common.collect.Maps;

public class HttpserverTest {
	
	@Test
	public void test_httpserver() {
		Httpserver httpserver = new Httpserver(Maps.newHashMap());
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

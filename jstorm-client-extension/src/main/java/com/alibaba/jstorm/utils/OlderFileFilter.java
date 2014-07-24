package com.alibaba.jstorm.utils;

import java.io.File;
import java.io.FileFilter;

/**
 * filter the older file, skip the files' modify time which is less sec than now
 * 
 * @author lixin
 * 
 */
public class OlderFileFilter implements FileFilter {

	private int seconds;

	public OlderFileFilter(int seconds) {
		this.seconds = seconds;
	}

	@Override
	public boolean accept(File pathname) {

		long current_time = System.currentTimeMillis();

		return (pathname.isFile() && (pathname.lastModified() + seconds * 1000 <= current_time))
				|| pathname.isDirectory();
	}

}

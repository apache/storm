package com.alibaba.jstorm.schedule;

import java.io.File;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.utils.OlderFileFilter;

/**
 * clean /nimbus/inbox jar every 600 seconds
 * 
 * Default expire time is 3600 seconds
 * 
 * @author lixin
 * 
 */
public class CleanRunnable implements Runnable {

	private static Logger log = Logger.getLogger(CleanRunnable.class);

	private String dir_location;

	private int seconds;

	public CleanRunnable(String dir_location, int inbox_jar_expiration_secs) {
		this.dir_location = dir_location;
		this.seconds = inbox_jar_expiration_secs;
	}

	@Override
	public void run() {
		File inboxdir = new File(dir_location);
		clean(inboxdir);
	}

	private void clean(File file) {
		// filter
		OlderFileFilter filter = new OlderFileFilter(seconds);

		File[] files = file.listFiles(filter);
		for (File f : files) {
			if (f.isFile()) {
				log.info("Cleaning inbox ... deleted: " + f.getName());
				try {
					f.delete();
				} catch (Exception e) {
					log.error("Cleaning inbox ... error deleting:"
							+ f.getName() + "," + e);
				}
			} else {
				clean(f);
				if (f.listFiles().length == 0) {
					log.info("Cleaning inbox ... deleted: " + f.getName());
					try {
						f.delete();
					} catch (Exception e) {
						log.error("Cleaning inbox ... error deleting:"
								+ f.getName() + "," + e);
					}
				}
			}
		}
	}

}

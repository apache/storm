package org.apache.storm.scheduler.blacklist.reporter;

import org.apache.storm.scheduler.blacklist.CircularBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by howard.li on 2016/7/13.
 */
public class LogReporter implements IReporter {
    private static Logger LOG = LoggerFactory.getLogger(LogReporter.class);
    @Override
    public void report(String message) {
        LOG.info(message);
    }

    @Override
    public void reportBlacklist(String supervisor, CircularBuffer<HashMap<String, Set<Integer>>> toleranceBuffer) {
        String message="add supervisor "+supervisor+" to blacklist. The bad slot history of supervisors is :"+toleranceBuffer;
        report(message);
    }
}

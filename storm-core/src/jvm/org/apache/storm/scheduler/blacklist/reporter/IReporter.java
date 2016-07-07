package org.apache.storm.scheduler.blacklist.reporter;

/**
 * Created by howard.li on 2016/7/13.
 */

import org.apache.storm.scheduler.blacklist.CircularBuffer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/**
 * report blacklist to alert system
 */
public interface IReporter {
    public void report(String message);
    public void reportBlacklist(String supervisor,CircularBuffer<HashMap<String,Set<Integer>>> toleranceBuffer);
}

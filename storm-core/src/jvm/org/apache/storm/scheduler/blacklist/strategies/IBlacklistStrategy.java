package org.apache.storm.scheduler.blacklist.strategies;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.blacklist.CircularBuffer;
import org.apache.storm.scheduler.blacklist.reporters.IReporter;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by howard.li on 2016/7/13.
 */
public interface IBlacklistStrategy {

    public void prepare(IReporter reporter ,int toleranceTime,int toleranceCount,int resumeTime,int nimbusMonitorFreqSecs);

    public Set<String> getBlacklist(CircularBuffer<HashMap<String,Set<Integer>>> toleranceBuffer,Cluster cluster, Topologies topologies);
    public void resumeFromBlacklist();

    //public void releaseBlacklistWhenNeeded(Cluster cluster,Topologies topologies);
}

package org.apache.storm.utils;

/**
 * Created by rfarivar on 1/29/16.
 */
public class UptimeComputer {
    int startTime = 0;

    public UptimeComputer() {
        startTime = Utils.currentTimeSecs();
    }

    public int upTime() {
        return Time.delta(startTime);
    }
}

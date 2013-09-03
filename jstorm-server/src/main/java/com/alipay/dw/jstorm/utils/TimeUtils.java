package com.alipay.dw.jstorm.utils;

import backtype.storm.utils.Time;

/**
 * Time utils
 * 
 * @author yannian
 * 
 */
public class TimeUtils {
    
    public static int current_time_secs() {
        return (int) (Time.currentTimeMillis() / 1000);
    }
    
    public static int time_delta(int time_secs) {
        return current_time_secs() - time_secs;
    }
    
    public static long time_delta_ms(long time_ms) {
        return System.currentTimeMillis() - time_ms;
    }
}

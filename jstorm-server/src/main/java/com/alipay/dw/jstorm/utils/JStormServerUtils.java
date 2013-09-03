package com.alipay.dw.jstorm.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;

import com.alipay.dw.jstorm.callback.AsyncLoopDefaultKill;
import com.alipay.dw.jstorm.callback.RunnableCallback;

/**
 * storm utils
 * 
 * 
 * @author yannian/Longda/Xin.Zhou/Xin.Li
 * 
 */
public class JStormServerUtils {
    
    private static final Logger LOG = Logger.getLogger(JStormServerUtils.class);
    
    public static RunnableCallback getDefaultKillfn() {
        
        return new AsyncLoopDefaultKill();
    }
    
    public static TreeMap<Integer, Integer> integer_divided(int sum,
            int num_pieces) {
        return Utils.integerDivided(sum, num_pieces);
    }
    
    public static void sleep_secs(long secs) throws InterruptedException {
        Time.sleep(1000 * secs);
    }
    
    public static <K, V> HashMap<K, V> filter_val(RunnableCallback fn,
            Map<K, V> amap) {
        HashMap<K, V> rtn = new HashMap<K, V>();
        
        for (Entry<K, V> entry : amap.entrySet()) {
            V value = entry.getValue();
            Object result = fn.execute(value);
            
            if (result == (Boolean) true) {
                rtn.put(entry.getKey(), value);
            }
        }
        return rtn;
    }
    
}

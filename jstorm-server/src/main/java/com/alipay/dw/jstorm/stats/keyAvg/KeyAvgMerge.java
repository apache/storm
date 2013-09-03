package com.alipay.dw.jstorm.stats.keyAvg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.stats.Pair;
import com.alipay.dw.jstorm.stats.StatFunction;

public class KeyAvgMerge extends RunnableCallback {
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> Object execute(T... args) {
        List<Map<Object, Pair>> list = (List<Map<Object, Pair>>) args[0];
        
        Map<Object, Pair> result = new HashMap<Object, Pair>();
        
        Map<Object, List<Pair>> trans = new HashMap<Object, List<Pair>>();
        
        for (Map<Object, Pair> each : list) {
            
            for (Entry<Object, Pair> e : each.entrySet()) {
                
                Object key = e.getKey();
                List<Pair> val = trans.get(key);
                if (val == null) {
                    val = new ArrayList<Pair>();
                }
                val.add(e.getValue());
                trans.put(key, val);
            }
        }
        
        for (Entry<Object, List<Pair>> e : trans.entrySet()) {
            result.put(e.getKey(), StatFunction.merge_keyed_avg(e.getValue()));
        }
        return result;
    }
}

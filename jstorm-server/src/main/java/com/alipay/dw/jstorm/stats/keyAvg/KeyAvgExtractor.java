package com.alipay.dw.jstorm.stats.keyAvg;

import java.util.HashMap;
import java.util.Map;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.stats.Pair;
import com.alipay.dw.jstorm.stats.StatFunction;

public class KeyAvgExtractor extends RunnableCallback {
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> Object execute(T... args) {
        Map<Object, Double> result = null;
        if (args != null && args.length > 0) {
            Map<Object, Pair> v = (Map<Object, Pair>) args[0];
            result = StatFunction.extract_key_avg(v);
        }
        
        if (result == null) {
            result = new HashMap<Object, Double>();
        }
        
        return result;
    }
}

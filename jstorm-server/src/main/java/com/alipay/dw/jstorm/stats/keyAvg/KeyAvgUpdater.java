package com.alipay.dw.jstorm.stats.keyAvg;

import java.util.HashMap;
import java.util.Map;

import com.alipay.dw.jstorm.callback.RunnableCallback;
import com.alipay.dw.jstorm.stats.Pair;
import com.alipay.dw.jstorm.stats.StatFunction;
import com.alipay.dw.jstorm.stats.rolling.UpdateParams;

public class KeyAvgUpdater extends RunnableCallback {
    
    @SuppressWarnings("unchecked")
    @Override
    public <T> Object execute(T... args) {
        Map<Object, Pair> curr = null;
        if (args != null && args.length > 0) {
            UpdateParams p = (UpdateParams) args[0];
            if (p.getCurr() != null) {
                curr = (Map<Object, Pair>) p.getCurr();
            } else {
                curr = new HashMap<Object, Pair>();
            }
            Object[] keyAvgArgs = p.getArgs();
            
            Long amt = 1l;
            if (keyAvgArgs.length > 1) {
                amt = Long.parseLong(String.valueOf(keyAvgArgs[1]));
            }
            StatFunction.update_keyed_avg(curr, keyAvgArgs[0], amt);
        }
        return curr;
    }
}

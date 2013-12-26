package com.alipay.dw.jstorm.example;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.IntervalCheck;

public class TpsCounter {
    public static Logger  LOG           = LoggerFactory.getLogger(TpsCounter.class);
    
    private AtomicLong    total         = new AtomicLong(0);
    private AtomicLong    tps           = new AtomicLong(0);
    private AtomicLong    spend         = new AtomicLong(0);
    
    private IntervalCheck intervalCheck = new IntervalCheck();
    
    private String        id;
    
    public TpsCounter(String id) {
        this.id = id;
        
    }
    
    public void count() {
        long totalValue = total.incrementAndGet();
        long tpsValue = tps.incrementAndGet();
        
        if (intervalCheck.check() == true) {
            LOG.info(id + " tps " + tpsValue / intervalCheck.getInterval()
                    + ", total:" + totalValue);
            tps.set(0);
        }
    }
    
    public void count(long value) {
        long totalValue = total.incrementAndGet();
        long tpsValue = tps.incrementAndGet();
        long spendValue = spend.addAndGet(value);
        
        if (intervalCheck.check() == true) {
            StringBuilder sb = new StringBuilder();
            sb.append(id);
            sb.append(" tps:" + tpsValue / intervalCheck.getInterval());
            sb.append(", total:" + totalValue);
            sb.append(", spend:" + spendValue/tpsValue);
            LOG.info(sb.toString());
            tps.set(0);
            spend.set(0);
        }
    }
    
    public void cleanup() {
        
        LOG.info(id + ", total:" + total);
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
    }
    
}

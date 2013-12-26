package com.alibaba.jstorm.utils;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;


public class RunCounter {
    
    
    private AtomicLong    total         = new AtomicLong(0);
    private AtomicLong    times           = new AtomicLong(0);
    private AtomicLong    values        = new AtomicLong(0);
    
    
    private IntervalCheck intervalCheck;
    
    private final String        id;
    private final Logger        LOG;
    
    public RunCounter() {
        this("", RunCounter.class);
    }
    
    public RunCounter(String id) {
        this(id, RunCounter.class);
    }
    
    public RunCounter(Class tclass) {
        this("", tclass);
        
    }
    
    public RunCounter(String id, Class tclass) {
        this.id = id;
        this.LOG = Logger.getLogger(tclass);
        
        intervalCheck = new IntervalCheck();
        intervalCheck.setInterval(60);
    }
    
    public void count(long value) {
        long totalValue = total.incrementAndGet();
        long timesValue = times.incrementAndGet();
        long v = values.addAndGet(value);
        
        Double pass = intervalCheck.checkAndGet();
        if (pass != null) {
            times.set(0);
            values.set(0);
            
            StringBuilder sb = new StringBuilder();
            sb.append(id);
            sb.append(", tps:" + timesValue / pass);
            sb.append(", avg:" + ((double)v)/timesValue);
            sb.append(", total:" + totalValue);
            LOG.info(sb.toString());
            
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

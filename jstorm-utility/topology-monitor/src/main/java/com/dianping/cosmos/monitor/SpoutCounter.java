package com.dianping.cosmos.monitor;

import java.util.concurrent.atomic.AtomicLong;

public class SpoutCounter {
    private AtomicLong repeatCounter = new AtomicLong(0l);
    private AtomicLong tupleCounter = new AtomicLong(0l);
    
    public void incrRepeatCounter(){
        repeatCounter.incrementAndGet();
    }
    
    public long getRepeatCounter(){
        return repeatCounter.get();
    }
    
    public void incrTupleCounter(long increment){
        tupleCounter.addAndGet(increment);
    }
    
    public long getTupleCounter(){
        return tupleCounter.get();
    }
}

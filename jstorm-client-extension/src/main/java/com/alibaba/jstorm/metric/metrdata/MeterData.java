package com.alibaba.jstorm.metric.metrdata;

import java.io.Serializable;


public class MeterData implements Serializable {

	private static final long serialVersionUID = 954627168057659269L;
	
	private long count;
    private double meanRate;
    private double oneMinuteRate;
    private double fiveMinuteRate;
    private double fifteenMinuteRate;
    
    public MeterData() {
    }
    
    public void setCount(long count) {
    	this.count = count;
    }
    
    public long getCount() {
    	return this.count;
    }
    
    public void setMeanRate(double meanRate) {
    	this.meanRate = meanRate;
    }
    
    public double getMeanRate() {
    	return this.meanRate;
    }
    
    public void setOneMinuteRate(double oneMinuteRate) {
    	this.oneMinuteRate = oneMinuteRate;
    }
    
    public double getOneMinuteRate() {
    	return this.oneMinuteRate;
    }
    
    public void setFiveMinuteRate(double fiveMinuteRate) {
    	this.fiveMinuteRate = fiveMinuteRate;
    }
    
    public double getFiveMinuteRate() {
    	return this.fiveMinuteRate;
    }
    
    public void setFifteenMinuteRate(double fifteenMinuteRate) {
    	this.fifteenMinuteRate = fifteenMinuteRate;
    }
    
    public double getFifteenMinuteRate() {
    	return this.fifteenMinuteRate;
    }
}
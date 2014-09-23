package com.alibaba.jstorm.metric.metrdata;

import java.io.Serializable;


public class HistogramData implements Serializable {

	private static final long serialVersionUID = 954627168057639289L;
	
	private long count;
	private long min;
	private long max;
	private double mean;
    private double stdDev;
    private double median;
    private double percent75th;
    private double percent95th;
    private double percent98th;
    private double percent99th;
    private double percent999th;
	
	public HistogramData() {
	}
	
	public long getCount() {
		return count;
	}
	
	public void setCount(long count) {
		this.count = count;
	}
	
	public long getMin() {
		return min;
	}
	
	public void setMin(long min) {
		this.min = min;
	}
	
	public long getMax() {
		return max;
	}
	
	public void setMax(long max) {
		this.max = max;
	}
	
	public double getMean() {
		return mean;
	}
	
	public void setMean(double mean) {
		this.mean = mean;
	}
	
	public double getStdDev() {
		return stdDev;
	}
	
	public void setStdDev(double stdDev) {
		this.stdDev = stdDev;
	}
	
	public double getMedian() {
		return median;
	}
	
	public void setMedian(double median) {
		this.median = median;
	}
	
	public double getPercent75th() {
		return percent75th;
	}
	
	public void setPercent75th(double percent75th) {
		this.percent75th = percent75th;
	}
	
	public double getPercent95th() {
		return percent95th;
	}
	
	public void setPercent95th(double percent95th) {
		this.percent95th = percent95th;
	}
	
	public double getPercent98th() {
		return percent98th;
	}
	
	public void setPercent98th(double percent98th) {
		this.percent98th = percent98th;
	}
	
	public double getPercent99th() {
		return percent99th;
	}
	
	public void setPercent99th(double percent99th) {
		this.percent99th = percent99th;
	}
	
	public double getPercent999th() {
		return percent999th;
	}
	
	public void setPercent999th(double percent999th) {
		this.percent999th = percent999th;
	}
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.metric;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.common.metric.Histogram;
import com.alibaba.jstorm.common.metric.MetricRegistry;
import com.alibaba.jstorm.common.metric.window.Metric;

public class SimpleJStormMetric extends JStormMetrics implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(SimpleJStormMetric.class);
    
    protected static MetricRegistry metrics = JStormMetrics.workerMetrics;
    static {
        Metric.setEnable(true);
    }
    
    protected static SimpleJStormMetric instance = null;
    
    
    public static SimpleJStormMetric mkInstance() {
        synchronized (SimpleJStormMetric.class) {
            if (instance == null) {
                instance = new SimpleJStormMetric();
            }
            
            return instance;
        }
    }
    
    protected SimpleJStormMetric() {
        
    }
    
    public static Histogram registerHistorgram(String name) {
        return JStormMetrics.registerWorkerHistogram(name);
    }
    
    public static void updateHistorgram(String name, Number obj) {
        LOG.debug(name  + ":" + obj.doubleValue());
        Histogram histogram =  (Histogram)metrics.getMetric(name);
        if (histogram == null) {
        	try {
        		histogram = registerHistorgram(name);
        	}catch(Exception e) {
        		LOG.info("{} has been register", name);
        		return;
        	}
        }
        
        histogram.update(obj);
        
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        Map<String, Metric> map = metrics.getMetrics();
        
        for (Entry<String, Metric> entry : map.entrySet()) {
            String key = entry.getKey();
            Metric metric = entry.getValue();
            
            LOG.info(key + ":" +  metric.getSnapshot());
        }
    }
    
    
    public static void main(String[] args) {
        updateHistorgram("test", 11100.0);
        
        SimpleJStormMetric instance = new SimpleJStormMetric();
        
        instance.run();
    }
}

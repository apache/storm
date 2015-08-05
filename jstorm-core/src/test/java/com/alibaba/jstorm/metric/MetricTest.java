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

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.MetricInfo;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.common.metric.Counter;
import com.alibaba.jstorm.common.metric.Histogram;
import com.alibaba.jstorm.common.metric.LongCounter;
import com.alibaba.jstorm.common.metric.Meter;
import com.alibaba.jstorm.common.metric.MetricRegistry;
import com.alibaba.jstorm.common.metric.window.Metric;
import com.alibaba.jstorm.utils.JStormUtils;

public class MetricTest {
    private static final Logger LOG = LoggerFactory.getLogger(MetricTest.class);
    static int[] windows = new int[4];
    
    static {
        windows[0] = 60;
        for (int i = 1; i < windows.length; i++) {
            windows[i] = windows[i - 1] * 2;
        }
        System.out.println("Start Test " + new Date());
    }
    static int   interval = windows[0]/20;

    public void check_value(Map<Integer, Number> map, Long[] result) {
        int i = 0;
        for (Integer key : map.keySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Time window ").append(key).append(" should be ");
            sb.append(result[i]).append(", but it is ").append(map.get(key));
            Assert.assertTrue(sb.toString(), map.get(key).equals(result[i]));
        }

    }
    

    public Output testTimes(Input input) {
        int firstRunSeconds = input.firstRunSeconds;
        int interruptSeconds = input.interruptSeconds;
        int endRunSeconds = input.endRunSeconds;
        
        System.out.println("Start test " + new Date() + ", first:" + firstRunSeconds + ", interrupt:" + interruptSeconds + ", endRun:" + endRunSeconds);
        final long intervalTimes = 1000000l;

        long firstCounter = 0l;
        long start = System.currentTimeMillis();
        long now = System.currentTimeMillis();
        long firstStop = start + firstRunSeconds * 1000;
        Metric metric = input.metric;
        while(now < firstStop) {
            for (long l = 0; l < intervalTimes; l++) {
                metric.update(1.0d);
            }
            firstCounter += intervalTimes;
            now = System.currentTimeMillis();
        }
        
        try {
            Thread.sleep(interruptSeconds * 1000); 
        }catch(Exception e) {
            e.printStackTrace();
        }
        
        now = System.currentTimeMillis();
        long endStop = now + endRunSeconds * 1000;
        long secondCounter = 0l;
        while(now < endStop) {
            for (long l = 0; l < intervalTimes; l++) {
                metric.update(1.0d);
            }
            secondCounter += intervalTimes;
            now = System.currentTimeMillis();
        }
        
        long end = System.currentTimeMillis();
        long totalCounter = firstCounter + secondCounter;
        Output output = new Output();
        
        output.output = metric.getSnapshot();
        output.firstCounter = firstCounter;
        output.secondCounter = secondCounter;
        System.out.println("update " + totalCounter + " cost " + (end - start) / 1000 + ", firstCounter:" + firstCounter + ", endCounter:" + secondCounter);
        System.out.println("End test " + new Date());
        return output;
        
    }
    
    public void testUpdates(Input input) {
        int firstRunSeconds = input.firstRunSeconds;
        int interruptSeconds = input.interruptSeconds;
        int endRunSeconds = input.endRunSeconds;
        
        System.out.println("Start test " + new Date() + ", first:" + firstRunSeconds + ", interrupt:" + interruptSeconds + ", endRun:" + endRunSeconds);
        final long intervalTimes = 1000000l;

        long firstCounter = 0l;
        long start = System.currentTimeMillis();
        long now = System.currentTimeMillis();
        long firstStop = start + firstRunSeconds * 1000;
        Metric metric = input.metric;
        while(now < firstStop) {
            for (long l = 0; l < intervalTimes; l++) {
                metric.update(1.0d);
            }
            firstCounter += intervalTimes;
            now = System.currentTimeMillis();
        }
        
        try {
            Thread.sleep(interruptSeconds * 1000); 
        }catch(Exception e) {
            e.printStackTrace();
        }
        
        now = System.currentTimeMillis();
        long endStop = now + endRunSeconds * 1000;
        long secondCounter = 0l;
        while(now < endStop) {
            for (long l = 0; l < intervalTimes; l++) {
                metric.update(1.0d);
            }
            secondCounter += intervalTimes;
            now = System.currentTimeMillis();
        }
        
        long end = System.currentTimeMillis();
        long totalCounter = firstCounter + secondCounter;
        Output output = new Output();
        
        
        System.out.println("update " + totalCounter + " cost " + (end - start) / 1000 + ", firstCounter:" + firstCounter + ", endCounter:" + secondCounter);
        System.out.println("End test " + new Date());
        return ;
        
    }

    public Metric getMetric(int type) {
        Metric metric = null;
        if (type == 0) {
            Counter<Double> counter = new Counter<Double>(0.0d);
            metric = counter;
        }else if (type == 1) {
            metric = new Meter();
        }else if (type == 2) {
            metric = new Histogram();
        }else {
            return null;
        }
        metric.setWindowSeconds(windows);
        metric.init();
        return metric;
        
        
    }
    
    public Metric getJStormMetric(int type) {
        Metric metric = null;
        if (type == 0) {
            metric = JStormMetrics.registerWorkerCounter("Counter");
        }else if (type == 1) {
            metric = JStormMetrics.registerWorkerMeter("Meter");
        }else if (type == 2) {
            metric = JStormMetrics.registerWorkerHistogram("Historgram");
        }else {
            return null;
        }
        
        metric.setWindowSeconds(windows);
        metric.init();
        return metric;
        
        
    }
    
    public Metric getJStormMetric(int type, String name) {
        Metric metric = null;
        if (type == 0) {
            metric = JStormMetrics.registerWorkerCounter("Counter", name);
        }else if (type == 1) {
            metric = JStormMetrics.registerWorkerMeter("Meter", name);
        }else if (type == 2) {
            metric = JStormMetrics.registerWorkerHistogram("Historgram", name);
        }else {
            return null;
        }
        
        
        return metric;
        
        
    }
    
    public void unregisterJStormMetric(int type) {
        if (type == 0) {
            JStormMetrics.unregisterWorkerMetric("Counter");
        }else if (type == 1) {
            JStormMetrics.unregisterWorkerMetric("Meter");
        }else if (type == 2) {
            JStormMetrics.unregisterWorkerMetric("Historgram");
        }else {
            return ;
        }
        return ;
        
        
    }
    
    
    public void test_speed() {
        for (int m = 0; m < 2; m++) {
            Input input = new Input();
            input.metric = getMetric(m);
            input.firstRunSeconds = 20;
            input.interruptSeconds = 0;
            input.endRunSeconds = 0;
            Output result = testTimes(input);
            System.out.println(input.metric.getClass().getSimpleName() + ":"
                    + result.output);

        }
    }

    // this test will cost too much time to run
    //@Test
    public void test_single_thread() {
        LOG.info("Begin to test");
        //int[] timeWindow = {0, interval/2, interval, interval + 1, windows[0]/2, windows[0], windows[0] + 1};
        int[] timeWindow = {0, windows[0]};
        
        for (int m = 2; m >= 0; m--) {
            for (int i = 0; i < timeWindow.length; i++) {
     //           for (int j = 0; j < timeWindow.length; j++) {
                    int j = 0;
                    for (int l = 0; l < timeWindow.length; l++) {
                        Input input = new Input();
                        input.metric = getJStormMetric(m);
                        input.firstRunSeconds = timeWindow[i];
                        input.interruptSeconds = timeWindow[j];
                        input.endRunSeconds = timeWindow[l];
                        Output result = testTimes(input);
                        System.out.println(input.metric.getClass()
                                .getSimpleName() + ":" + result.output);
                        unregisterJStormMetric(m);
                    }

    //            }
            }
        }
        
    }
    
    public void test_one_case() {
        int[] timeWindow = {0, windows[0]};
        int m = 1, i = 1, j = 0, l = 1;
        Input input = new Input();
        input.metric = getJStormMetric(m);
        input.metric.setWindowSeconds(windows);
        input.firstRunSeconds = timeWindow[i];
        input.interruptSeconds = timeWindow[j];
        input.endRunSeconds = timeWindow[l];
        Output result = testTimes(input);
        System.out.println(input.metric.getClass()
                .getSimpleName() + ":" + result.output);
        unregisterJStormMetric(m);
    }
    
    public void alltest() {
        test_one_case();
        //test_single_thread();
        //testMultipleThread();
    }
    
    public MetricInfo computWorkerMetrics(MetricRegistry workerMetrics) {
        MetricInfo workerMetricInfo = MetricThrift.mkMetricInfo();
        Map<String, Metric> workerMetricMap = workerMetrics.getMetrics();
        for (Entry<String, Metric> entry : workerMetricMap.entrySet()) {
            String name = entry.getKey();
            Map<Integer, Double> snapshot = entry.getValue().getSnapshot();

            MetricThrift.insert(workerMetricInfo, name, snapshot);
        }
        
        System.out.println(Utils.toPrettyJsonString(workerMetricInfo));
        return workerMetricInfo;
    }
    
    public void testMultipleThread() {
        LOG.info("Begin to test");
        //int[] timeWindow = {0, interval/2, interval, interval + 1, windows[0]/2, windows[0], windows[0] + 1};
        int[] timeWindow = {0, windows[0]};
        
        //for (int m = 2; m >= 0; m--) {
            for (int i = 0; i < timeWindow.length; i++) {
                for (int j = 0; j < timeWindow.length; j++) {
                    //int j = 0;
                    for (int l = 0; l < timeWindow.length; l++) {
                        
                        final Input input = new Input();
                        input.metric = getJStormMetric(1, String.valueOf(i) + ":" + String.valueOf(j) + ":" + String.valueOf(l));
                        input.firstRunSeconds = timeWindow[i];
                        input.interruptSeconds = timeWindow[j];
                        input.endRunSeconds = timeWindow[l];
                        
                        Thread thread = new Thread(new Runnable(){

                            @Override
                            public void run() {
                                // TODO Auto-generated method stub
                                testUpdates(input);
                            }
                            
                        }
                                );
                        
                        thread.start();
                        
                        
                    }

                //}
            }
        }
        
        for (int i = 0; i < 4 * windows[0]/60 ;  i ++) {
            MetricInfo metricInfo = computWorkerMetrics(JStormMetrics.workerMetrics);
            JStormUtils.sleepMs(60 * 1000);
        }
        
        JStormUtils.haltProcess(0);
        
    }
    
    public static class Input {
        public Metric metric;
        public int firstRunSeconds;
        public int interruptSeconds;
        public int endRunSeconds;
    }
    
    public static class Output {
        public Object output;
        public long firstCounter;
        public long secondCounter;
    }

    public static void main(String[] args) {
        MetricTest test = new MetricTest();
        test.alltest();
        
    }
}

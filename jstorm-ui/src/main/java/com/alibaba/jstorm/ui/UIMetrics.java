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
package com.alibaba.jstorm.ui;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.metric.SimpleJStormMetric;
import com.alibaba.jstorm.utils.JStormUtils;

public class UIMetrics extends SimpleJStormMetric {
    
    private static final long serialVersionUID = -5726788610247404713L;
    private static final Logger LOG = LoggerFactory.getLogger(UIMetrics.class);
    
    protected static UIMetrics instance = null;
    
    
    public static UIMetrics mkInstance() {
        synchronized (UIMetrics.class) {
            if (instance == null) {
                instance = new UIMetrics();
                LOG.info("Make instance");
            }
            
            return instance;
        }
    }
    
    protected UIMetrics() {
        super();
    }
    
    static {
        instance = mkInstance();
        UIUtils.scheduExec.scheduleAtFixedRate(instance, 60, 60, TimeUnit.SECONDS);
        LOG.info("Start scheduler");
    }
    
    public static void main(String[] args) {
        UIMetrics test = mkInstance();
        test.run();
        JStormUtils.sleepMs(100 * 1000);
    }
}

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
package org.apache.storm.metrics2;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.storm.Config;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.metrics2.reporters.StormReporter;
import org.apache.storm.task.WorkerTopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class StormMetricRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(StormMetricRegistry.class);

    private static final MetricRegistry REGISTRY = new MetricRegistry();

    private static final List<StormReporter> REPORTERS = new ArrayList<>();

    private static String hostName = null;

    public static <T> SimpleGauge<T>  gauge(T initialValue, String name, String topologyId, Integer port){
        SimpleGauge<T> gauge = new SimpleGauge<>(initialValue);
        String metricName = String.format("storm.worker.%s.%s-%s", topologyId, port, name);
        if(REGISTRY.getGauges().containsKey(metricName)){
            return (SimpleGauge)REGISTRY.getGauges().get(metricName);
        } else {
            return REGISTRY.register(metricName, gauge);
        }
    }

    public static DisruptorMetrics disruptorMetrics(String name, String topologyId, Integer port){
        return new DisruptorMetrics(
                StormMetricRegistry.gauge(0L, name + "-capacity", topologyId, port),
                StormMetricRegistry.gauge(0L, name + "-population", topologyId, port),
                StormMetricRegistry.gauge(0L, name + "-write-position", topologyId, port),
                StormMetricRegistry.gauge(0L, name + "-read-position", topologyId, port),
                StormMetricRegistry.gauge(0.0, name + "-arrival-rate", topologyId, port),
                StormMetricRegistry.gauge(0.0, name + "-sojourn-time-ms", topologyId, port),
                StormMetricRegistry.gauge(0L, name + "-overflow", topologyId, port),
                StormMetricRegistry.gauge(0.0F, name + "-percent-full", topologyId, port)
        );
    }

    public static Meter meter(String name, WorkerTopologyContext context, String componentId){
        // storm.worker.{topology}.{host}.{port}
        String metricName = String.format("storm.worker.%s.%s.%s.%s-%s", context.getStormId(), hostName,
                componentId, context.getThisWorkerPort(), name);
        return REGISTRY.meter(metricName);
    }

    public static void start(Map<String, Object> stormConfig, DaemonType type){
        String localHost = (String)stormConfig.get(Config.STORM_LOCAL_HOSTNAME);
        if(localHost != null){
            hostName = localHost;
        } else {
            try {
                hostName = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (UnknownHostException e) {
                 LOG.warn("Unable to determine hostname while starting the metrics system. Hostname ill be reported" +
                         " as 'localhost'.");
            }
        }

        LOG.info("Starting metrics reporters...");
        List<Map<String, Object>> reporterList = (List<Map<String, Object>>)stormConfig.get(Config.STORM_METRICS_REPORTERS);
        for(Map<String, Object> reporterConfig : reporterList){
            // only start those requested
            List<String> daemons = (List<String>)reporterConfig.get("daemons");
            for(String daemon : daemons){
                if(DaemonType.valueOf(daemon.toUpperCase()) == type){
                    startReporter(stormConfig, reporterConfig);
                }
            }
        }
    }

    public static MetricRegistry registtry(){
        return REGISTRY;
    }

    private static void startReporter(Map<String, Object> stormConfig, Map<String, Object> reporterConfig){
        String clazz = (String)reporterConfig.get("class");
        StormReporter reporter = null;
        LOG.info("Attempting to instantiate reporter class: {}", clazz);
        try{
            reporter = instantiate(clazz);
        } catch(Exception e){
            LOG.warn("Unable to instantiate metrics reporter class: {}. Will skip this reporter.", clazz, e);
        }
        if(reporter != null){
            reporter.prepare(REGISTRY, stormConfig, reporterConfig);
            reporter.start();
            REPORTERS.add(reporter);
        }

    }

    private static StormReporter instantiate(String klass) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> c = Class.forName(klass);
        return  (StormReporter) c.newInstance();
    }

    public static void stop(){
        for(StormReporter sr : REPORTERS){
            sr.stop();
        }
    }
}

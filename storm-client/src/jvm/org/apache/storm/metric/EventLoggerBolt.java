/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metric;

import static org.apache.storm.daemon.StormCommon.TOPOLOGY_EVENT_LOGGER_ARGUMENTS;
import static org.apache.storm.daemon.StormCommon.TOPOLOGY_EVENT_LOGGER_CLASS;
import static org.apache.storm.metric.IEventLogger.EventInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventLoggerBolt implements IBolt {

    /*
     The below field declarations are also used in common.clj to define the event logger output fields
      */
    public static final String FIELD_TS = "ts";
    public static final String FIELD_VALUES = "values";
    public static final String FIELD_COMPONENT_ID = "component-id";
    public static final String FIELD_MESSAGE_ID = "message-id";
    private static final Logger LOG = LoggerFactory.getLogger(EventLoggerBolt.class);
    private List<IEventLogger> eventLoggers;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        LOG.info("EventLoggerBolt prepare called");

        eventLoggers = new ArrayList<>();
        List<Map<String, Object>> registerInfo = (List<Map<String, Object>>) topoConf.get(Config.TOPOLOGY_EVENT_LOGGER_REGISTER);
        if (registerInfo != null && !registerInfo.isEmpty()) {
            initializeEventLoggers(topoConf, context, registerInfo);
        } else {
            initializeDefaultEventLogger(topoConf, context);
        }
    }

    @Override
    public void execute(Tuple input) {
        LOG.debug("** EventLoggerBolt got tuple from sourceComponent {}, with values {}", input.getSourceComponent(), input.getValues());

        Object msgId = input.getValueByField(FIELD_MESSAGE_ID);
        EventInfo eventInfo = new EventInfo(input.getLongByField(FIELD_TS), input.getSourceComponent(),
                                            input.getSourceTask(), msgId, (List<Object>) input.getValueByField(FIELD_VALUES));

        for (IEventLogger eventLogger : eventLoggers) {
            eventLogger.log(eventInfo);
        }
    }

    @Override
    public void cleanup() {
        for (IEventLogger eventLogger : eventLoggers) {
            eventLogger.close();
        }
    }

    private void initializeEventLoggers(Map<String, Object> topoConf, TopologyContext context, List<Map<String, Object>> registerInfo) {
        for (Map<String, Object> info : registerInfo) {
            String className = (String) info.get(TOPOLOGY_EVENT_LOGGER_CLASS);
            Map<String, Object> arguments = (Map<String, Object>) info.get(TOPOLOGY_EVENT_LOGGER_ARGUMENTS);

            IEventLogger eventLogger;
            try {
                eventLogger = (IEventLogger) Class.forName(className).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Could not instantiate a class listed in config under section "
                                           + Config.TOPOLOGY_EVENT_LOGGER_REGISTER + " with fully qualified name " + className, e);
            }

            eventLogger.prepare(topoConf, arguments, context);
            eventLoggers.add(eventLogger);
        }
    }

    private void initializeDefaultEventLogger(Map<String, Object> topoConf, TopologyContext context) {
        FileBasedEventLogger eventLogger = new FileBasedEventLogger();
        eventLogger.prepare(topoConf, null, context);
        eventLoggers.add(eventLogger);
    }
}

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

package org.apache.storm.messaging;

import java.lang.reflect.Method;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.metrics2.StormMetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportFactory {
    public static final Logger LOG = LoggerFactory.getLogger(TransportFactory.class);

    public static IContext makeContext(Map<String, Object> topoConf, StormMetricRegistry metricRegistry) {

        //get factory class name
        String transportPluginClassName = (String) topoConf.get(Config.STORM_MESSAGING_TRANSPORT);
        LOG.info("Storm peer transport plugin:" + transportPluginClassName);

        IContext transport;
        try {
            //create a factory class
            Class klass = Class.forName(transportPluginClassName);
            //obtain a context object
            Object obj = klass.newInstance();
            if (obj instanceof IContext) {
                //case 1: plugin is a IContext class
                transport = (IContext) obj;
                //initialize with storm configuration
                transport.prepare(topoConf, metricRegistry);
            } else {
                //case 2: Non-IContext plugin must have a makeContext(topoConf) method that returns IContext object
                // StormMetricRegistry is ignored if IContext is created this way
                Method method = klass.getMethod("makeContext", Map.class);
                LOG.debug("object:" + obj + " method:" + method);
                transport = (IContext) method.invoke(obj, topoConf);
            }
        } catch (Exception e) {
            throw new RuntimeException("Fail to construct messaging plugin from plugin " + transportPluginClassName, e);
        }
        return transport;
    }
}

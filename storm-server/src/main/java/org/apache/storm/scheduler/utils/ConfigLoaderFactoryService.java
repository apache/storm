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

package org.apache.storm.scheduler.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.storm.DaemonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The user interface to create a concrete IConfigLoader instance for use.
 */
public class ConfigLoaderFactoryService {

    private static Logger LOG = LoggerFactory.getLogger(IConfigLoaderFactory.class);

    /**
     * SerivceLoader loads all the implementations of IConfigLoaderFactory for use.
     */
    private static ServiceLoader<IConfigLoaderFactory> serviceLoader = ServiceLoader.load(IConfigLoaderFactory.class);

    /**
     * The user interface to create an IConfigLoader instance.
     * It iterates all the implementations of IConfigLoaderFactory and finds the one which supports the
     * specific scheme of the URI and then uses it to create an IConfigLoader instance.
     * @param conf The storm configuration.
     * @return A concrete IConfigLoader implementation which supports the scheme of the URI.
     *         If multiple implementations are available, return the first one; otherwise, return null.
     */
    public static IConfigLoader createConfigLoader(Map<String, Object> conf) {
        String uriString = (String) conf.get(DaemonConfig.SCHEDULER_CONFIG_LOADER_URI);
        if (null != uriString) {
            try {
                URI uri = new URI(uriString);
                for (IConfigLoaderFactory factory : serviceLoader) {
                    IConfigLoader ret = factory.createIfSupported(uri, conf);
                    if (ret != null) {
                        return ret;
                    }
                }
            } catch (URISyntaxException e) {
                LOG.error("Failed to parse uri={}", uriString);
            }
        } else {
            LOG.debug("Config {} is not set.", DaemonConfig.SCHEDULER_CONFIG_LOADER_URI);
        }
        return null;
    }
}

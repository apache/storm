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
import org.apache.storm.DaemonConfig;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduler configuration loader which loads configs from a file.
 */
public class FileConfigLoader implements IConfigLoader {
    private static final Logger LOG = LoggerFactory.getLogger(FileConfigLoader.class);

    private Map<String, Object> conf;
    private String targetFilePath = null;

    public FileConfigLoader(Map<String, Object> conf) {
        this.conf = conf;
        String uriString = (String) conf.get(DaemonConfig.SCHEDULER_CONFIG_LOADER_URI);
        if (uriString == null) {
            LOG.error("No URI defined in {} configuration.", DaemonConfig.SCHEDULER_CONFIG_LOADER_URI);
        } else {
            try {
                targetFilePath = new URI(uriString).getPath();
            } catch (URISyntaxException e) {
                LOG.error("Failed to parse uri={}", uriString);
            }
        }
    }

    /**
     * Load the configs associated with the configKey from the targetFilePath.
     * @param configKey The key from which we want to get the scheduler config.
     * @return The scheduler configuration if exists; null otherwise.
     */
    @Override
    public Map<String, Object> load(String configKey) {
        if (targetFilePath != null) {
            try {
                Map<String, Object> raw = (Map<String, Object>) Utils.readYamlFile(targetFilePath);
                if (raw != null) {
                    return (Map<String, Object>) raw.get(configKey);
                }
            } catch (Exception e) {
                LOG.error("Failed to load from file {}", targetFilePath);
            }
        }
        return null;
    }
}
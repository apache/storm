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
package org.apache.storm.scheduler.utils;

import org.apache.storm.utils.Utils;
import java.util.Map;
import java.io.File;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchedulerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SchedulerUtils.class);

    public static IConfigLoader getConfigLoader(Map conf, String loaderClassConfig, String loaderConfConfig) {
        if (conf.get(loaderClassConfig) != null) {
            Map<String, String> loaderConf = (Map<String, String>)conf.get(loaderConfConfig);
            String clazz = (String)conf.get(loaderClassConfig);
            if (clazz != null) {
                IConfigLoader loader = (IConfigLoader)Utils.newInstance(clazz);
                if (loader != null) {
                    loader.prepare(loaderConf);
                    return loader;
                }
            }
        }
        return null;
    }

    public static Map loadYamlFromFile(File file) {
        Map ret = null;
        String pathString="Invalid";
        try {
            pathString = file.getCanonicalPath();
            Yaml yaml = new Yaml(new SafeConstructor());
            try (FileInputStream fis = new FileInputStream(file)) {
                ret = (Map) yaml.load(new InputStreamReader(fis));
            }
        } catch (Exception e) {
            LOG.error("Failed to load from file {}", pathString, e);
            return null;
        }

        return ret;
    }
}

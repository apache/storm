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


public interface IConfigLoader {

    /**
     * Pass in configurable parameters for the location of the data, (filename / artifactory url).
     * 
     * See implementing classes for configuration details.
     */
    void prepare(Map<String, Object> conf);

    Map<?,?> load();

    public static IConfigLoader getConfigLoader(Map conf, String loaderClassConfig, String loaderConfConfig) {
        if (conf.get(loaderClassConfig) != null) {
            Map<String, Object> loaderConf = (Map<String, Object>)conf.get(loaderConfConfig);
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

    /**
     * @param file File object referring to the local file containing the configuration
     *        in yaml format.
     *
     * @return null on failure or a map containing the loaded configuration
     */
    public static Map<?,?> loadYamlConfigFromFile(File file) {
        Map ret = null;
        String pathString="Invalid";
        try {
            pathString = file.getCanonicalPath();
            Yaml yaml = new Yaml(new SafeConstructor());
            try (FileInputStream fis = new FileInputStream(file)) {
                ret = (Map) yaml.load(new InputStreamReader(fis));
            }
        } catch (Exception e) {
            IConfigLoaderLogger.LOG.error("Failed to load from file {}", pathString, e);
            return null;
        }

        return ret;
    }
}

final class IConfigLoaderLogger {
    static final Logger LOG = LoggerFactory.getLogger(IConfigLoader.class);
}

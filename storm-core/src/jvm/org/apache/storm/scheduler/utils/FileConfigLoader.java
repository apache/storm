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

import java.io.File;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileConfigLoader implements IConfigLoader {
    /**
     * Configuration items for this config loader are passed in via confg settings in
     * each scheduler that has a configurable loader.
     *
     * For example, the resource aware scheduler has configuration items defined in Config.java
     * that allow a user to configure which implementation of IConfigLoader to use to load
     * specific scheduler configs as well as any parameters to pass into the prepare method of
     * tht configuration.
     *
     * resource.aware.scheduler.user.pools.loader can be set to org.apache.storm.scheduler.utils.ArtifactoryConfigLoader
     *
     * and then
     *
     * resource.aware.scheduler.user.pools.loader.params can be set to the following
     *
     *  {"file.config.loader.local.file.yaml": "/path/to/my/config.yaml"}
     *
     **/

    @SuppressWarnings("rawtypes")
    Map conf;
    protected static final String LOCAL_FILE_YAML="file.config.loader.local.file.yaml";
    private static final Logger LOG = LoggerFactory.getLogger(FileConfigLoader.class);

    @Override
    public void prepare(Map conf) {
        this.conf = conf;
    }

    @Override
    public Map load() {
        String localFileName = (String) conf.get(LOCAL_FILE_YAML);
        if (localFileName == null) {
            LOG.warn("No yaml file defined in configuration.");
            return null;
        }

        return IConfigLoader.loadYamlConfigFromFile(new File(localFileName));
    }
}

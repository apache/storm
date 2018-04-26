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

import com.google.auto.service.AutoService;
import java.net.URI;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for FileConfigLoader.
 */
@AutoService(IConfigLoaderFactory.class)
public class FileConfigLoaderFactory implements IConfigLoaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FileConfigLoaderFactory.class);

    /**
     * Create a FileConfigLoader if the scheme of the URI is "file"; else return null.
     * @param uri The URI which pointing to the config file location.
     * @param conf The storm configuration.
     * @return A FileConfigLoader if the scheme is "file"; otherwise, null.
     */
    @Override
    public IConfigLoader createIfSupported(URI uri, Map<String, Object> conf) {
        String scheme = uri.getScheme();
        if ("file".equalsIgnoreCase(scheme)) {
            return new FileConfigLoader(conf);
        } else {
            LOG.debug("scheme {} not supported in this factory.", scheme);
            return null;
        }
    }
}

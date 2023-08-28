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

package org.apache.storm.dependency;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.net.minidev.json.JSONValue;
import org.apache.storm.shade.net.minidev.json.parser.ParseException;

public class DependencyPropertiesParser {
    public List<File> parseJarsProperties(String prop) {
        if (prop.trim().isEmpty()) {
            // handle no input
            return Collections.emptyList();
        }

        List<String> dependencies = Arrays.asList(prop.split(","));
        return Lists.transform(dependencies, File::new);
    }

    public Map<String, File> parseArtifactsProperties(String prop) {
        try {
            Map<String, String> parsed = (Map<String, String>) JSONValue.parseWithException(prop);
            Map<String, File> packages = new LinkedHashMap<>(parsed.size());
            for (Map.Entry<String, String> artifactToFilePath : parsed.entrySet()) {
                packages.put(artifactToFilePath.getKey(), new File(artifactToFilePath.getValue()));
            }

            return packages;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}

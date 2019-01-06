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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.shade.org.json.simple.parser.ParseException;

public class DependencyPropertiesParser {
    public List<Path> parseJarsProperties(String prop) {
        if (prop.trim().isEmpty()) {
            // handle no input
            return Collections.emptyList();
        }

        List<String> dependencies = Arrays.asList(prop.split(","));
        return dependencies.stream()
            .map(Paths::get)
            .collect(Collectors.toList());
    }

    public Map<String, Path> parseArtifactsProperties(String prop) {
        try {
            Map<String, String> parsed = (Map<String, String>) JSONValue.parseWithException(prop);
            Map<String, Path> packages = new LinkedHashMap<>(parsed.size());
            for (Map.Entry<String, String> artifactToFilePath : parsed.entrySet()) {
                packages.put(artifactToFilePath.getKey(), Paths.get(artifactToFilePath.getValue()));
            }

            return packages;
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}

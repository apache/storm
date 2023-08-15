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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DependencyPropertiesParserTest {
    private final DependencyPropertiesParser sut = new DependencyPropertiesParser();

    @Test
    public void parseJarsProperties() {
        List<File> parsed = sut.parseJarsProperties("storm-core-1.0.0.jar,json-simple-1.1.jar");
        assertEquals(2, parsed.size());
        assertEquals("storm-core-1.0.0.jar", parsed.get(0).getName());
        assertEquals("json-simple-1.1.jar", parsed.get(1).getName());
    }

    @Test
    public void parseEmptyJarsProperties() {
        List<File> parsed = sut.parseJarsProperties("");
        assertEquals(0, parsed.size());
    }

    @Test
    public void parsePackagesProperties() {
        Map<String, String> testInputMap = new HashMap<>();
        testInputMap.put("org.apache.storm:storm-core:1.0.0", "storm-core-1.0.0.jar");
        testInputMap.put("com.googlecode.json-simple:json-simple:1.1", "json-simple-1.1.jar");

        String testJson = JSONValue.toJSONString(testInputMap);

        Map<String, File> parsed = sut.parseArtifactsProperties(testJson);
        assertEquals(2, parsed.size());
        assertEquals("storm-core-1.0.0.jar", parsed.get("org.apache.storm:storm-core:1.0.0").getName());
        assertEquals("json-simple-1.1.jar", parsed.get("com.googlecode.json-simple:json-simple:1.1").getName());
    }

    @Test
    public void parseEmptyPackagesProperties() {
        Map<String, File> parsed = sut.parseArtifactsProperties("{}");
        assertEquals(0, parsed.size());
    }

    @Test
    public void parsePackagesPropertiesWithBrokenJSON() {
        assertThrows(RuntimeException.class,
            () -> sut.parseArtifactsProperties("{\"group:artifact:version\": \"a.jar\""));
    }

}

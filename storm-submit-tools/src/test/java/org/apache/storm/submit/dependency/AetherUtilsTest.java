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
package org.apache.storm.submit.dependency;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.Exclusion;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.util.artifact.JavaScopes;

import java.util.List;

import static org.junit.Assert.*;

public class AetherUtilsTest {
    @Test
    public void parseDependency() throws Exception {
        String testDependency = "testgroup:testartifact:1.0.0^testgroup:testexcartifact^testgroup:*";

        Dependency dependency = AetherUtils.parseDependency(testDependency);

        assertEquals("testgroup", dependency.getArtifact().getGroupId());
        assertEquals("testartifact", dependency.getArtifact().getArtifactId());
        assertEquals("1.0.0", dependency.getArtifact().getVersion());
        assertEquals(JavaScopes.COMPILE, dependency.getScope());

        assertEquals(2, dependency.getExclusions().size());

        List<Exclusion> exclusions = Lists.newArrayList(dependency.getExclusions());

        Exclusion exclusion = exclusions.get(0);
        assertEquals("testgroup", exclusion.getGroupId());
        assertEquals("testexcartifact", exclusion.getArtifactId());
        assertEquals(JavaScopes.COMPILE, dependency.getScope());

        exclusion = exclusions.get(1);
        assertEquals("testgroup", exclusion.getGroupId());
        assertEquals("*", exclusion.getArtifactId());
        assertEquals(JavaScopes.COMPILE, dependency.getScope());
    }

    @Test
    public void createExclusion() throws Exception {
        String testExclusion = "group";
        Exclusion exclusion = AetherUtils.createExclusion(testExclusion);

        assertEquals("group", exclusion.getGroupId());
        assertEquals("*", exclusion.getArtifactId());
        assertEquals("*", exclusion.getClassifier());
        assertEquals("*", exclusion.getExtension());

        testExclusion = "group:artifact";
        exclusion = AetherUtils.createExclusion(testExclusion);

        assertEquals("group", exclusion.getGroupId());
        assertEquals("artifact", exclusion.getArtifactId());
        assertEquals("*", exclusion.getClassifier());
        assertEquals("*", exclusion.getExtension());

        testExclusion = "group:artifact:site";
        exclusion = AetherUtils.createExclusion(testExclusion);

        assertEquals("group", exclusion.getGroupId());
        assertEquals("artifact", exclusion.getArtifactId());
        assertEquals("site", exclusion.getClassifier());
        assertEquals("*", exclusion.getExtension());

        testExclusion = "group:artifact:site:jar";
        exclusion = AetherUtils.createExclusion(testExclusion);

        assertEquals("group", exclusion.getGroupId());
        assertEquals("artifact", exclusion.getArtifactId());
        assertEquals("site", exclusion.getClassifier());
        assertEquals("jar", exclusion.getExtension());
    }

    @Test
    public void artifactToString() throws Exception {
        Artifact testArtifact = new DefaultArtifact("org.apache.storm:storm-core:1.0.0");

        String ret = AetherUtils.artifactToString(testArtifact);
        assertEquals("org.apache.storm:storm-core:jar:1.0.0", ret);
    }

}

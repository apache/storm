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
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.util.artifact.JavaScopes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class DependencyResolverTest {
    private static Path tempDirForTest;

    private DependencyResolver sut;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        tempDirForTest = Files.createTempDirectory("dr-test");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        FileUtils.deleteQuietly(tempDirForTest.toFile());
    }

    @Before
    public void setUp() {
        sut = new DependencyResolver(tempDirForTest.toAbsolutePath().toString());
    }

    @Test
    public void resolveValid() throws Exception {
        // please pick small artifact which has small transitive dependency
        // and let's mark as Ignore if we want to run test even without internet or maven central is often not stable
        Dependency dependency = new Dependency(new DefaultArtifact("org.apache.storm:flux-core:1.0.0"), JavaScopes.COMPILE);
        List<ArtifactResult> results = sut.resolve(Lists.newArrayList(dependency));

        assertTrue(results.size() > 0);
        // it should be org.apache.storm:flux-core:jar:1.0.0 and commons-cli:commons-cli:jar:1.2
        assertContains(results, "org.apache.storm", "flux-core", "1.0.0");
        assertContains(results, "commons-cli", "commons-cli", "1.2");
    }

    private void assertContains(List<ArtifactResult> results, String groupId, String artifactId, String version) {
        for (ArtifactResult result : results) {
            if (result.getArtifact().getGroupId().equals(groupId) &&
                    result.getArtifact().getArtifactId().equals(artifactId) &&
                    result.getArtifact().getVersion().equals(version) &&
                    result.isResolved()) {
                return;
            }
        }

        throw new AssertionError("Result doesn't contain expected artifact > " + groupId + ":" + artifactId + ":" + version);
    }

}

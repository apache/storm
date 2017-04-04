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

import org.sonatype.aether.artifact.Artifact;
import org.sonatype.aether.graph.Dependency;
import org.sonatype.aether.graph.Exclusion;
import org.sonatype.aether.repository.RemoteRepository;
import org.sonatype.aether.util.artifact.DefaultArtifact;
import org.sonatype.aether.util.artifact.JavaScopes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class AetherUtils {
    private AetherUtils() {
    }

    public static Dependency parseDependency(String dependency) {
        List<String> dependencyAndExclusions = Arrays.asList(dependency.split("\\^"));
        Collection<Exclusion> exclusions = new ArrayList<>();
        for (int idx = 1 ; idx < dependencyAndExclusions.size() ; idx++) {
            exclusions.add(AetherUtils.createExclusion(dependencyAndExclusions.get(idx)));
        }

        Artifact artifact = new DefaultArtifact(dependencyAndExclusions.get(0));
        return new Dependency(artifact, JavaScopes.COMPILE, false, exclusions);
    }

    public static Exclusion createExclusion(String exclusionString) {
        String[] parts = exclusionString.split(":");

        // length of parts should be greater than 0
        String groupId = parts[0];

        String artifactId = "*";
        String classifier = "*";
        String extension = "*";

        int len = parts.length;
        if (len > 1) {
            artifactId = parts[1];
        }
        if (len > 2) {
            classifier = parts[2];
        }
        if (len > 3) {
            extension = parts[3];
        }

        return new Exclusion(groupId, artifactId, classifier, extension);
    }

    public static String artifactToString(Artifact artifact) {
        StringBuilder buffer = new StringBuilder(128);
        buffer.append(artifact.getGroupId());
        buffer.append(':').append(artifact.getArtifactId());
        buffer.append(':').append(artifact.getExtension());
        if (artifact.getClassifier().length() > 0) {
            buffer.append(':').append(artifact.getClassifier());
        }
        buffer.append(':').append(artifact.getVersion());
        return buffer.toString();
    }

    public static RemoteRepository parseRemoteRepository(String repository) {
        String[] parts = repository.split("\\^");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Bad remote repository form: " + repository);
        }

        return new RemoteRepository(parts[0], "default", parts[1]);
    }
}
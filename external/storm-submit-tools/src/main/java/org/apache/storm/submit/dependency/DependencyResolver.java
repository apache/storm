/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.submit.dependency;

import org.sonatype.aether.RepositorySystem;
import org.sonatype.aether.RepositorySystemSession;
import org.sonatype.aether.collection.CollectRequest;
import org.sonatype.aether.graph.Dependency;
import org.sonatype.aether.graph.DependencyFilter;
import org.sonatype.aether.repository.RemoteRepository;
import org.sonatype.aether.resolution.ArtifactResolutionException;
import org.sonatype.aether.resolution.ArtifactResult;
import org.sonatype.aether.resolution.DependencyRequest;
import org.sonatype.aether.resolution.DependencyResolutionException;
import org.sonatype.aether.util.artifact.JavaScopes;
import org.sonatype.aether.util.filter.DependencyFilterUtils;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.List;

public class DependencyResolver {
    private RepositorySystem system = Booter.newRepositorySystem();
    private RepositorySystemSession session;
    private RemoteRepository mavenCentral = Booter.newCentralRepository();
    private RemoteRepository mavenLocal = Booter.newLocalRepository();

    public DependencyResolver(String localRepoPath) {
        session = Booter.newRepositorySystemSession(system, localRepoPath);
    }

    public List<ArtifactResult> resolve(List<Dependency> dependencies) throws MalformedURLException,
            DependencyResolutionException, ArtifactResolutionException {

        DependencyFilter classpathFilter = DependencyFilterUtils
                .classpathFilter(JavaScopes.COMPILE, JavaScopes.RUNTIME);

        if (dependencies.size() == 0) {
            return Collections.EMPTY_LIST;
        }

        CollectRequest collectRequest = new CollectRequest();
        collectRequest.setRoot(dependencies.get(0));
        for (int idx = 1; idx < dependencies.size(); idx++) {
            collectRequest.addDependency(dependencies.get(idx));
        }

        collectRequest.addRepository(mavenCentral);
        collectRequest.addRepository(mavenLocal);

        DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, classpathFilter);
        return system.resolveDependencies(session, dependencyRequest).getArtifactResults();
    }

}
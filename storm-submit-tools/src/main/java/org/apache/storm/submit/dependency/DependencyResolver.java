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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.graph.DependencyFilter;
import org.eclipse.aether.repository.Proxy;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactResolutionException;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.util.artifact.JavaScopes;
import org.eclipse.aether.util.filter.DependencyFilterUtils;

/**
 * Resolver class of dependencies.
 */
public class DependencyResolver {
    private final RepositorySystem system = Booter.newRepositorySystem();
    private final RepositorySystemSession session;

    private List<RemoteRepository> remoteRepositories;
    private Proxy proxy;

    /**
     * Constuctor.
     *
     * @param localRepoPath the directory of local repository
     */
    public DependencyResolver(String localRepoPath) {
        this(localRepoPath, Collections.emptyList());
    }

    /**
     * Constuctor.
     *
     * @param localRepoPath the directory of local repository
     * @param repositories list of remote repositories
     */
    public DependencyResolver(String localRepoPath, List<RemoteRepository> repositories) {
        localRepoPath = handleRelativePath(localRepoPath);

        session = Booter.newRepositorySystemSession(system, localRepoPath);

        remoteRepositories = new ArrayList<>();
        remoteRepositories.add(Booter.newCentralRepository());
        remoteRepositories.addAll(repositories);
    }

    /**
     * Setter of proxy if needed.
     *
     * @param proxyParam proxy object
     */
    public void setProxy(Proxy proxyParam) {
        this.proxy = proxyParam;
        applyProxy();
    }

    private String handleRelativePath(String localRepoPath) {
        File repoDir = new File(localRepoPath);
        if (!repoDir.isAbsolute()) {
            // find homedir
            String home = System.getProperty("storm.home");
            if (home == null) {
                home = ".";
            }

            localRepoPath = home + "/" + localRepoPath;
        }
        return localRepoPath;
    }

    /**
     * Resolve dependencies and return downloaded information of artifacts.
     *
     * @param dependencies the list of dependency
     * @return downloaded information of artifacts
     * @throws DependencyResolutionException If the dependency tree could not be built or any dependency
     *     artifact could not be resolved.
     * @throws ArtifactResolutionException If the artifact could not be resolved.
     */
    public List<ArtifactResult> resolve(List<Dependency> dependencies) throws
            DependencyResolutionException, ArtifactResolutionException {

        if (dependencies.size() == 0) {
            return Collections.EMPTY_LIST;
        }

        CollectRequest collectRequest = new CollectRequest();
        collectRequest.setRoot(dependencies.get(0));
        for (int idx = 1; idx < dependencies.size(); idx++) {
            collectRequest.addDependency(dependencies.get(idx));
        }

        for (RemoteRepository repository : remoteRepositories) {
            collectRequest.addRepository(repository);
        }

        DependencyFilter classpathFilter = DependencyFilterUtils
                .classpathFilter(JavaScopes.COMPILE, JavaScopes.RUNTIME);

        DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, classpathFilter);
        return system.resolveDependencies(session, dependencyRequest).getArtifactResults();
    }

    private void applyProxy() {
        List<RemoteRepository> appliedRepositories = new ArrayList<>(remoteRepositories.size());
        for (RemoteRepository repository : remoteRepositories) {
            appliedRepositories.add(new RemoteRepository.Builder(repository).setProxy(proxy).build());
        }

        this.remoteRepositories = appliedRepositories;
    }

}

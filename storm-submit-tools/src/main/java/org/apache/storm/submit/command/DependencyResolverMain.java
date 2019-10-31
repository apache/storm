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

package org.apache.storm.submit.command;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.submit.dependency.AetherUtils;
import org.apache.storm.submit.dependency.DependencyResolver;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.repository.Proxy;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.util.repository.AuthenticationBuilder;
import org.json.simple.JSONValue;

/**
 * Main class of dependency resolver.
 */
public class DependencyResolverMain {
    private static final String OPTION_ARTIFACTS_LONG = "artifacts";
    private static final String OPTION_ARTIFACT_REPOSITORIES_LONG = "artifactRepositories";
    private static final String OPTION_MAVEN_LOCAL_REPOSITORY_DIRECTORY_LONG = "mavenLocalRepositoryDirectory";
    private static final String OPTION_PROXY_URL_LONG = "proxyUrl";
    private static final String OPTION_PROXY_USERNAME_LONG = "proxyUsername";
    private static final String OPTION_PROXY_PASSWORD_LONG = "proxyPassword";
    public static final String DEFAULT_FAILBACK_MAVEN_LOCAL_REPOSITORY_DIRECTORY = "local-repo";

    /**
     * Main entry of dependency resolver.
     *
     * @param args console arguments
     * @throws ParseException If there's parsing error on option parse.
     * @throws MalformedURLException If proxy URL is malformed.
     */
    public static void main(String[] args) throws ParseException, MalformedURLException {
        Options options = buildOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        if (!commandLine.hasOption(OPTION_ARTIFACTS_LONG)) {
            throw new IllegalArgumentException("artifacts must be presented.");
        }

        String artifactsArg = commandLine.getOptionValue(OPTION_ARTIFACTS_LONG);

        // DO NOT CHANGE THIS TO SYSOUT
        System.err.println("DependencyResolver input - artifacts: " + artifactsArg);
        List<Dependency> dependencies = parseArtifactArgs(artifactsArg);

        List<RemoteRepository> repositories;
        if (commandLine.hasOption(OPTION_ARTIFACT_REPOSITORIES_LONG)) {
            String remoteRepositoryArg = commandLine.getOptionValue(OPTION_ARTIFACT_REPOSITORIES_LONG);

            // DO NOT CHANGE THIS TO SYSOUT
            System.err.println("DependencyResolver input - repositories: " + remoteRepositoryArg);

            repositories = parseRemoteRepositoryArgs(remoteRepositoryArg);
        } else {
            repositories = Collections.emptyList();
        }

        try {
            String localMavenRepoPath = getOrDefaultLocalMavenRepositoryPath(
                    commandLine.getOptionValue(OPTION_MAVEN_LOCAL_REPOSITORY_DIRECTORY_LONG),
                    DEFAULT_FAILBACK_MAVEN_LOCAL_REPOSITORY_DIRECTORY);

            // create root directory if not exist
            Files.createDirectories(new File(localMavenRepoPath).toPath());

            DependencyResolver resolver = new DependencyResolver(localMavenRepoPath, repositories);

            if (commandLine.hasOption(OPTION_PROXY_URL_LONG)) {
                String proxyUrl = commandLine.getOptionValue(OPTION_PROXY_URL_LONG);
                String proxyUsername = commandLine.getOptionValue(OPTION_PROXY_USERNAME_LONG);
                String proxyPassword = commandLine.getOptionValue(OPTION_PROXY_PASSWORD_LONG);

                resolver.setProxy(parseProxyArg(proxyUrl, proxyUsername, proxyPassword));
            }

            List<ArtifactResult> artifactResults = resolver.resolve(dependencies);

            Iterable<ArtifactResult> missingArtifacts = filterMissingArtifacts(artifactResults);
            if (missingArtifacts.iterator().hasNext()) {
                printMissingArtifactsToSysErr(missingArtifacts);
                throw new RuntimeException("Some artifacts are not resolved");
            }

            System.out.println(JSONValue.toJSONString(transformArtifactResultToArtifactToPaths(artifactResults)));
            System.out.flush();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static Iterable<ArtifactResult> filterMissingArtifacts(List<ArtifactResult> artifactResults) {
        return Iterables.filter(artifactResults, new Predicate<ArtifactResult>() {
            @Override
            public boolean apply(ArtifactResult artifactResult) {
                return artifactResult.isMissing();
            }
        });
    }

    private static void printMissingArtifactsToSysErr(Iterable<ArtifactResult> missingArtifacts) {
        for (ArtifactResult artifactResult : missingArtifacts) {
            System.err.println("ArtifactResult : " + artifactResult + " / Errors : " + artifactResult.getExceptions());
        }
    }

    private static List<Dependency> parseArtifactArgs(String artifactArgs) {
        List<String> artifacts = Arrays.asList(artifactArgs.split(","));
        List<Dependency> dependencies = new ArrayList<>(artifacts.size());
        for (String artifactOpt : artifacts) {
            if (artifactOpt.trim().isEmpty()) {
                continue;
            }

            dependencies.add(AetherUtils.parseDependency(artifactOpt));
        }

        return dependencies;
    }

    private static List<RemoteRepository> parseRemoteRepositoryArgs(String remoteRepositoryArg) {
        List<String> repositories = Arrays.asList(remoteRepositoryArg.split(","));
        List<RemoteRepository> remoteRepositories = new ArrayList<>(repositories.size());
        for (String repositoryOpt : repositories) {
            if (repositoryOpt.trim().isEmpty()) {
                continue;
            }

            remoteRepositories.add(AetherUtils.parseRemoteRepository(repositoryOpt));
        }

        return remoteRepositories;
    }

    private static Proxy parseProxyArg(String proxyUrl, String proxyUsername, String proxyPassword) throws MalformedURLException {
        URL url = new URL(proxyUrl);
        if (StringUtils.isNotEmpty(proxyUsername) && StringUtils.isNotEmpty(proxyPassword)) {
            AuthenticationBuilder authBuilder = new AuthenticationBuilder();
            authBuilder.addUsername(proxyUsername).addPassword(proxyPassword);
            return new Proxy(url.getProtocol(), url.getHost(), url.getPort(), authBuilder.build());
        } else {
            return new Proxy(url.getProtocol(), url.getHost(), url.getPort());
        }
    }

    private static Map<String, String> transformArtifactResultToArtifactToPaths(List<ArtifactResult> artifactResults) {
        Map<String, String> artifactToPath = new LinkedHashMap<>();
        for (ArtifactResult artifactResult : artifactResults) {
            Artifact artifact = artifactResult.getArtifact();
            artifactToPath.put(AetherUtils.artifactToString(artifact), artifact.getFile().getAbsolutePath());
        }
        return artifactToPath;
    }

    private static String getOrDefaultLocalMavenRepositoryPath(String customLocalMavenPath, String defaultPath) {
        if (customLocalMavenPath != null) {
            Path customPath = new File(customLocalMavenPath).toPath();
            Preconditions.checkArgument(!Files.exists(customPath) || Files.isDirectory(customPath),
                    "Custom local maven repository path exist and is not a directory!");
            return customLocalMavenPath;
        }

        String localMavenRepoPathStr = getLocalMavenRepositoryPath();
        if (StringUtils.isNotEmpty(localMavenRepoPathStr)) {
            Path localMavenRepoPath = new File(localMavenRepoPathStr).toPath();
            if (Files.exists(localMavenRepoPath) && Files.isDirectory(localMavenRepoPath)) {
                return localMavenRepoPathStr;
            }
        }

        return defaultPath;
    }

    private static String getLocalMavenRepositoryPath() {
        String userHome = System.getProperty("user.home");
        if (StringUtils.isNotEmpty(userHome)) {
            return userHome + File.separator + ".m2" + File.separator + "repository";
        }

        return null;
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(null,
                OPTION_ARTIFACTS_LONG,
                true,
                "REQUIRED string representation of artifacts");
        options.addOption(null,
                OPTION_ARTIFACT_REPOSITORIES_LONG,
                true,
                "OPTIONAL string representation of artifact repositories");
        options.addOption(null,
                OPTION_MAVEN_LOCAL_REPOSITORY_DIRECTORY_LONG,
                true,
                "OPTIONAL string representation of local maven repository directory path");
        options.addOption(null,
                OPTION_PROXY_URL_LONG,
                true,
                "OPTIONAL URL representation of proxy server");
        options.addOption(null,
                OPTION_PROXY_USERNAME_LONG,
                true,
                "OPTIONAL Username of proxy server (basic auth)");
        options.addOption(null,
                OPTION_PROXY_PASSWORD_LONG,
                true,
                "OPTIONAL Password of proxy server (basic auth)");
        return options;
    }
}

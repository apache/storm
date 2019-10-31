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

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;

/**
 * Manage mvn repository.
 */
public class Booter {
    public static RepositorySystem newRepositorySystem() {
        return RepositorySystemFactory.newRepositorySystem();
    }

    public static RepositorySystemSession newRepositorySystemSession(
        RepositorySystem system, String localRepoPath) {
        DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();

        LocalRepository localRepo =
                new LocalRepository(new File(localRepoPath).getAbsolutePath());
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

        return session;
    }

    public static RemoteRepository newCentralRepository() {
        return new RemoteRepository.Builder("central", "default", "https://repo1.maven.org/maven2/").build();
    }

    public static RemoteRepository newLocalRepository() {
        return new RemoteRepository.Builder("local",
                "default", "file://" + System.getProperty("user.home") + "/.m2/repository").build();
    }
}

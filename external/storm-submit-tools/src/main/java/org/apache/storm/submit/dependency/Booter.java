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

import org.apache.maven.repository.internal.MavenRepositorySystemSession;
import org.sonatype.aether.RepositorySystem;
import org.sonatype.aether.RepositorySystemSession;
import org.sonatype.aether.repository.LocalRepository;
import org.sonatype.aether.repository.RemoteRepository;

import java.io.File;

/**
 * Manage mvn repository.
 */
public class Booter {
    public static RepositorySystem newRepositorySystem() {
        return RepositorySystemFactory.newRepositorySystem();
    }

    public static RepositorySystemSession newRepositorySystemSession(
            RepositorySystem system, String localRepoPath) {
        MavenRepositorySystemSession session = new MavenRepositorySystemSession();

        LocalRepository localRepo =
                new LocalRepository(new File(localRepoPath).getAbsolutePath());
        session.setLocalRepositoryManager(system.newLocalRepositoryManager(localRepo));

        return session;
    }

    public static RemoteRepository newCentralRepository() {
        return new RemoteRepository("central", "default", "http://repo1.maven.org/maven2/");
    }
}
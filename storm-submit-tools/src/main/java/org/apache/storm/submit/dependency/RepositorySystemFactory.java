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

import org.apache.maven.repository.internal.DefaultServiceLocator;
import org.apache.maven.wagon.Wagon;
import org.apache.maven.wagon.providers.http.HttpWagon;
import org.apache.maven.wagon.providers.http.LightweightHttpWagon;
import org.sonatype.aether.RepositorySystem;
import org.sonatype.aether.connector.file.FileRepositoryConnectorFactory;
import org.sonatype.aether.connector.wagon.WagonProvider;
import org.sonatype.aether.connector.wagon.WagonRepositoryConnectorFactory;
import org.sonatype.aether.spi.connector.RepositoryConnectorFactory;

/**
 * Get maven repository instance.
 */
public class RepositorySystemFactory {
    public static RepositorySystem newRepositorySystem() {
        DefaultServiceLocator locator = new DefaultServiceLocator();
        locator.addService(RepositoryConnectorFactory.class, FileRepositoryConnectorFactory.class);
        locator.addService(RepositoryConnectorFactory.class, WagonRepositoryConnectorFactory.class);
        locator.setServices(WagonProvider.class, new ManualWagonProvider());

        return locator.getService(RepositorySystem.class);
    }

    /**
     * ManualWagonProvider
     */
    public static class ManualWagonProvider implements WagonProvider {

        @Override
        public Wagon lookup(String roleHint) throws Exception {
            if ("http".equals(roleHint)) {
                return new LightweightHttpWagon();
            }

            if ("https".equals(roleHint)) {
                return new HttpWagon();
            }

            return null;
        }

        @Override
        public void release(Wagon arg0) {

        }
    }
}
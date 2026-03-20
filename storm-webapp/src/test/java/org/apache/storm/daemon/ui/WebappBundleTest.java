/*
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

package org.apache.storm.daemon.ui;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the webpack-built frontend bundles are present on the
 * classpath after the Maven build. This catches build configuration
 * regressions where the frontend-maven-plugin or webpack step fails
 * silently.
 */
public class WebappBundleTest {

    @Test
    public void testMainBundleJsExists() {
        assertBundleExists("/WEB-INF/dist/main.bundle.js");
    }

    @Test
    public void testMainBundleCssExists() {
        assertBundleExists("/WEB-INF/dist/main.bundle.css");
    }

    @Test
    public void testVisualizeBundleJsExists() {
        assertBundleExists("/WEB-INF/dist/visualize.bundle.js");
    }

    @Test
    public void testVisualizeBundleCssExists() {
        assertBundleExists("/WEB-INF/dist/visualize.bundle.css");
    }

    @Test
    public void testFluxBundleJsExists() {
        assertBundleExists("/WEB-INF/dist/flux.bundle.js");
    }

    @Test
    public void testBundlesAreNonEmpty() throws Exception {
        String[] bundles = {
            "/WEB-INF/dist/main.bundle.js",
            "/WEB-INF/dist/main.bundle.css",
            "/WEB-INF/dist/visualize.bundle.js",
            "/WEB-INF/dist/flux.bundle.js"
        };
        for (String bundle : bundles) {
            try (InputStream is = getClass().getResourceAsStream(bundle)) {
                assertNotNull(is, "Bundle missing: " + bundle);
                assertTrue(is.available() > 0, "Bundle is empty: " + bundle);
            }
        }
    }

    private void assertBundleExists(String classpathResource) {
        assertNotNull(
            getClass().getResource(classpathResource),
            "Expected webpack bundle on classpath: " + classpathResource
        );
    }
}

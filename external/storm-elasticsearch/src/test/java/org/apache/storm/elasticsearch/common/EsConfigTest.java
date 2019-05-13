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
package org.apache.storm.elasticsearch.common;

import static org.junit.Assert.assertEquals;

import org.apache.http.HttpHost;

import com.google.common.testing.NullPointerTester;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EsConfigTest {

    @Test
    public void urlsCannotBeEmpty() throws Exception {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new EsConfig(new String[] {}));
    }

    @Test
    public void constructorThrowsOnNull() throws Exception {
        new NullPointerTester().testAllPublicConstructors(EsConfig.class);
    }

    @Test
    public void usesElasticsearchDefaults() {
        EsConfig esConfig = new EsConfig();
        HttpHost[] httpHosts = esConfig.getHttpHosts();
        assertEquals(1, httpHosts.length);
        assertEquals("http", httpHosts[0].getSchemeName());
        assertEquals(9200, httpHosts[0].getPort());
        assertEquals("localhost", httpHosts[0].getHostName());
    }

    @Test
    public void setsSchemePortAndHost() {
        EsConfig esConfig = new EsConfig("https://somehost:1234");
        HttpHost[] httpHosts = esConfig.getHttpHosts();
        assertEquals(1, httpHosts.length);
        assertEquals("https", httpHosts[0].getSchemeName());
        assertEquals(1234, httpHosts[0].getPort());
        assertEquals("somehost", httpHosts[0].getHostName());
    }

    @Test
    public void usesMultipleHostnames() {
        EsConfig esConfig = new EsConfig("http://host1:9200", "http://host2:9200");
        HttpHost[] httpHosts = esConfig.getHttpHosts();
        assertEquals(2, httpHosts.length);
        assertEquals("host1", httpHosts[0].getHostName());
        assertEquals("host2", httpHosts[1].getHostName());
    }
}

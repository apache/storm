/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm;

import com.google.common.base.Strings;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SubmitterTest {

    @Test
    public void testMd5DigestSecretGeneration01() {
        // No payload or scheme are generated when already present
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "foobar:12345");
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "anything");
        Map<String, Object> result = StormSubmitter.prepareZookeeperAuthentication(conf);
        Object actualPayload = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
        Object actualScheme = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
        assertThat(actualPayload, is("foobar:12345"));
        assertThat(actualScheme, is("digest"));
    }

    @Test
    public void testMd5DigestSecretGeneration02() {
        // Scheme is set to digest if not already.
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "foobar:12345");
        Map<String, Object> result = StormSubmitter.prepareZookeeperAuthentication(conf);
        Object actualPayload = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
        Object actualScheme = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
        assertThat(actualPayload, is("foobar:12345"));
        assertThat(actualScheme, is("digest"));
    }

    @Test
    public void testMd5DigestSecretGeneration03() {
        // A payload is generated when no payload is present.
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "anything");
        Map<String, Object> result = StormSubmitter.prepareZookeeperAuthentication(conf);
        Object actualPayload = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
        Object actualScheme = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
        assertThat(Strings.isNullOrEmpty((String)actualPayload), is(false));
        assertThat(actualScheme, is("digest"));
    }

    @Test
    public void testMd5DigestSecretGeneration04() {
        // A payload is generated when payload is not correctly formatted.
        String bogusPayload = "not-a-valid-payload";
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, bogusPayload);
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "anything");
        Map<String, Object> result = StormSubmitter.prepareZookeeperAuthentication(conf);
        Object actualPayload = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
        Object actualScheme = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
        assertThat(StormSubmitter.validateZKDigestPayload(bogusPayload), is(false));
        assertThat(Strings.isNullOrEmpty((String)actualPayload), is(false));
        assertThat(actualScheme, is("digest"));
    }

    @Test
    public void testMd5DigestSecretGeneration05() {
        // A payload is generated when payload is null.
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, null);
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "anything");
        Map<String, Object> result = StormSubmitter.prepareZookeeperAuthentication(conf);
        Object actualPayload = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
        Object actualScheme = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
        assertThat(Strings.isNullOrEmpty((String)actualPayload), is(false));
        assertThat(actualScheme, is("digest"));
    }

    @Test
    public void testMd5DigestSecretGeneration06() {
        // A payload is generated when payload is blank.
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD, "");
        conf.put(Config.STORM_ZOOKEEPER_AUTH_SCHEME, "anything");
        Map<String, Object> result = StormSubmitter.prepareZookeeperAuthentication(conf);
        Object actualPayload = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_PAYLOAD);
        Object actualScheme = result.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME);
        assertThat(Strings.isNullOrEmpty((String)actualPayload), is(false));
        assertThat(actualScheme, is("digest"));
    }
}

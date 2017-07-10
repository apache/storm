/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.utils;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.DaemonConfig;
import org.apache.storm.utils.Time;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class ArtifactoryConfigLoaderTest {

    private static final Logger LOG = LoggerFactory.getLogger(ArtifactoryConfigLoaderTest.class);
    private Path tmpDirPath;
    private static final String ARTIFACTORY_HTTP_SCHEME_PREFIX = "artifactory+http://";

    private class ArtifactoryConfigLoaderMock extends ArtifactoryConfigLoader {
        String getData;
        HashMap<String, String> getDataMap = new HashMap<String, String>();

        public ArtifactoryConfigLoaderMock(Map conf) {
            super(conf);
        }

        public void setData(String api, String artifact, String data) {
            if (api == null) {
                getData = data;
            }  else {
                getDataMap.put(artifact, data);
            }
        }

        @Override
        protected String doGet(String api, String artifact, String host, Integer port) {
            if (api == null) {
                return getData;
            }
            return getDataMap.get(artifact);
        }
    };

    @Before
    public void createTempDir() throws Exception {
        tmpDirPath = Files.createTempDirectory("TestArtifactoryConfigLoader");
        File f = tmpDirPath.toFile();
        f.mkdir();
        File dir = new File(f, "nimbus");
        dir.mkdir();
    }

    @After
    public void removeTempDir() throws Exception {
        FileUtils.deleteDirectory(tmpDirPath.toFile());
    }

    @Test
    public void testInvalidConfig() {
        Config conf = new Config();
        ArtifactoryConfigLoaderMock loaderMock = new ArtifactoryConfigLoaderMock(conf);
        Map ret = loaderMock.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
        Assert.assertNull("Unexpectedly returned not null", ret);
    }

    @Test
    public void testPointingAtDirectory() {
        // This is a test where we are configured to point right at an artifact dir
        Config conf = new Config();
        conf.put(DaemonConfig.SCHEDULER_CONFIG_LOADER_URI, ARTIFACTORY_HTTP_SCHEME_PREFIX + "bogushost.yahoo.com:9999/location/of/this/dir");
        conf.put(Config.STORM_LOCAL_DIR, tmpDirPath.toString());

        ArtifactoryConfigLoaderMock loaderMock = new ArtifactoryConfigLoaderMock(conf);

        loaderMock.setData("Anything", "/location/of/this/dir",
                "{\"children\" : [ { \"uri\" : \"/20160621204337.yaml\", \"folder\" : false }]}" );
        loaderMock.setData(null, null, "{ \"" + DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS + "\": {one: 1, two: 2, three: 3, four : 4}}");

        Map ret = loaderMock.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
        Assert.assertNotNull("Unexpectedly returned null", ret);
        Assert.assertEquals(1, ret.get("one"));
        Assert.assertEquals(2, ret.get("two"));
        Assert.assertEquals(3, ret.get("three"));
        Assert.assertEquals(4, ret.get("four"));

        // Now let's load w/o setting up gets and we should still get valid map back
        ArtifactoryConfigLoaderMock tc2 = new ArtifactoryConfigLoaderMock(conf);

        Map ret2 = tc2.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
        Assert.assertNotNull("Unexpectedly returned null", ret2);
        Assert.assertEquals(1, ret2.get("one"));
        Assert.assertEquals(2, ret2.get("two"));
        Assert.assertEquals(3, ret2.get("three"));
        Assert.assertEquals(4, ret2.get("four"));
    }

    @Test
    public void testArtifactUpdate() {
        // This is a test where we are configured to point right at an artifact dir
        Config conf = new Config();
        conf.put(DaemonConfig.SCHEDULER_CONFIG_LOADER_URI,  ARTIFACTORY_HTTP_SCHEME_PREFIX + "bogushost.yahoo.com:9999/location/of/test/dir");
        conf.put(Config.STORM_LOCAL_DIR, tmpDirPath.toString());

        Time.startSimulating();

        try {
            ArtifactoryConfigLoaderMock loaderMock = new ArtifactoryConfigLoaderMock(conf);

            loaderMock.setData("Anything", "/location/of/test/dir",
                    "{\"children\" : [ { \"uri\" : \"/20160621204337.yaml\", \"folder\" : false }]}" );
            loaderMock.setData(null, null, "{ \"" + DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS + "\": {one: 1, two: 2, three: 3}}");
            Map ret = loaderMock.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);

            Assert.assertNotNull("Unexpectedly returned null", ret);
            Assert.assertEquals(1, ret.get("one"));
            Assert.assertEquals(2, ret.get("two"));
            Assert.assertEquals(3, ret.get("three"));
            Assert.assertNull("Unexpectedly contained \"four\"", ret.get("four"));

            // Now let's load w/o setting up gets and we should still get valid map back
            ArtifactoryConfigLoaderMock tc2 = new ArtifactoryConfigLoaderMock(conf);
            Map ret2 = tc2.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
            Assert.assertNotNull("Unexpectedly returned null", ret2);
            Assert.assertEquals(1, ret2.get("one"));
            Assert.assertEquals(2, ret2.get("two"));
            Assert.assertEquals(3, ret2.get("three"));
            Assert.assertNull("Unexpectedly did not return null", ret2.get("four"));

            // Now let's update it, but not advance time.  Should get old map again.
            loaderMock.setData("Anything", "/location/of/test/dir",
                    "{\"children\" : [ { \"uri\" : \"/20160621204999.yaml\", \"folder\" : false }]}");
            loaderMock.setData(null, null, "{ \"" + DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS + "\": {one: 1, two: 2, three: 3, four : 4}}");
            ret = loaderMock.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
            Assert.assertNotNull("Unexpectedly returned null", ret);
            Assert.assertEquals(1, ret.get("one"));
            Assert.assertEquals(2, ret.get("two"));
            Assert.assertEquals(3, ret.get("three"));
            Assert.assertNull("Unexpectedly did not return null, not enough time passed!", ret.get("four"));

            // Re-load from cached' file.
            ret2 = tc2.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
            Assert.assertNotNull("Unexpectedly returned null", ret2);
            Assert.assertEquals(1, ret2.get("one"));
            Assert.assertEquals(2, ret2.get("two"));
            Assert.assertEquals(3, ret2.get("three"));
            Assert.assertNull("Unexpectedly did not return null, last cached result should not have \"four\"", ret2.get("four"));

            // Now, let's advance time.
            Time.advanceTime(11*60*1000);

            ret = loaderMock.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
            Assert.assertNotNull("Unexpectedly returned null", ret);
            Assert.assertEquals(1, ret.get("one"));
            Assert.assertEquals(2, ret.get("two"));
            Assert.assertEquals(3, ret.get("three"));
            Assert.assertEquals(4, ret.get("four"));

            // Re-load from cached' file.
            ret2 = tc2.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
            Assert.assertNotNull("Unexpectedly returned null", ret2);
            Assert.assertEquals(1, ret2.get("one"));
            Assert.assertEquals(2, ret2.get("two"));
            Assert.assertEquals(3, ret2.get("three"));
            Assert.assertEquals(4, ret2.get("four"));
        } finally {
            Time.stopSimulating();
        }
    }

    @Test
    public void testPointingAtSpecificArtifact() {
        // This is a test where we are configured to point right at a single artifact
        Config conf = new Config();
        conf.put(DaemonConfig.SCHEDULER_CONFIG_LOADER_URI, ARTIFACTORY_HTTP_SCHEME_PREFIX + "bogushost.yahoo.com:9999/location/of/this/artifact");
        conf.put(Config.STORM_LOCAL_DIR, tmpDirPath.toString());

        ArtifactoryConfigLoaderMock loaderMock = new ArtifactoryConfigLoaderMock(conf);

        loaderMock.setData("Anything", "/location/of/this/artifact", "{ \"downloadUri\": \"anything\"}");
        loaderMock.setData(null, null, "{ \"" + DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS + "\": {one: 1, two: 2, three: 3}}");
        Map ret = loaderMock.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);

        Assert.assertNotNull("Unexpectedly returned null", ret);
        Assert.assertEquals(1, ret.get("one"));
        Assert.assertEquals(2, ret.get("two"));
        Assert.assertEquals(3, ret.get("three"));

        // Now let's load w/o setting up gets and we should still get valid map back
        ArtifactoryConfigLoaderMock tc2 = new ArtifactoryConfigLoaderMock(conf);
        Map ret2 = tc2.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
        Assert.assertNotNull("Unexpectedly returned null", ret2);
        Assert.assertEquals(1, ret2.get("one"));
        Assert.assertEquals(2, ret2.get("two"));
        Assert.assertEquals(3, ret2.get("three"));
    }

    @Test
    public void testMalformedYaml() throws Exception {
        // This is a test where we are configured to point right at a single artifact
        Config conf = new Config();
        conf.put(DaemonConfig.SCHEDULER_CONFIG_LOADER_URI, ARTIFACTORY_HTTP_SCHEME_PREFIX + "bogushost.yahoo.com:9999/location/of/this/artifact");
        conf.put(Config.STORM_LOCAL_DIR, tmpDirPath.toString());

        ArtifactoryConfigLoaderMock loaderMock = new ArtifactoryConfigLoaderMock(conf);
        loaderMock.setData("Anything", "/location/of/this/artifact", "{ \"downloadUri\": \"anything\"}");
        loaderMock.setData(null, null, "ThisIsNotValidYaml");

        Map ret = loaderMock.load(DaemonConfig.MULTITENANT_SCHEDULER_USER_POOLS);
        Assert.assertNull("Unexpectedly returned a map", ret);
    }
}
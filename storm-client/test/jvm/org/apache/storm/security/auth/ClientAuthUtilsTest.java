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

package org.apache.storm.security.auth;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class ClientAuthUtilsTest {

    // JUnit ensures that the temporary folder is removed after
    // the test finishes
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test(expected = IOException.class)
    public void getOptionsThrowsOnMissingSectionTest() throws IOException {
        Configuration mockConfig = Mockito.mock(Configuration.class);
        ClientAuthUtils.get(mockConfig, "bogus-section", "");
    }

    @Test
    public void getNonExistentSectionTest() throws IOException {
        Map<String, String> optionMap = new HashMap<String, String>();
        AppConfigurationEntry entry = Mockito.mock(AppConfigurationEntry.class);

        Mockito.<Map<String, ?>>when(entry.getOptions()).thenReturn(optionMap);
        String section = "bogus-section";
        Configuration mockConfig = Mockito.mock(Configuration.class);
        Mockito.when(mockConfig.getAppConfigurationEntry(section))
               .thenReturn(new AppConfigurationEntry[]{ entry });
        Assert.assertNull(
            ClientAuthUtils.get(mockConfig, section, "nonexistent-key"));
    }

    @Test
    public void getFirstValueForValidKeyTest() throws IOException {
        String k = "the-key";
        String expected = "good-value";

        Map<String, String> optionMap = new HashMap<String, String>();
        optionMap.put(k, expected);

        Map<String, String> badOptionMap = new HashMap<String, String>();
        badOptionMap.put(k, "bad-value");

        AppConfigurationEntry emptyEntry = Mockito.mock(AppConfigurationEntry.class);
        AppConfigurationEntry badEntry = Mockito.mock(AppConfigurationEntry.class);
        AppConfigurationEntry goodEntry = Mockito.mock(AppConfigurationEntry.class);

        Mockito.<Map<String, ?>>when(emptyEntry.getOptions()).thenReturn(new HashMap<String, String>());
        Mockito.<Map<String, ?>>when(badEntry.getOptions()).thenReturn(badOptionMap);
        Mockito.<Map<String, ?>>when(goodEntry.getOptions()).thenReturn(optionMap);

        String section = "bogus-section";
        Configuration mockConfig = Mockito.mock(Configuration.class);
        Mockito.when(mockConfig.getAppConfigurationEntry(section))
               .thenReturn(new AppConfigurationEntry[]{ emptyEntry, goodEntry, badEntry });

        Assert.assertEquals(
            ClientAuthUtils.get(mockConfig, section, k), expected);
    }

    @Test
    public void objGettersReturnNullWithNullConfigTest() throws IOException {
        Map<String, Object> topoConf = new HashMap<>();
        Assert.assertNull(ClientAuthUtils.pullConfig(topoConf, "foo"));
        Assert.assertNull(ClientAuthUtils.get(topoConf, "foo", "bar"));

        Assert.assertNull(ClientAuthUtils.getConfiguration(Collections.emptyMap()));
    }

    @Test
    public void getAutoCredentialsTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(Config.TOPOLOGY_AUTO_CREDENTIALS,
                Arrays.asList(new String[]{ "org.apache.storm.security.auth.AuthUtilsTestMock" }));

        Assert.assertTrue(ClientAuthUtils.getAutoCredentials(Collections.emptyMap()).isEmpty());
        Assert.assertEquals(ClientAuthUtils.getAutoCredentials(map).size(), 1);
    }

    @Test
    public void getNimbusAutoCredPluginTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(Config.NIMBUS_AUTO_CRED_PLUGINS,
                Arrays.asList(new String[]{ "org.apache.storm.security.auth.AuthUtilsTestMock" }));

        Assert.assertTrue(ClientAuthUtils.getNimbusAutoCredPlugins(Collections.emptyMap()).isEmpty());
        Assert.assertEquals(ClientAuthUtils.getNimbusAutoCredPlugins(map).size(), 1);
    }

    @Test
    public void GetCredentialRenewersTest() {
        Map<String, Object> map = new HashMap<>();
        map.put(Config.NIMBUS_CREDENTIAL_RENEWERS,
                Arrays.asList(new String[]{ "org.apache.storm.security.auth.AuthUtilsTestMock" }));

        Assert.assertTrue(ClientAuthUtils.getCredentialRenewers(Collections.emptyMap()).isEmpty());
        Assert.assertEquals(ClientAuthUtils.getCredentialRenewers(map).size(), 1);
    }

    @Test
    public void populateSubjectTest() {
        AuthUtilsTestMock autoCred = Mockito.mock(AuthUtilsTestMock.class);
        Subject subject = new Subject();
        Map<String, String> cred = new HashMap<String, String>();
        Collection<IAutoCredentials> autos = Arrays.asList(new IAutoCredentials[]{ autoCred });
        ClientAuthUtils.populateSubject(subject, autos, cred);
        Mockito.verify(autoCred, Mockito.times(1)).populateSubject(subject, cred);
    }

    @Test(expected = RuntimeException.class)
    public void invalidConfigResultsInIOException() throws RuntimeException {
        HashMap<String, Object> conf = new HashMap<>();
        conf.put("java.security.auth.login.config", "__FAKE_FILE__");
        Assert.assertNotNull(ClientAuthUtils.getConfiguration(conf));
    }

    @Test
    public void validConfigResultsInNotNullConfigurationTest() throws IOException {
        File file1 = folder.newFile("mockfile.txt");
        HashMap<String, Object> conf = new HashMap<>();
        conf.put("java.security.auth.login.config", file1.getAbsolutePath());
        Assert.assertNotNull(ClientAuthUtils.getConfiguration(conf));
    }

    @Test(expected = RuntimeException.class)
    public void updateSubjectWithNullThrowsTest() {
        ClientAuthUtils.updateSubject(null, null, null);
    }

    @Test(expected = RuntimeException.class)
    public void updateSubjectWithNullAutosThrowsTest() {
        ClientAuthUtils.updateSubject(new Subject(), null, null);
    }

    @Test
    public void updateSubjectWithNullAutosTest() {
        AuthUtilsTestMock mock = Mockito.mock(AuthUtilsTestMock.class);
        Collection<IAutoCredentials> autos = Arrays.asList(new IAutoCredentials[]{ mock });
        Subject s = new Subject();
        ClientAuthUtils.updateSubject(s, autos, null);
        Mockito.verify(mock, Mockito.times(1)).updateSubject(s, null);
    }

    @Test
    public void pluginCreationTest() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(
            Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, AuthUtilsTestMock.class.getName());
        conf.put(
            Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN, AuthUtilsTestMock.class.getName());

        Assert.assertTrue(
            ClientAuthUtils.getPrincipalToLocalPlugin(conf).getClass() == AuthUtilsTestMock.class);
        Assert.assertTrue(
            ClientAuthUtils.getGroupMappingServiceProviderPlugin(conf).getClass() == AuthUtilsTestMock.class);
    }
}

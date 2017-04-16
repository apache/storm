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
package org.apache.storm.security.auth;

import java.io.IOException;
import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.Subject;

import org.apache.commons.codec.binary.Hex;
import org.apache.storm.Config;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.Test;
import org.mockito.Mockito;

public class AuthUtilsTest {

    @Test(expected = IOException.class)
    public void getOptionsThrowsOnMissingSectionTest() throws IOException {
        Configuration mockConfig = Mockito.mock(Configuration.class);
        AuthUtils.get(mockConfig, "bogus-section", "");
    }

    @Test
    public void getNonExistentSectionTest() throws IOException {
        Map<String, String> optionMap = new HashMap<String, String>();
        AppConfigurationEntry entry = Mockito.mock(AppConfigurationEntry.class);
        
        Mockito.<Map<String, ?>>when(entry.getOptions()).thenReturn(optionMap);
        String section = "bogus-section";
        Configuration mockConfig = Mockito.mock(Configuration.class);
        Mockito.when(mockConfig.getAppConfigurationEntry(section))
               .thenReturn(new AppConfigurationEntry[] {entry});
        Assert.assertNull(
                AuthUtils.get(mockConfig, section, "nonexistent-key"));
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
               .thenReturn(new AppConfigurationEntry[] {emptyEntry, goodEntry, badEntry});

        Assert.assertEquals(
                AuthUtils.get(mockConfig, section, k), expected);
    }

    @Test
    public void objGettersReturnNullWithNullConfigTest() throws IOException {
        Assert.assertNull(AuthUtils.pullConfig(null, "foo"));
        Assert.assertNull(AuthUtils.get(null, "foo", "bar"));

        Map emptyMap = new HashMap<String, String>();
        Assert.assertNull(AuthUtils.GetConfiguration(emptyMap));
    }

    @Test
    public void getAutoCredentialsTest() {
        Map emptyMap = new HashMap<String, String>();
        Map<String, Collection<String>> map = new HashMap<String, Collection<String>>();
        map.put(Config.TOPOLOGY_AUTO_CREDENTIALS, 
                Arrays.asList(new String[]{"org.apache.storm.security.auth.AuthUtilsTestMock"}));

        Assert.assertTrue(AuthUtils.GetAutoCredentials(emptyMap).isEmpty());
        Assert.assertEquals(AuthUtils.GetAutoCredentials(map).size(), 1);
    }

    @Test
    public void getNimbusAutoCredPluginTest() {
        Map emptyMap = new HashMap<String, String>();
        Map<String, Collection<String>> map = new HashMap<String, Collection<String>>();
        map.put(Config.NIMBUS_AUTO_CRED_PLUGINS, 
                Arrays.asList(new String[]{"org.apache.storm.security.auth.AuthUtilsTestMock"}));

        Assert.assertTrue(AuthUtils.getNimbusAutoCredPlugins(emptyMap).isEmpty());
        Assert.assertEquals(AuthUtils.getNimbusAutoCredPlugins(map).size(), 1);
    }

    @Test
    public void GetCredentialRenewersTest() {
        Map emptyMap = new HashMap<String, String>();
        Map<String, Collection<String>> map = new HashMap<String, Collection<String>>();
        map.put(Config.NIMBUS_CREDENTIAL_RENEWERS, 
                Arrays.asList(new String[]{"org.apache.storm.security.auth.AuthUtilsTestMock"}));

        Assert.assertTrue(AuthUtils.GetCredentialRenewers(emptyMap).isEmpty());
        Assert.assertEquals(AuthUtils.GetCredentialRenewers(map).size(), 1);
    }

    @Test
    public void populateSubjectTest() {
        AuthUtilsTestMock autoCred = Mockito.mock(AuthUtilsTestMock.class);
        Subject subject = new Subject();
        Map<String, String> cred = new HashMap<String, String>();
        Collection<IAutoCredentials> autos = Arrays.asList(new IAutoCredentials[]{autoCred}); 
        AuthUtils.populateSubject(subject, autos, cred);
        Mockito.verify(autoCred, Mockito.times(1)).populateSubject(subject, cred); 
    }

    @Test
    public void makeDigestPayloadTest() throws NoSuchAlgorithmException { 
        String section = "user-pass-section";
        Map<String, String> optionMap = new HashMap<String, String>();
        String user = "user";
        String pass = "pass";
        optionMap.put("username", user);
        optionMap.put("password", pass);
        AppConfigurationEntry entry = Mockito.mock(AppConfigurationEntry.class);
        
        Mockito.<Map<String, ?>>when(entry.getOptions()).thenReturn(optionMap);
        Configuration mockConfig = Mockito.mock(Configuration.class);
        Mockito.when(mockConfig.getAppConfigurationEntry(section))
               .thenReturn(new AppConfigurationEntry[] {entry});

        MessageDigest digest = MessageDigest.getInstance("SHA-512");
        byte[] output = digest.digest((user + ":" + pass).getBytes());
        String sha = Hex.encodeHexString(output);

        // previous code used this method to generate the string, ensure the two match
        StringBuilder builder = new StringBuilder();        
        for(byte b : output) {     
            builder.append(String.format("%02x", b));      
        }      
        String stringFormatMethod = builder.toString();

        Assert.assertEquals(
            AuthUtils.makeDigestPayload(mockConfig, "user-pass-section"),
            sha);

        Assert.assertEquals(sha, stringFormatMethod); 
    }

    @Test(expected = RuntimeException.class)
    public void invalidConfigResultsInIOException() throws RuntimeException {
        HashMap<String, String> conf = new HashMap<String, String>();
        conf.put("java.security.auth.login.config", "__FAKE_FILE__");
        Assert.assertNotNull(AuthUtils.GetConfiguration(conf));
    }

    // JUnit ensures that the temporary folder is removed after
    // the test finishes
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void validConfigResultsInNotNullConfigurationTest() throws IOException {
        File file1 = folder.newFile("mockfile.txt");
        HashMap<String, String> conf = new HashMap<String, String>();
        conf.put("java.security.auth.login.config", file1.getAbsolutePath());
        Assert.assertNotNull(AuthUtils.GetConfiguration(conf));
    }

    @Test
    public void uiHttpCredentialsPluginTest(){
        Map conf = new HashMap<String, String>();
        conf.put(
            Config.UI_HTTP_CREDS_PLUGIN, 
            "org.apache.storm.security.auth.AuthUtilsTestMock");
        conf.put(
            Config.DRPC_HTTP_CREDS_PLUGIN, 
            "org.apache.storm.security.auth.AuthUtilsTestMock");
        conf.put(
            Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, 
            "org.apache.storm.security.auth.AuthUtilsTestMock");
        conf.put(
            Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN, 
            "org.apache.storm.security.auth.AuthUtilsTestMock");

        Assert.assertTrue(
            AuthUtils.GetUiHttpCredentialsPlugin(conf).getClass() == AuthUtilsTestMock.class);
        Assert.assertTrue(
            AuthUtils.GetDrpcHttpCredentialsPlugin(conf).getClass() == AuthUtilsTestMock.class);
        Assert.assertTrue(
            AuthUtils.GetPrincipalToLocalPlugin(conf).getClass() == AuthUtilsTestMock.class);
        Assert.assertTrue(
            AuthUtils.GetGroupMappingServiceProviderPlugin(conf).getClass() == AuthUtilsTestMock.class);
    }

    @Test(expected = RuntimeException.class)
    public void updateSubjectWithNullThrowsTest() {
        AuthUtils.updateSubject(null, null, null);
    }

    @Test(expected = RuntimeException.class)
    public void updateSubjectWithNullAutosThrowsTest() {
        AuthUtils.updateSubject(new Subject(), null, null);
    }

    @Test
    public void updateSubjectWithNullAutosTest() {
        AuthUtilsTestMock mock = Mockito.mock(AuthUtilsTestMock.class);
        Collection<IAutoCredentials> autos = Arrays.asList(new IAutoCredentials[]{mock}); 
        Subject s = new Subject();
        AuthUtils.updateSubject(s, autos, null);
        Mockito.verify(mock, Mockito.times(1)).updateSubject(s, null);
    }
}

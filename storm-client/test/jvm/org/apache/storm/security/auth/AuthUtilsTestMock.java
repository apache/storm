/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.security.auth;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.storm.security.INimbusCredentialPlugin;

public class AuthUtilsTestMock implements IAutoCredentials,
                                          ICredentialsRenewer,
                                          INimbusCredentialPlugin,
                                          IPrincipalToLocal,
                                          IGroupMappingServiceProvider {

    // IAutoCredentials
    // ICredentialsRenewer
    // INimbusCredentialPlugin 
    // IPrincipalToLocal 
    // IGroupMappingServiceProvider 
    @Override
    public void prepare(Map<String, Object> conf) {}

    // IPrincipalToLocal 
    @Override
    public String toLocal(String principal) {
        return null;
    }

    // IGroupMappingServiceProvider 
    @Override
    public Set<String> getGroups(String user) throws IOException {
        return null;
    }

    // ICredentialsRenewer
    @Override
    public void renew(Map<String, String> credentials, Map<String, Object> topologyConf, String ownerPrincipal) {}

    // IAutoCredentials
    @Override
    public void updateSubject(Subject subject, Map<String, String> creds) {}

    // IAutoCredentials
    @Override
    public void populateSubject(Subject subject, Map<String, String> creds) {}

    // IAutoCredentials
    @Override
    public void populateCredentials(Map<String, String> creds) {}

    // INimbusCredentialPlugin
    @Override
    public void populateCredentials(Map<String, String> credentials, Map<String, Object> topoConf) {}

    // Shutdownable via INimbusCredentailPlugin
    @Override
    public void shutdown() {}
}

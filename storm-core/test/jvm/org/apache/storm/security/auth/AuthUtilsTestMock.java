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
import java.security.Principal;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import org.apache.storm.security.INimbusCredentialPlugin;

public class AuthUtilsTestMock implements IAutoCredentials, 
                                          ICredentialsRenewer,
                                          IHttpCredentialsPlugin, 
                                          INimbusCredentialPlugin,
                                          IPrincipalToLocal,
                                          IGroupMappingServiceProvider {

    // IAutoCredentials
    // ICredentialsRenewer
    // IHttpCredentialsPlugin
    // INimbusCredentialPlugin 
    // IPrincipalToLocal 
    // IGroupMappingServiceProvider 
    public void prepare(Map conf) {}

    // IHttpCredentialsPlugin
    public ReqContext populateContext(ReqContext ctx, HttpServletRequest req) {
        return null;
    }

    // IHttpCredentialsPlugin
    public String getUserName(HttpServletRequest req){
        return null;
    }

    // IPrincipalToLocal 
    public String toLocal(Principal principal) {
        return null;
    }

    // IGroupMappingServiceProvider 
    public Set<String> getGroups(String user) throws IOException {
        return null;
    }

    // ICredentialsRenewer
    public void renew(Map<String, String> credentials, Map topologyConf) {}

    // IAutoCredentials
    public void updateSubject(Subject subject, Map<String,String> conf) {}

    // IAutoCredentials
    public void populateSubject(Subject subject, Map<String,String> conf) {}

    // IAutoCredentials
    public void populateCredentials(Map<String,String> conf) {}

    // INimbusCredentialPlugin
    public void populateCredentials(Map<String,String> credentials, Map conf) {}

    // Shutdownable via INimbusCredentailPlugin
    public void shutdown() {}
}

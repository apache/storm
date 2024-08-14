/*
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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class X509CertOrKerberosPrincipalToLocal implements IPrincipalToLocal {
    private static final Logger LOG = LoggerFactory.getLogger(X509CertOrKerberosPrincipalToLocal.class);

    X509CertPrincipalToLocal x509CertPrincipalToLocal;
    KerberosPrincipalToLocal kerberosPrincipalToLocal;

    @Override
    public void prepare(Map<String, Object> conf) {
        x509CertPrincipalToLocal = new X509CertPrincipalToLocal();
        kerberosPrincipalToLocal = new KerberosPrincipalToLocal();

        x509CertPrincipalToLocal.prepare(conf);
        kerberosPrincipalToLocal.prepare(conf);
    }

    @Override
    public String toLocal(String principalName) {
        String localName = null;
        try {
            localName = x509CertPrincipalToLocal.toLocal(principalName);
            LOG.debug("{} translates principal {} to {}",
                    x509CertPrincipalToLocal.getClass().getCanonicalName(), principalName, localName);
        } catch (RuntimeException e) {
            //ignore x509CertPrincipalToLocal error.
            LOG.debug("Error reading localName from x509CertPrincipalToLocal. The error will be ignored. Error: " + e.getMessage());
        }

        if (localName == null) {
            localName = kerberosPrincipalToLocal.toLocal(principalName);
            LOG.debug("{} translates principal {} to {}",
                    kerberosPrincipalToLocal.getClass().getCanonicalName(), principalName, localName);
        }

        return localName;
    }
}

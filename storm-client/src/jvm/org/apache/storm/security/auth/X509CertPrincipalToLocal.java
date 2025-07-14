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

import java.security.AccessControlException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.ConfigurationException;
import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class X509CertPrincipalToLocal implements IPrincipalToLocal {
    private static final Logger LOG = LoggerFactory.getLogger(X509CertPrincipalToLocal.class);

    public static final String X509_CERT_PRINCIPAL_TO_LOCAL_REGEX = "x509.cert.principal.to.local.regex";
    private Pattern pattern;

    private static String extractCn(final String subjectPrincipal) {
        if (subjectPrincipal == null) {
            return null;
        }
        try {
            final List<Rdn> rdns = new LdapName(subjectPrincipal).getRdns();
            for (int i = rdns.size() - 1; i >= 0; i--) {
                final Rdn rdn = rdns.get(i);
                if (rdn.getType().equals("CN")) {
                    return String.valueOf(rdn.getValue());
                }
            }
            return null;
        } catch (final InvalidNameException e) {
            throw new AccessControlException(subjectPrincipal + " is not a valid X500 distinguished name");
        }
    }

    @Override
    public void prepare(Map<String, Object> conf) {
        if (conf.get(X509_CERT_PRINCIPAL_TO_LOCAL_REGEX) == null) {
            throw new IllegalStateException(X509_CERT_PRINCIPAL_TO_LOCAL_REGEX + " is not configured");
        }
        pattern = Pattern.compile(conf.get(X509_CERT_PRINCIPAL_TO_LOCAL_REGEX).toString());
    }

    @Override
    public String toLocal(String principalName) {
        Matcher roleMatcher = pattern.matcher(extractCn(principalName));
        if (roleMatcher.find()) {
            for (int i = 1; i <= roleMatcher.groupCount(); ++i) {
                if (roleMatcher.group(i) != null) {
                    return roleMatcher.group(i);
                }
            }
        }
        throw new AccessControlException("Invalid principal " + principalName);
    }


}
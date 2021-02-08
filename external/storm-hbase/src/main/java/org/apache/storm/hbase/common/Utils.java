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

package org.apache.storm.hbase.common;

import java.io.IOException;
import java.math.BigDecimal;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    public static final String TOKEN_KIND_HBASE_AUTH_TOKEN = "HBASE_AUTH_TOKEN";
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private Utils() {}

    public static Table getTable(UserProvider provider, Configuration config, String tableName)
        throws IOException, InterruptedException {
        UserGroupInformation ugi;
        if (provider != null) {
            ugi = provider.getCurrent().getUGI();
            LOG.debug("Current USER for provider: {}", ugi.getUserName());
        } else {
            // autocreds puts delegation token into current user UGI
            ugi = UserGroupInformation.getCurrentUser();

            LOG.debug("UGI for current USER : {}", ugi.getUserName());
            if (ugi.hasKerberosCredentials()) {
                LOG.debug("UGI has Kerberos credentials");
            } else {
                boolean foundHBaseAuthToken = false;
                for (Token<? extends TokenIdentifier> token : ugi.getTokens()) {
                    LOG.debug("Token in UGI (delegation token): {} / {}", token.toString(),
                            token.decodeIdentifier().getUser());

                    // token.getKind() = Text, Text is annotated by @Stringable
                    // which ensures toString() implementation
                    if (token.getKind().toString().equals(TOKEN_KIND_HBASE_AUTH_TOKEN)) {
                        // use UGI from token
                        if (!foundHBaseAuthToken) {
                            LOG.debug("Found HBASE_AUTH_TOKEN - using the token to replace current user.");

                            ugi = token.decodeIdentifier().getUser();
                            ugi.addToken(token);

                            foundHBaseAuthToken = true;
                        } else {
                            LOG.warn("Found multiple HBASE_AUTH_TOKEN - will use already found token. "
                                    + "Please enable DEBUG log level to track delegation tokens.");
                        }
                    }
                }

                if (!foundHBaseAuthToken) {
                    LOG.warn("Can't find HBase auth token in delegation tokens.");
                }
            }
        }

        return ugi.doAs(new PrivilegedExceptionAction<Table>() {
            @Override
            public Table run() throws IOException {
                return ConnectionFactory.createConnection(config).getTable(TableName.valueOf(tableName));
            }
        });
    }

    public static long toLong(Object obj) {
        long l = 0;
        if (obj != null) {
            if (obj instanceof Number) {
                l = ((Number) obj).longValue();
            } else {
                LOG.warn("Could not coerce {} to Long", obj.getClass().getName());
            }
        }
        return l;
    }

    public static byte[] toBytes(Object obj) {
        if (obj == null) {
            return null;
        } else if (obj instanceof String) {
            return ((String) obj).getBytes();
        } else if (obj instanceof Integer) {
            return Bytes.toBytes((Integer) obj);
        } else if (obj instanceof Long) {
            return Bytes.toBytes((Long) obj);
        } else if (obj instanceof Short) {
            return Bytes.toBytes((Short) obj);
        } else if (obj instanceof Float) {
            return Bytes.toBytes((Float) obj);
        } else if (obj instanceof Double) {
            return Bytes.toBytes((Double) obj);
        } else if (obj instanceof Boolean) {
            return Bytes.toBytes((Boolean) obj);
        } else if (obj instanceof BigDecimal) {
            return Bytes.toBytes((BigDecimal) obj);
        } else {
            LOG.error("Can't convert class to byte array: " + obj.getClass().getName());
            return new byte[0];
        }
    }
}

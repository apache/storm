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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import javax.security.auth.Subject;
import javax.xml.bind.DatatypeConverter;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This plugin is intended to be used for user topologies to send SSL keystore/truststore files to the remote workers. On the client side,
 * this takes the files specified in ssl.credential.files, reads the file contents, base64's it, converts it to a String, and adds it to the
 * credentials map. The key in the credentials map is the name of the file. On the worker side it uses the filenames from the
 * ssl.credential.files config to lookup the keys in the credentials map and decodes it and writes it back out as a file.
 *
 * <p>User is responsible for referencing them from the topology code as {@code filename}.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class AutoSSL implements IAutoCredentials {
    public static final String SSL_FILES_CONF = "ssl.credential.files";
    private static final Logger LOG = LoggerFactory.getLogger(AutoSSL.class);
    private Map<String, Object> conf;
    private String writeDir = "./";

    // Adds the serialized and base64 file to the credentials map as a string with the filename as
    // the key.
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static void serializeSSLFile(String readFile, Map<String, String> credentials) {
        try (FileInputStream in = new FileInputStream(readFile)) {
            LOG.debug("serializing ssl file: {}", readFile);
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int length;
            while ((length = in.read(buffer)) != -1) {
                result.write(buffer, 0, length);
            }
            String resultStr = DatatypeConverter.printBase64Binary(result.toByteArray());

            File f = new File(readFile);
            LOG.debug("ssl read files is name: {}", f.getName());
            credentials.put(f.getName(), resultStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static void deserializeSSLFile(String credsKey, String directory,
                                          Map<String, String> credentials) {
        try {
            LOG.debug("deserializing ssl file with key: {}", credsKey);
            String resultStr = null;

            if (credentials != null
                    && credentials.containsKey(credsKey)
                    && credentials.get(credsKey) != null) {
                resultStr = credentials.get(credsKey);
            }
            if (resultStr != null) {
                byte[] decodedData = DatatypeConverter.parseBase64Binary(resultStr);
                File f = new File(directory, credsKey);
                try (FileOutputStream fout = new FileOutputStream(f)) {
                    fout.write(decodedData);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void prepare(Map<String, Object> conf) {
        this.conf = conf;
        writeDir = getSSLWriteDirFromConf(this.conf);
    }

    @VisibleForTesting
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    protected String getSSLWriteDirFromConf(Map<String, Object> conf) {
        return "./";
    }

    @VisibleForTesting
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    Collection<String> getSSLFilesFromConf(Map<String, Object> conf) {
        Object sslConf = conf.get(SSL_FILES_CONF);
        if (sslConf == null) {
            LOG.info("No ssl files requested, if you want to use SSL please set {} to the list of files",
                    SSL_FILES_CONF);
            return null;
        }
        Collection<String> sslFiles = null;
        if (sslConf instanceof Collection) {
            sslFiles = (Collection<String>) sslConf;
        } else if (sslConf instanceof String) {
            sslFiles = Arrays.asList(((String) sslConf).split(","));
        } else {
            throw new RuntimeException(
                SSL_FILES_CONF + " is not set to something that I know how to use " + sslConf);
        }
        return sslFiles;
    }

    @Override
    public void populateCredentials(Map<String, String> credentials) {
        try {
            Collection<String> sslFiles = getSSLFilesFromConf(conf);
            if (sslFiles == null) {
                return;
            }
            LOG.info("AutoSSL files: {}", sslFiles);
            for (String inputFile : sslFiles) {
                serializeSSLFile(inputFile, credentials);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateSubject(Subject subject, Map<String, String> credentials) {
        populateSubject(subject, credentials);
    }

    @Override
    public void populateSubject(Subject subject, Map<String, String> credentials) {
        LOG.info("AutoSSL populating credentials");
        Collection<String> sslFiles = getSSLFilesFromConf(conf);
        if (sslFiles == null) {
            LOG.debug("ssl files is null");
            return;
        }
        for (String outputFile : sslFiles) {
            deserializeSSLFile(new File(outputFile).getName(), writeDir, credentials);
        }
    }
}

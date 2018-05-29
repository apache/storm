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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AutoSSLTest {
    final static Logger LOG = LoggerFactory.getLogger(AutoSSLTest.class);

    @Test
    public void testgetSSLFilesFromConf() throws Exception {
        AutoSSL assl = new AutoSSL();
        Map<String, Object> conf = new HashMap<>();
        assertNull(assl.getSSLFilesFromConf(conf));
        conf.put(AutoSSL.SSL_FILES_CONF, "sslfile1.txt");
        assl.prepare(conf);
        Collection<String> sslFiles = assl.getSSLFilesFromConf(conf);
        assertNotNull(sslFiles);
        assertEquals(1, sslFiles.size());
        for (String file : sslFiles) {
            assertEquals("sslfile1.txt", file);
        }
    }

    @Test
    public void testgetSSLFilesFromConfMultipleComma() throws Exception {
        AutoSSL assl = new AutoSSL();
        Map<String, Object> conf = new HashMap<>();
        assertNull(assl.getSSLFilesFromConf(conf));
        conf.put(AutoSSL.SSL_FILES_CONF, "sslfile1.txt,sslfile2.txt,sslfile3.txt");
        assl.prepare(conf);
        Collection<String> sslFiles = assl.getSSLFilesFromConf(conf);
        assertNotNull(sslFiles);
        assertEquals(3, sslFiles.size());
        List<String> valid = new ArrayList<>();
        Collections.addAll(valid, "sslfile1.txt", "sslfile2.txt", "sslfile3.txt");
        for (String file : sslFiles) {
            assertTrue("removing: " + file, valid.remove(file));
        }
        assertEquals(0, valid.size());
    }

    @Test
    public void testpopulateCredentials() throws Exception {
        File temp = File.createTempFile("tmp-autossl-test", ".txt");
        temp.deleteOnExit();
        List<String> lines = Arrays.asList("The first line", "The second line");
        Files.write(temp.toPath(), lines, Charset.forName("UTF-8"));
        File baseDir = null;
        try {
            baseDir = new File("/tmp/autossl-test-" + UUID.randomUUID());
            if (!baseDir.mkdir()) {
                throw new IOException("failed to create base directory");
            }
            AutoSSL assl = new TestAutoSSL(baseDir.getPath());

            LOG.debug("base dir is; " + baseDir);
            Map<String, Object> sslconf = new HashMap<>();

            sslconf.put(AutoSSL.SSL_FILES_CONF, temp.getPath());
            assl.prepare(sslconf);
            Collection<String> sslFiles = assl.getSSLFilesFromConf(sslconf);

            Map<String, String> creds = new HashMap<>();
            assl.populateCredentials(creds);
            assertTrue(creds.containsKey(temp.getName()));

            Subject unusedSubject = new Subject();
            assl.populateSubject(unusedSubject, creds);
            String[] outputFiles = baseDir.list();
            assertEquals(1, outputFiles.length);

            // compare contents of files
            if (outputFiles.length > 0) {
                List<String> linesWritten = FileUtils.readLines(new File(baseDir, outputFiles[0]),
                                                                Charset.forName("UTF-8"));
                for (String l : linesWritten) {
                    assertTrue(lines.contains(l));
                }
            }
        } finally {
            if (baseDir != null) {
                FileUtils.deleteDirectory(baseDir);
            }
        }
    }

    // Test class to override the write directory
    public class TestAutoSSL extends AutoSSL {

        String baseDir = null;

        TestAutoSSL(String newDir) {
            baseDir = newDir;
        }

        @Override
        protected String getSSLWriteDirFromConf(Map<String, Object> conf) {
            return baseDir;
        }
    }
}

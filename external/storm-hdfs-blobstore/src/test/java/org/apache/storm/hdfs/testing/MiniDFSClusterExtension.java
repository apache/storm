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
package org.apache.storm.hdfs.testing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.util.function.Supplier;

import static org.apache.hadoop.test.GenericTestUtils.DEFAULT_TEST_DATA_DIR;
import static org.apache.hadoop.test.GenericTestUtils.SYSPROP_TEST_DATA_DIR;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MiniDFSClusterExtension implements BeforeEachCallback, AfterEachCallback {

    private static final String TEST_BUILD_DATA = "test.build.data";

    private final Supplier<Configuration> hadoopConfSupplier;
    private Configuration hadoopConf;
    private MiniDFSCluster dfscluster;

    public MiniDFSClusterExtension() {
        this(() -> new Configuration());
    }

    public MiniDFSClusterExtension(Supplier<Configuration> hadoopConfSupplier) {
        this.hadoopConfSupplier = hadoopConfSupplier;
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }

    public MiniDFSCluster getDfscluster() {
        return dfscluster;
    }

    @Override
    public void beforeEach(ExtensionContext arg0) throws Exception {
        System.setProperty(TEST_BUILD_DATA, "target/test/data");
        hadoopConf = hadoopConfSupplier.get();
        String tempDir = getTestDir("dfs").getAbsolutePath() + File.separator;
        hadoopConf.set("hdfs.minidfs.basedir", tempDir);
        dfscluster = new MiniDFSCluster.Builder(hadoopConf).numDataNodes(3).build();
        dfscluster.waitActive();
    }

    @Override
    public void afterEach(ExtensionContext arg0) throws Exception {
        dfscluster.shutdown();
        System.clearProperty(TEST_BUILD_DATA);
    }

    /**
     * Get an uncreated directory for tests.
     * We use this method to get rid of getTestDir() in GenericTestUtils in Hadoop code
     * which uses assert from junit4.
     * @return the absolute directory for tests. Caller is expected to create it.
     */
    public static File getTestDir(String subdir) {
        return new File(getTestDir(), subdir).getAbsoluteFile();
    }

    /**
     * Get the (created) base directory for tests.
     * @return the absolute directory
     */
    public static File getTestDir() {
        String prop = System.getProperty(SYSPROP_TEST_DATA_DIR, DEFAULT_TEST_DATA_DIR);
        if (prop.isEmpty()) {
            // corner case: property is there but empty
            prop = DEFAULT_TEST_DATA_DIR;
        }
        File dir = new File(prop).getAbsoluteFile();
        dir.mkdirs();
        assertTrue(dir.exists(), "File " + dir + " should exist");
        return dir;
    }
}

/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hdfs.testing;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @deprecated Use {@link MiniDFSClusterExtension} instead, along with JUnit 5 for new tests.
 */
@Deprecated
public class MiniDFSClusterRule implements TestRule {

    private static final String TEST_BUILD_DATA = "test.build.data";

    private final Supplier<Configuration> hadoopConfSupplier;
    private Configuration hadoopConf;
    private MiniDFSCluster dfscluster;

    public MiniDFSClusterRule() {
        this(() -> new Configuration());
    }

    public MiniDFSClusterRule(Supplier<Configuration> hadoopConfSupplier) {
        this.hadoopConfSupplier = hadoopConfSupplier;
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }

    public MiniDFSCluster getDfscluster() {
        return dfscluster;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    System.setProperty(TEST_BUILD_DATA, "target/test/data");
                    hadoopConf = hadoopConfSupplier.get();
                    dfscluster = new MiniDFSCluster.Builder(hadoopConf).numDataNodes(3).build();
                    dfscluster.waitActive();
                    base.evaluate();
                } finally {
                    if (dfscluster != null) {
                        dfscluster.shutdown();
                    }
                    System.clearProperty(TEST_BUILD_DATA);
                }
            }
        };
    }

}

/*
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
 *
 */

package org.apache.storm.sql;

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.utils.Utils;

public final class SqlTestUtil {

    public static void runStormTopology(LocalCluster cluster, final List<?> watchedList, final int expectedValueSize,
                                        AbstractStreamsProcessor proc, StormTopology topo) throws Exception {
        final Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setDebug(true);

        if (proc.getClassLoaders() != null && proc.getClassLoaders().size() > 0) {
            CompilingClassLoader lastClassloader = proc.getClassLoaders().get(proc.getClassLoaders().size() - 1);
            Utils.setClassLoaderForJavaDeSerialize(lastClassloader);
        }

        try (LocalCluster.LocalTopology stormTopo = cluster.submitTopology("storm-sql", conf, topo)) {
            waitForCompletion(1000 * 1000, () -> watchedList.size() < expectedValueSize);
        } finally {
            while (cluster.getTopologySummaries().size() > 0) {
                Thread.sleep(10);
            }
            Utils.resetClassLoaderForJavaDeSerialize();
        }
    }

    private static void waitForCompletion(long timeout, Callable<Boolean> cond) throws Exception {
        long start = TestUtils.monotonicNow();
        while (TestUtils.monotonicNow() - start < timeout && cond.call()) {
            Thread.sleep(100);
        }
    }
}

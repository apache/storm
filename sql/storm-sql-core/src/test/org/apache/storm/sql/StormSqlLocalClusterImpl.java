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

import java.util.function.Predicate;
import org.apache.calcite.sql.SqlNode;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.parser.SqlCreateFunction;
import org.apache.storm.sql.parser.SqlCreateTable;
import org.apache.storm.sql.parser.StormParser;
import org.apache.storm.utils.Utils;

public class StormSqlLocalClusterImpl {
    private final StormSqlContext sqlContext;

    public StormSqlLocalClusterImpl() {
        sqlContext = new StormSqlContext();
    }

    private static void waitForCompletion(long timeout, Predicate<Void> cond) throws Exception {
        long start = TestUtils.monotonicNow();
        while (TestUtils.monotonicNow() - start < timeout && !cond.test(null)) {
            Thread.sleep(100);
        }
    }

    public void runLocal(LocalCluster localCluster, Iterable<String> statements,
                         Predicate<Void> waitCondition, long waitTimeoutMs) throws Exception {
        final Config conf = new Config();
        conf.setMaxSpoutPending(20);

        for (String sql : statements) {
            StormParser parser = new StormParser(sql);
            SqlNode node = parser.impl().parseSqlStmtEof();
            if (node instanceof SqlCreateTable) {
                sqlContext.interpretCreateTable((SqlCreateTable) node);
            } else if (node instanceof SqlCreateFunction) {
                sqlContext.interpretCreateFunction((SqlCreateFunction) node);
            } else {
                AbstractStreamsProcessor processor = sqlContext.compileSql(sql);
                StormTopology topo = processor.build();

                if (processor.getClassLoaders() != null && processor.getClassLoaders().size() > 0) {
                    CompilingClassLoader lastClassloader = processor.getClassLoaders().get(processor.getClassLoaders().size() - 1);
                    Utils.setClassLoaderForJavaDeSerialize(lastClassloader);
                }

                try (LocalCluster.LocalTopology stormTopo = localCluster.submitTopology("storm-sql", conf, topo)) {
                    waitForCompletion(waitTimeoutMs, waitCondition);
                } finally {
                    while (localCluster.getTopologySummaries().size() > 0) {
                        Thread.sleep(10);
                    }
                    Utils.resetClassLoaderForJavaDeSerialize();
                }
            }
        }
    }
}

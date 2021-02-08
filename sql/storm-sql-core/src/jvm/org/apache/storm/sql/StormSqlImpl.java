/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.sql;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import org.apache.calcite.sql.SqlNode;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.sql.javac.CompilingClassLoader;
import org.apache.storm.sql.parser.SqlCreateFunction;
import org.apache.storm.sql.parser.SqlCreateTable;
import org.apache.storm.sql.parser.StormParser;

class StormSqlImpl extends StormSql {
    private final StormSqlContext sqlContext;

    StormSqlImpl() {
        sqlContext = new StormSqlContext();
    }

    @Override
    public void submit(
        String name, Iterable<String> statements, Map<String, Object> topoConf, SubmitOptions opts,
        StormSubmitter.ProgressListener progressListener, String asUser)
        throws Exception {
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

                Path jarPath = null;
                try {
                    // QueryPlanner on Streams mode configures the topology with compiled classes,
                    // so we need to add new classes into topology jar
                    // Topology will be serialized and sent to Nimbus, and deserialized and executed in workers.

                    jarPath = Files.createTempFile("storm-sql", ".jar");
                    System.setProperty("storm.jar", jarPath.toString());
                    packageTopology(jarPath, processor);
                    StormSubmitter.submitTopologyAs(name, topoConf, topo, opts, progressListener, asUser);
                } finally {
                    if (jarPath != null) {
                        Files.delete(jarPath);
                    }
                }
            }
        }
    }

    @Override
    public void explain(Iterable<String> statements) throws Exception {
        for (String sql : statements) {
            System.out.println("===========================================================");
            System.out.println("query>");
            System.out.println(sql);
            System.out.println("-----------------------------------------------------------");

            StormParser parser = new StormParser(sql);
            SqlNode node = parser.impl().parseSqlStmtEof();
            if (node instanceof SqlCreateTable) {
                sqlContext.interpretCreateTable((SqlCreateTable) node);
                System.out.println("No plan presented on DDL");
            } else if (node instanceof SqlCreateFunction) {
                sqlContext.interpretCreateFunction((SqlCreateFunction) node);
                System.out.println("No plan presented on DDL");
            } else {
                String plan = sqlContext.explain(sql);
                System.out.println("plan>");
                System.out.println(plan);
            }

            System.out.println("===========================================================");
        }
    }

    private void packageTopology(Path jar, AbstractStreamsProcessor processor) throws IOException {
        Manifest manifest = new Manifest();
        Attributes attr = manifest.getMainAttributes();
        attr.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        attr.put(Attributes.Name.MAIN_CLASS, processor.getClass().getCanonicalName());
        try (JarOutputStream out = new JarOutputStream(
            new BufferedOutputStream(new FileOutputStream(jar.toFile())), manifest)) {
            List<CompilingClassLoader> classLoaders = processor.getClassLoaders();
            if (classLoaders != null && !classLoaders.isEmpty()) {
                for (CompilingClassLoader classLoader : classLoaders) {
                    for (Map.Entry<String, ByteArrayOutputStream> e : classLoader.getClasses().entrySet()) {
                        out.putNextEntry(new ZipEntry(e.getKey().replace(".", "/") + ".class"));
                        out.write(e.getValue().toByteArray());
                        out.closeEntry();
                    }
                }
            }
        }
    }
}

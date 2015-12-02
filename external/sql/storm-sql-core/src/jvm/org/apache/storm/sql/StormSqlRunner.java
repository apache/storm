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
package org.apache.storm.sql;

import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyInitialStatus;
import backtype.storm.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class StormSqlRunner {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("storm-sql <topo-name> <sql-file>");
            return;
        }
        String topoName = args[0];
        List<String> stmts = Files.readAllLines(Paths.get(args[1]), StandardCharsets.UTF_8);
        StormSql sql = StormSql.construct();
        @SuppressWarnings("unchecked")
        Map<String, ?> conf = Utils.readStormConfig();
        SubmitOptions options = new SubmitOptions(TopologyInitialStatus.ACTIVE);
        sql.submit(topoName, stmts, conf, options, null, null);
    }
}

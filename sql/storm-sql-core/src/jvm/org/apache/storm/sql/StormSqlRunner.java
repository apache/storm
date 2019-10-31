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

package org.apache.storm.sql;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.utils.Utils;

public class StormSqlRunner {
    private static final String OPTION_SQL_FILE_SHORT = "f";
    private static final String OPTION_SQL_FILE_LONG = "file";
    private static final String OPTION_SQL_TOPOLOGY_NAME_SHORT = "t";
    private static final String OPTION_SQL_TOPOLOGY_NAME_LONG = "topology";
    private static final String OPTION_SQL_EXPLAIN_SHORT = "e";
    private static final String OPTION_SQL_EXPLAIN_LONG = "explain";

    public static void main(String[] args) throws Exception {
        Options options = buildOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        if (!commandLine.hasOption(OPTION_SQL_FILE_LONG)) {
            printUsageAndExit(options, OPTION_SQL_FILE_LONG + " is required");
        }

        String filePath = commandLine.getOptionValue(OPTION_SQL_FILE_LONG);
        List<String> stmts = Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8);
        StormSql sql = StormSql.construct();
        @SuppressWarnings("unchecked")
        Map<String, Object> conf = Utils.readStormConfig();

        if (commandLine.hasOption(OPTION_SQL_EXPLAIN_LONG)) {
            sql.explain(stmts);
        } else if (commandLine.hasOption(OPTION_SQL_TOPOLOGY_NAME_LONG)) {
            String topoName = commandLine.getOptionValue(OPTION_SQL_TOPOLOGY_NAME_LONG);
            SubmitOptions submitOptions = new SubmitOptions(TopologyInitialStatus.ACTIVE);
            sql.submit(topoName, stmts, conf, submitOptions, null, null);
        } else {
            printUsageAndExit(options, "Either " + OPTION_SQL_TOPOLOGY_NAME_LONG
                    + " or " + OPTION_SQL_EXPLAIN_LONG + " must be presented");
        }
    }

    private static void printUsageAndExit(Options options, String message) {
        System.out.println(message);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("storm-sql-runner ", options);
        System.exit(1);
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(OPTION_SQL_FILE_SHORT, OPTION_SQL_FILE_LONG, true, "REQUIRED SQL file which has sql statements");
        options.addOption(OPTION_SQL_TOPOLOGY_NAME_SHORT, OPTION_SQL_TOPOLOGY_NAME_LONG, true, "Topology name to submit");
        options
            .addOption(OPTION_SQL_EXPLAIN_SHORT, OPTION_SQL_EXPLAIN_LONG, false, "Activate explain mode (topology name will be ignored)");
        return options;
    }

}

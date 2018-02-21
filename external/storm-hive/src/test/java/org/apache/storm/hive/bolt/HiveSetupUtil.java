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

package org.apache.storm.hive.bolt;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.hcatalog.streaming.QueryFailedException;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class HiveSetupUtil {
    public static class RawFileSystem extends RawLocalFileSystem {
        private static final URI NAME;
        static {
            try {
                NAME = new URI("raw:///");
            } catch (URISyntaxException se) {
                throw new IllegalArgumentException("bad uri", se);
            }
        }

        @Override
        public URI getUri() {
            return NAME;
        }

        @Override
        public FileStatus getFileStatus(Path path) throws IOException {
            File file = pathToFile(path);
            if (!file.exists()) {
                throw new FileNotFoundException("Can't find " + path);
            }
            // get close enough
            short mod = 0;
            if (file.canRead()) {
                mod |= 0444;
            }
            if (file.canWrite()) {
                mod |= 0200;
            }
            if (file.canExecute()) {
                mod |= 0111;
            }
            ShimLoader.getHadoopShims();
            return new FileStatus(file.length(), file.isDirectory(), 1, 1024,
                                  file.lastModified(), file.lastModified(),
                                  FsPermission.createImmutable(mod), "owen", "users", path);
        }
    }

    private final static String txnMgr = "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager";

    public static HiveConf getHiveConf() {
        HiveConf conf = new HiveConf();
        // String metastoreDBLocation = "jdbc:derby:databaseName=/tmp/metastore_db;create=true";
        // conf.set("javax.jdo.option.ConnectionDriverName","org.apache.derby.jdbc.EmbeddedDriver");
        // conf.set("javax.jdo.option.ConnectionURL",metastoreDBLocation);
        conf.set("fs.raw.impl", RawFileSystem.class.getName());
        conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, txnMgr);
        conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
        conf.setVar(HiveConf.ConfVars.HIVEFETCHTASKCONVERSION, "none");
        conf.set("datanucleus.schema.autoCreateAll", "true");
        conf.set("hive.metastore.schema.verification", "false");
        return conf;
    }

    public static void createDbAndTable(Driver driver, HiveConf conf, String databaseName,
                                        String tableName, List<String> partVals,
                                        String[] colNames, String[] colTypes,
                                        String[] partNames, String dbLocation)
        throws Exception {
        String dbUri = "raw://" + new Path(dbLocation).toUri().toString();
        String tableLoc = dbUri + Path.SEPARATOR + tableName;

        boolean success = runDDL(driver, "create database IF NOT EXISTS " + databaseName + " location '" + dbUri + "'");
        runDDL(driver, "use " + databaseName);
        String crtTbl = "create table " + tableName +
            " ( " +  getTableColumnsStr(colNames,colTypes) + " )" +
            getPartitionStmtStr(partNames) +
            " clustered by ( " + colNames[0] + " )" +
            " into 10 buckets " +
            " stored as orc " +
            " location '" + tableLoc +  "'" +
            " TBLPROPERTIES ('transactional'='true')";
        runDDL(driver, crtTbl);

        if(partNames!=null && partNames.length!=0 && partVals != null) {
            String addPart = "alter table " + tableName + " add partition ( " +
                getTablePartsStr2(partNames, partVals) + " )";
            runDDL(driver, addPart);
        }
    }

    private static String getPartitionStmtStr(String[] partNames) {
        if ( partNames == null || partNames.length == 0) {
            return "";
        }
        return " partitioned by (" + getTablePartsStr(partNames) + " )";
    }

    private static String getTableColumnsStr(String[] colNames, String[] colTypes) {
        StringBuffer sb = new StringBuffer();
        for (int i=0; i < colNames.length; ++i) {
            sb.append(colNames[i] + " " + colTypes[i]);
            if (i<colNames.length-1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    // converts partNames into "partName1 string, partName2 string"
    private static String getTablePartsStr(String[] partNames) {
        if (partNames==null || partNames.length==0) {
            return "";
        }
        StringBuffer sb = new StringBuffer();
        for (int i=0; i < partNames.length; ++i) {
            sb.append(partNames[i] + " string");
            if (i < partNames.length-1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    // converts partNames,partVals into "partName1=val1, partName2=val2"
    private static String getTablePartsStr2(String[] partNames, List<String> partVals) {
        StringBuffer sb = new StringBuffer();
        for (int i=0; i < partVals.size(); ++i) {
            sb.append(partNames[i] + " = '" + partVals.get(i) + "'");
            if(i < partVals.size()-1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    private static boolean runDDL(Driver driver, String sql) throws QueryFailedException {
        int retryCount = 1; // # of times to retry if first attempt fails
        for (int attempt=0; attempt <= retryCount; ++attempt) {
                driver.run(sql);
                return true;
        } // for
        return false;
    }

    // delete db and all tables in it
    public static void dropDB(HiveConf conf, String databaseName) throws HiveException, MetaException {
        IMetaStoreClient client = new HiveMetaStoreClient(conf);
        try {
            for (String table : client.listTableNamesByFilter(databaseName, "", (short) -1)) {
                client.dropTable(databaseName, table, true, true);
            }
            client.dropDatabase(databaseName);
        } catch (TException e) {
            client.close();
        }
    }

    private static String makePartPath(List<FieldSchema> partKeys, List<String> partVals) {
        if(partKeys.size()!=partVals.size()) {
            throw new IllegalArgumentException("Partition values:" + partVals +
                                               ", does not match the partition Keys in table :" + partKeys );
        }
        StringBuffer buff = new StringBuffer(partKeys.size()*20);
        int i=0;
        for(FieldSchema schema : partKeys) {
            buff.append(schema.getName());
            buff.append("=");
            buff.append(partVals.get(i));
            if(i!=partKeys.size()-1) {
                buff.append(Path.SEPARATOR);
            }
            ++i;
        }
        return buff.toString();
    }


}

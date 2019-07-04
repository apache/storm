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

package org.apache.storm.hive.security;

import static org.apache.storm.hive.security.HiveSecurityUtil.HIVE_KEYTAB_FILE_KEY;
import static org.apache.storm.hive.security.HiveSecurityUtil.HIVE_PRINCIPAL_KEY;

import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command tool of Hive credential renewer.
 */
public final class AutoHiveCommand {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHiveCommand.class);

    private AutoHiveCommand() {
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put(HIVE_PRINCIPAL_KEY, args[1]); // hive principal storm-hive@WITZEN.COM
        conf.put(HIVE_KEYTAB_FILE_KEY, args[2]); // storm hive keytab /etc/security/keytabs/storm-hive.keytab
        // hive.metastore.uris : "thrift://pm-eng1-cluster1.field.hortonworks.com:9083"
        conf.put(HiveConf.ConfVars.METASTOREURIS.varname, args[3]);

        AutoHive autoHive = new AutoHive();
        autoHive.prepare(conf);
        AutoHiveNimbus autoHiveNimbus = new AutoHiveNimbus();
        autoHiveNimbus.prepare(conf);

        Map<String, String> creds = new HashMap<>();
        autoHiveNimbus.populateCredentials(creds, conf, args[0]);
        LOG.info("Got Hive credentials" + autoHive.getCredentials(creds));

        Subject subject = new Subject();
        autoHive.populateSubject(subject, creds);
        LOG.info("Got a Subject " + subject);

        autoHiveNimbus.renew(creds, conf, args[0]);
        LOG.info("Renewed credentials" + autoHive.getCredentials(creds));
    }

}

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

package org.apache.storm.hbase.security;

import static org.apache.storm.hbase.security.HBaseSecurityUtil.HBASE_KEYTAB_FILE_KEY;
import static org.apache.storm.hbase.security.HBaseSecurityUtil.HBASE_PRINCIPAL_KEY;

import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;

import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command tool of Hive credential renewer.
 */
public final class AutoHBaseCommand {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHBaseCommand.class);

    private AutoHBaseCommand() {
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map<String, Object> conf = new HashMap<>();
        conf.put(HBASE_PRINCIPAL_KEY, args[1]); // hbase principal storm-hbase@WITZEN.COM
        conf.put(HBASE_KEYTAB_FILE_KEY,
            args[2]); // storm hbase keytab /etc/security/keytabs/storm-hbase.keytab

        AutoHBase autoHBase = new AutoHBase();
        autoHBase.prepare(conf);
        AutoHBaseNimbus autoHBaseNimbus = new AutoHBaseNimbus();
        autoHBaseNimbus.prepare(conf);

        Map<String, String> creds = new HashMap<>();
        autoHBaseNimbus.populateCredentials(creds, conf, args[0]); //with realm e.g. storm@WITZEND.COM
        LOG.info("Got HBase credentials" + autoHBase.getCredentials(creds));

        Subject s = new Subject();
        autoHBase.populateSubject(s, creds);
        LOG.info("Got a Subject " + s);

        autoHBaseNimbus.renew(creds, conf, args[0]);
        LOG.info("renewed credentials" + autoHBase.getCredentials(creds));
    }
}

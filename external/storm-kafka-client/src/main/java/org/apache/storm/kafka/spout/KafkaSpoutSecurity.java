/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.storm.kafka.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.security.auth.login.Configuration;

public class KafkaSpoutSecurity {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutSecurity.class);
    private static final String KERBEROS_AUTH_JAASCONF = "java.security.auth.login.config";

    public synchronized static void PrepareKerberos(String pathToJaas) {
        if(pathToJaas == null || pathToJaas.isEmpty()){
            LOG.info("No jaas.conf found.Can't use storm-kakfa-client in kerberos mode.");
        }else{
            Configuration login_conf = Configuration.getConfiguration();
            System.setProperty(KERBEROS_AUTH_JAASCONF, pathToJaas);
            login_conf.refresh();
            LOG.info("Use storm-kakfa-client in kerberos mode.");
    }
    }
}


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
package org.apache.storm.hdfs.security;

import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.hdfs.security.HdfsSecurityUtil.STORM_KEYTAB_FILE_KEY;
import static org.apache.storm.hdfs.security.HdfsSecurityUtil.STORM_USER_NAME_KEY;

/**
 * Command tool of HDFS credential renewer
 */
public final class AutoHDFSCommand {
  private static final Logger LOG = LoggerFactory.getLogger(AutoHDFSCommand.class);

  private AutoHDFSCommand() {
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    Map conf = new HashMap();
    conf.put(STORM_USER_NAME_KEY, args[1]); //with realm e.g. hdfs@WITZEND.COM
    conf.put(STORM_KEYTAB_FILE_KEY, args[2]);// /etc/security/keytabs/storm.keytab

    AutoHDFS autoHDFS = new AutoHDFS();
    autoHDFS.prepare(conf);
    AutoHDFSNimbus autoHDFSNimbus = new AutoHDFSNimbus();
    autoHDFSNimbus.prepare(conf);

    Map<String,String> creds  = new HashMap<>();
    autoHDFSNimbus.populateCredentials(creds, conf, args[0]);
    LOG.info("Got HDFS credentials", autoHDFS.getCredentials(creds));

    Subject s = new Subject();
    autoHDFS.populateSubject(s, creds);
    LOG.info("Got a Subject "+ s);

    autoHDFSNimbus.renew(creds, conf, args[0]);
    LOG.info("renewed credentials", autoHDFS.getCredentials(creds));
  }

}

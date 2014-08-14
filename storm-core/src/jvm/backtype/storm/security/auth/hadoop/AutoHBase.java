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

package backtype.storm.security.auth.hadoop;

import backtype.storm.Config;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Automatically get hbase delegation tokens and push it to user's topology. The class
 * assumes that hadoop/hbase configuration files are in your class path.
 */
public class AutoHBase extends AbstractAutoHadoopPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(AutoHBase.class);

    public static final String HBASE_CREDENTIALS = "HBASE_CREDENTIALS";

    @SuppressWarnings("unchecked")
    @Override
    protected byte[] getHadoopCredentials(Map conf) {

        try {
            // What we want to do is following:
            //  if(UserGroupInformation.isSecurityEnabled) {
            //      Configuration hbaseConf = HBaseConfiguration.create();
            //      UserGroupInformation.setConfiguration(hbaseConf);
            //      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            //      UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologySubmitterUser, ugi);
            //
            //      User u = User.create(ugi);
            //      if(u.isHBaseSecurityEnabled()) {
            //             TokenUtil.obtainAndCacheToken(hbaseConf, proxyUser);
            //      }
            // }
            // and then return the credential object from the proxyUser.getCredentials() as a bytearray.


            //Configuration hbaseConf = HBaseConfiguration.create();
            Class configurationClass = Class.forName("org.apache.hadoop.hbase.HBaseConfiguration");

            Method createConfigMethod = configurationClass.getMethod("create");
            Object hbaseConf = createConfigMethod.invoke(null);

            //UserGroupInformation.isSecurityEnabled
            final Class ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");
            final Method isSecurityEnabledMethod = ugiClass.getDeclaredMethod("isSecurityEnabled");
            boolean isSecurityEnabled = (Boolean)isSecurityEnabledMethod.invoke(null);

            if(isSecurityEnabled) {
                final String topologySubmitterUser = (String) conf.get(Config.TOPOLOGY_SUBMITTER_PRINCIPAL);
                //UserGroupInformation.setConfiguration(hbaseConf);
                Class hadoopConfigClass = Class.forName("org.apache.hadoop.conf.Configuration");
                Method setConfigMethod = ugiClass.getMethod("setConfiguration", hadoopConfigClass);
                setConfigMethod.invoke(null,hbaseConf);

                //UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                Method getCurrentUserMethod = ugiClass.getMethod("getCurrentUser");
                final Object ugi = getCurrentUserMethod.invoke(null);

                //UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(topologySubmitterUser, ugi);
                Method createProxyUserMethod = ugiClass.getMethod("createProxyUser", String.class, ugiClass);
                Object proxyUGI = createProxyUserMethod.invoke(null, topologySubmitterUser, ugi);

                //User user = User.create(ugi);
                Class userClass = Class.forName("org.apache.hadoop.hbase.security.User");
                Method createMethod = userClass.getMethod("create", ugiClass);
                Object user = createMethod.invoke(null, proxyUGI);

                //user.isHBaseSecurityEnabled()
                Method isHBaseSecurityEnabledMethod = userClass.getMethod("isHBaseSecurityEnabled", hadoopConfigClass);
                Boolean isHbaseSecurityEnabled = (Boolean) isHBaseSecurityEnabledMethod.invoke(user, hbaseConf);
                if(isHbaseSecurityEnabled) {
                    //TokenUtil.obtainAndCacheToken(hbaseConf, proxyUser);
                    Class tokenUtilClass = Class.forName("org.apache.hadoop.hbase.security.token.TokenUtil");

                    Method obtainAndCacheTokenMethod = tokenUtilClass.getMethod("obtainAndCacheToken",
                            hadoopConfigClass,
                            ugiClass);
                    obtainAndCacheTokenMethod.invoke(null, hbaseConf, proxyUGI);

                    //Credentials credential= proxyUser.getCredentials();
                    Method getCredentialsMethod = ugiClass.getMethod("getCredentials");
                    Object credentials = getCredentialsMethod.invoke(proxyUGI);

                    Class credentialClass = Class.forName("org.apache.hadoop.security.Credentials");

                    ByteArrayOutputStream bao = new ByteArrayOutputStream();
                    ObjectOutputStream out = new ObjectOutputStream(bao);
                    Method writeMethod = credentialClass.getMethod("write", DataOutput.class);
                    writeMethod.invoke(credentials, out);
                    out.flush();
                    out.close();

                    return bao.toByteArray();
                } else {
                    throw new RuntimeException("Security is not enabled for HBase.");
                }
            } else {
                throw new RuntimeException("Security is not enabled for Hadoop");
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get delegation tokens." , ex);
        }
    }

    @Override
    public void renew(Map<String, String> credentials, Map topologyConf) {
        //HBASE tokens are not renewable so we always have to get new ones.
        populateCredentials(credentials, topologyConf);
    }

    @Override
    public void renew(Map<String, String> credentials, Map topologyConf) {
        //HBASE tokens are not renewable so we always have to get new ones.
        populateCredentials(credentials, topologyConf);
    }

    @Override
    protected String getCredentialKey() {
        return HBASE_CREDENTIALS;
    }


    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_SUBMITTER_PRINCIPAL, args[0]); //with realm e.g. storm@WITZEND.COM

        AutoHBase autoHBase = new AutoHBase();
        autoHBase.prepare(conf);

        Map<String,String> creds  = new HashMap<String, String>();
        autoHBase.populateCredentials(creds, conf);
        LOG.info("Got HBase credentials" + autoHBase.getCredentials(creds));

        Subject s = new Subject();
        autoHBase.populateSubject(s, creds);
        LOG.info("Got a Subject " + s);

        autoHBase.renew(creds, conf);
        LOG.info("renewed credentials" + autoHBase.getCredentials(creds));
    }
}


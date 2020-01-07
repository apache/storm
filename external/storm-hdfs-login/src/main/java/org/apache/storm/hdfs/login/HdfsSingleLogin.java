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
 */

package org.apache.storm.hdfs.login;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.storm.Config;
import org.apache.storm.utils.IHdfsLoginPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hadoop {@link UserGroupInformation} has some limitations/bugs (at least in hadoop-2.x).
 * We should only use {@link UserGroupInformation#loginUserFromKeytab(String, String)} to login to HDFS
 * at most once in a JVM process because this method changes the static fields of {@link UserGroupInformation},
 * which could introduce nasty bugs if it's called multiple times.
 * {@link UserGroupInformation#loginUserFromKeytabAndReturnUGI(String, String)} also has bugs so
 * it shouldn't be used.
 * <br>
 * Don't use this class directly. Use {@link org.apache.storm.utils.HdfsLoginUtil} instead.
 * The combination of this class and {@link org.apache.storm.utils.HdfsLoginUtil}
 * guarantees single login in a JVM process.
 */
public class HdfsSingleLogin implements IHdfsLoginPlugin {
    private Subject loggedInSubject;
    private static final Logger LOG = LoggerFactory.getLogger(HdfsSingleLogin.class);

    /**
     * Login to hdfs using principal and keytab at most once.
     * @param conf the storm configuration.
     * @return the subject.
     */
    @Override
    public Subject login(Map<String, Object> conf) {
        if (loggedInSubject != null) {
            LOG.debug("Already logged in to hdfs as {}. Will not re-login or login as another user.",
                loggedInSubject.getPrincipals());
            return loggedInSubject;
        }

        try {
            String principal = Config.getHdfsPrincipal(conf);
            String keyTab = Config.getHdfsKeytab(conf);

            if (principal != null && keyTab != null) {
                synchronized (this) {
                    if (loggedInSubject == null) {
                        UserGroupInformation.loginUserFromKeytab(principal, keyTab);
                        loggedInSubject = getHdfsUser();
                        LOG.info("Successfully logged in to hdfs as {}", loggedInSubject.getPrincipals());
                    } else {
                        LOG.debug("Already logged in to hdfs as {}. Will not re-login or login as another user.",
                            loggedInSubject.getPrincipals());
                    }
                }
            } else {
                throw new IllegalArgumentException("You must specify both HDFS principal and keytab!");
            }
        } catch (IOException e) {
            throw new RuntimeException("Error logging in from keytab: " + e.getMessage(), e);
        }
        return loggedInSubject;
    }

    /**
     * Get the subject from Hadoop. There is no direct interface from UserGroupInformation
     * to get the subject, so do a doAs and get the context.
     */
    private Subject getHdfsUser() {
        Subject subj;
        try {
            subj = UserGroupInformation.getCurrentUser().doAs(
                new PrivilegedAction<Subject>() {
                    @Override
                    public Subject run() {
                        return Subject.getSubject(AccessController.getContext());
                    }
                });
        } catch (IOException e) {
            throw new RuntimeException("Error creating subject and logging user in!", e);
        }
        return subj;
    }
}

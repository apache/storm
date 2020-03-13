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

package org.apache.storm.utils;

import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.storm.Config;
import org.apache.storm.shade.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UserGroupInformation#loginUserFromKeytab(String, String) changes the static fields of UserGroupInformation,
 * especially the current logged-in user, and UserGroupInformation itself is not thread-safe.
 * So it could introduce bugs if it is called multiple times in a JVM process.
 * HadoopLoginUtil.loginHadoop guarantees at-most-once login in a JVM process.
 * This should only be used on the daemon side.
 */
public class HadoopLoginUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopLoginUtil.class);

    private static Subject loginSubject = null;
    private static boolean firstTimeLogin = true;

    /**
     * Login if a HDFS keytab/principal have been supplied;
     * otherwise, assume it's already logged in or running on insecure HDFS.
     * This also guarantees that login only happens at most once.
     * @param conf the daemon conf
     * @return the logged in subject or null
     */
    public static Subject loginHadoop(Map<String, Object> conf) {
        if (firstTimeLogin) {
            synchronized (HadoopLoginUtil.class) {
                if (firstTimeLogin) {
                    String principal;
                    try {
                        principal = Config.getHdfsPrincipal(conf);
                    } catch (UnknownHostException e) {
                        throw new IllegalArgumentException("Failed to get principal", e);
                    }
                    String keyTab = Config.getHdfsKeytab(conf);
                    if (principal != null && keyTab != null) {
                        loginFromKeytab(principal, keyTab);
                    } else {
                        if (principal == null && keyTab != null) {
                            throw new IllegalArgumentException("HDFS principal is null while keytab is present");
                        } else {
                            if (principal != null && keyTab == null) {
                                throw new IllegalArgumentException("HDFS keytab is null while principal is present");
                            }
                        }
                    }

                    loginSubject = getHadoopUser();
                    firstTimeLogin = false;
                } else {
                    LOG.debug("Already logged in to Hadoop");
                }
            }
        } else {
            LOG.debug("Already logged in to Hadoop");
        }
        LOG.debug("The subject is: {}", loginSubject);
        return loginSubject;
    }

    //The Hadoop UserGroupInformation class name
    private static final String HADOOP_USER_GROUP_INFORMATION_CLASS = "org.apache.hadoop.security.UserGroupInformation";

    private static void loginFromKeytab(String principal, String keyTab) {
        Preconditions.checkNotNull(principal);
        Preconditions.checkNotNull(keyTab);

        /* The following code is essentially:
         UserGroupInformation.loginUserFromKeytab(principal, keyTab);
         */

        Class<?> ugi;
        try {
            ugi = Class.forName(HADOOP_USER_GROUP_INFORMATION_CLASS);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Hadoop is not in the classpath", e);
        }
        try {
            Method login = ugi.getMethod("loginUserFromKeytab", String.class, String.class);
            login.invoke(null, principal, keyTab);
        } catch (Exception e) {
            throw new RuntimeException("Failed to login hadoop user from Keytab!", e);
        }

        LOG.info("Successfully login to Hadoop using keytab as {}", principal);
    }

    /**
     * Get the subject from Hadoop. There is no direct interface from UserGroupInformation
     * to get the subject, so do a doAs and get the context.
     */
    private static Subject getHadoopUser() {
        /* The following code is essentially:
        Subject sub = UserGroupInformation.getCurrentUser().doAs(
            (PrivilegedAction<Subject>) () -> Subject.getSubject(AccessController.getContext()));
        */

        Class<?> ugiClass;
        try {
            ugiClass = Class.forName(HADOOP_USER_GROUP_INFORMATION_CLASS);
        } catch (ClassNotFoundException e) {
            LOG.info("Hadoop was not found on the class path", e);
            return null;
        }

        try {
            Method currentUserMethod = ugiClass.getMethod("getCurrentUser");
            Method doAsMethod = ugiClass.getMethod("doAs", PrivilegedAction.class);
            Object ugi = currentUserMethod.invoke(null);
            return (Subject) doAsMethod.invoke(ugi,
                (PrivilegedAction<Subject>) () -> Subject.getSubject(AccessController.getContext()));
        } catch (Exception e) {
            throw new RuntimeException("Error getting hadoop user!", e);
        }
    }
}


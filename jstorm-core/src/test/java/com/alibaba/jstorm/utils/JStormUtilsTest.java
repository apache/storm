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
package com.alibaba.jstorm.utils;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JStormUtilsTest {
    private static final Logger LOG = LoggerFactory
            .getLogger(JStormUtilsTest.class);

    private static final int RESTART_TIMES = 1000;

    private static String cmd = "";

    private static String pidDir = "/tmp/pids";

    private static int killedTimes = 0;
    private static int forkTimes = 0;

    public static void testRestartProcess() {
        final int intervalSec = Integer.valueOf("10");
        new Thread(new Runnable() {

            @Override
            public void run() {
                LOG.info("Begin to start fork thread");
                Map<String, String> environment = new HashMap<String, String>();

                // TODO Auto-generated method stub
                while (forkTimes < RESTART_TIMES) {

                    try {
                        JStormUtils.launch_process(cmd + " " + forkTimes,
                                environment, true);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        LOG.error("Failed to fork process " + cmd + forkTimes,
                                e);
                        continue;
                    }

                    LOG.info("Successfully launch " + forkTimes);

                    JStormUtils.sleepMs(1000);

                    forkTimes++;

                }
                LOG.info("Successfully shutdown fok thread");
            }

        }).start();

        new Thread(new Runnable() {

            @Override
            public void run() {
                LOG.info("Begin to start killing thread");

                File file = new File(pidDir);
                // TODO Auto-generated method stub
                while (killedTimes < RESTART_TIMES) {
                    File[] pids = file.listFiles();
                    if (pids == null) {
                        JStormUtils.sleepMs(100);
                        continue;
                    }

                    for (File pidFile : pids) {
                        String pid = pidFile.getName();

                        JStormUtils.ensure_process_killed(Integer.valueOf(pid));

                        killedTimes++;

                        pidFile.delete();
                    }

                    JStormUtils.sleepMs(100);

                }
                LOG.info("Successfully shutdown killing thread");
            }

        }).start();

        while (killedTimes < RESTART_TIMES) {
            JStormUtils.sleepMs(100);
        }
    }

    public static void fillData() {
        Map<Long, String> map = new HashMap<Long, String>();

        for (long l = 0; l < 1000000; l++) {
            map.put(l, String.valueOf(l));
        }
    }

    public static void testJar(String id) {
        try {

            PathUtils.local_mkdirs(pidDir);
        } catch (IOException e) {
            LOG.error("Failed to rmr " + pidDir, e);
        }

        fillData();
        LOG.info("Finish load data");

        String pid = JStormUtils.process_pid();

        String pidFile = pidDir + File.separator + pid;

        try {
            PathUtils.touch(pidFile);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error("Failed to touch " + pidFile, e);
        }
        try {

            DataOutputStream raf =
                    new DataOutputStream(new BufferedOutputStream(
                            new FileOutputStream(new File(pidFile), true)));

            raf.writeBytes(pid);
        } catch (Exception e) {
            LOG.error("", e);
        }

        while (true) {
            JStormUtils.sleepMs(1000);
            LOG.info(id + " is living");
        }

    }

    public static void main(String[] args) {
        if (args.length == 0) {
            testRestartProcess();
        } else {
            testJar(args[0]);
        }
    }

}

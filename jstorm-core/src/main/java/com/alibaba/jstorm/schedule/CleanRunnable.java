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
package com.alibaba.jstorm.schedule;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.OlderFileFilter;

/**
 * clean /nimbus/inbox jar every 600 seconds
 * 
 * Default expire time is 3600 seconds
 * 
 * @author lixin
 * 
 */
public class CleanRunnable implements Runnable {

    private static Logger log = LoggerFactory.getLogger(CleanRunnable.class);

    private String dir_location;

    private int seconds;

    public CleanRunnable(String dir_location, int inbox_jar_expiration_secs) {
        this.dir_location = dir_location;
        this.seconds = inbox_jar_expiration_secs;
    }

    @Override
    public void run() {
        File inboxdir = new File(dir_location);
        clean(inboxdir);
    }

    private void clean(File file) {
        // filter
        OlderFileFilter filter = new OlderFileFilter(seconds);

        File[] files = file.listFiles(filter);
        for (File f : files) {
            if (f.isFile()) {
                log.info("Cleaning inbox ... deleted: " + f.getName());
                try {
                    f.delete();
                } catch (Exception e) {
                    log.error("Cleaning inbox ... error deleting:"
                            + f.getName() + "," + e);
                }
            } else {
                clean(f);
                if (f.listFiles().length == 0) {
                    log.info("Cleaning inbox ... deleted: " + f.getName());
                    try {
                        f.delete();
                    } catch (Exception e) {
                        log.error("Cleaning inbox ... error deleting:"
                                + f.getName() + "," + e);
                    }
                }
            }
        }
    }

}

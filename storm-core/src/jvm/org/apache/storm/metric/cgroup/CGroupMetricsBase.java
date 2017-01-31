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
package org.apache.storm.metric.cgroup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for checking if CGroups are enabled, etc.
 */
public abstract class CGroupMetricsBase<T> implements IMetric {
    private static final Logger LOG = LoggerFactory.getLogger(CGroupMetricsBase.class);
    private boolean enabled;
    private File fullFile;
    
    public CGroupMetricsBase(Map<String, Object> conf, String fileName) {
        enabled = false;
        String hierarchyDir = (String)conf.get(Config.STORM_CGROUP_HIERARCHY_DIR);
        if (hierarchyDir == null || hierarchyDir.isEmpty()) {
            LOG.warn("{} is disabled {} is not set", getClass().getSimpleName(), Config.STORM_CGROUP_HIERARCHY_DIR);
            return;
        }
        //Good so far, check if we are in a CGroup
        File cgroupFile = new File("/proc/self/cgroup");
        if (!cgroupFile.exists()) {
            LOG.warn("{} is disabled we do not appear to be a part of a CGroup", getClass().getSimpleName());
            return;
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(cgroupFile))) {
            //There can be more then one line if cgroups are mounted in more then one place, but we assume the first is good enough
            String line = reader.readLine();
            //hierarchy-ID:controller-list:cgroup-path
            String[] parts = line.split(":");
            //parts[0] == 0 for CGroup V2, else maps to hierarchy in /proc/cgroups
            //parts[1] is empty for CGroups V2 else what is mapped that we are looking for
            String cgroupPath = parts[2];
            
            fullFile = new File(new File(new File(hierarchyDir), cgroupPath), fileName);
            if (!fullFile.exists()) {
                LOG.warn("{} is disabled {} does not exist", getClass().getSimpleName(), fullFile);
                return;
            }
        } catch (Exception e) {
            LOG.warn("{} is disabled error trying to read or parse {}", getClass().getSimpleName(), cgroupFile);
            return;
        }
        enabled = true;
        LOG.info("{} is ENABLED {} exists...", getClass().getSimpleName(), fullFile);
    }
    
    @Override
    public Object getValueAndReset() {
        if (!enabled) {
            return null;
        }
        StringBuffer contents = new StringBuffer();
        try (BufferedReader reader = new BufferedReader(new FileReader(fullFile))) {
            char[] buf = new char[4096];
            int len;
            while((len = reader.read(buf)) > 0) {
                contents.append(buf, 0, len);
            }
            Object ret = parseFileContents(contents.toString());
            LOG.debug("{} is returning {} from {}", getClass().getSimpleName(), ret, fullFile);
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public abstract T parseFileContents(String contents);
}

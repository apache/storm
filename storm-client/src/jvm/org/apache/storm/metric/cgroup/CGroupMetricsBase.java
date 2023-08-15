/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.metric.cgroup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.container.cgroup.CgroupCenter;
import org.apache.storm.container.cgroup.CgroupCoreFactory;
import org.apache.storm.container.cgroup.SubSystemType;
import org.apache.storm.container.cgroup.core.CgroupCore;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for checking if CGroups are enabled, etc.
 */
@Deprecated
public abstract class CGroupMetricsBase<T> implements IMetric {
    private static final Logger LOG = LoggerFactory.getLogger(CGroupMetricsBase.class);
    private boolean enabled;
    private CgroupCore core = null;

    public CGroupMetricsBase(Map<String, Object> conf, SubSystemType type) {
        final String simpleName = getClass().getSimpleName();
        enabled = false;
        CgroupCenter center = CgroupCenter.getInstance();
        if (center == null) {
            LOG.warn("{} is disabled. cgroups do not appear to be enabled on this system", simpleName);
            return;
        }
        if (!center.isSubSystemEnabled(type)) {
            LOG.warn("{} is disabled. {} is not an enabled subsystem", simpleName, type);
            return;
        }

        //Check to see if the CGroup is mounted at all
        if (null == center.getHierarchyWithSubSystem(type)) {
            LOG.warn("{} is disabled. {} is not a mounted subsystem", simpleName, type);
            return;
        }

        //Good so far, check if we are in a CGroup
        File cgroupFile = new File("/proc/self/cgroup");
        if (!cgroupFile.exists()) {
            LOG.warn("{} is disabled we do not appear to be a part of a CGroup", getClass().getSimpleName());
            return;
        }

        String cgroupPath;
        try (BufferedReader reader = new BufferedReader(new FileReader(cgroupFile))) {
            //There can be more then one line if cgroups are mounted in more then one place, but we assume the first is good enough
            String line = reader.readLine();
            //hierarchy-ID:controller-list:cgroup-path
            String[] parts = line.split(":");
            //parts[0] == 0 for CGroup V2, else maps to hierarchy in /proc/cgroups
            //parts[1] is empty for CGroups V2 else what is mapped that we are looking for
            cgroupPath = parts[2];
        } catch (Exception e) {
            LOG.warn("{} is disabled error trying to read or parse {}", simpleName, cgroupFile);
            return;
        }

        //Storm on Rhel6 and Rhel7 use different cgroup settings.
        //On Rhel6, the cgroup of the worker is under
        // "Config.STORM_CGROUP_HIERARCHY_DIR/DaemonConfig.STORM_SUPERVISOR_CGROUP_ROOTDIR/<worker-id>"
        //On Rhel7, the cgroup of the worker is under
        // "Config.STORM_OCI_CGROUP_ROOT/<subsystem>/DaemonConfig.STORM_OCI_CGROUP_PARENT/<container-id>"
        // This block of code is a workaround for the CGroupMetrics to work on both system
        String hierarchyDir = (String) conf.get(Config.STORM_CGROUP_HIERARCHY_DIR);
        if (StringUtils.isEmpty(hierarchyDir) || !new File(hierarchyDir, cgroupPath).exists()) {
            LOG.info("{} is not set or does not exist. checking {}", Config.STORM_CGROUP_HIERARCHY_DIR,
                Config.STORM_OCI_CGROUP_ROOT);

            String ociCgroupRoot = (String) conf.get(Config.STORM_OCI_CGROUP_ROOT);
            hierarchyDir = ociCgroupRoot + File.separator + type;
            if (StringUtils.isEmpty(ociCgroupRoot) || !new File(hierarchyDir, cgroupPath).exists()) {
                LOG.info("{} is not set or does not exist", Config.STORM_OCI_CGROUP_ROOT);
                LOG.warn("{} is disabled", simpleName);
                return;
            }
        }

        core = CgroupCoreFactory.getInstance(type, new File(hierarchyDir, cgroupPath).getAbsolutePath());

        enabled = true;
        LOG.info("{} is ENABLED {} exists...", simpleName, hierarchyDir);
    }

    @Override
    public Object getValueAndReset() {
        if (!enabled) {
            return null;
        }
        try {
            return getDataFrom(core);
        } catch (FileNotFoundException e) {
            LOG.warn("Exception trying to read a file {}", e);
            //Something happened and we couldn't find the file, so ignore it for now.
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public abstract T getDataFrom(CgroupCore core) throws Exception;
}
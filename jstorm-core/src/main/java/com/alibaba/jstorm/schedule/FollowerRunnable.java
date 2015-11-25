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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.Cluster;
import com.alibaba.jstorm.cluster.StormClusterState;
import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.daemon.nimbus.NimbusData;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.alibaba.jstorm.utils.PathUtils;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class FollowerRunnable implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(FollowerRunnable.class);

    private NimbusData data;

    private int sleepTime;

    private volatile boolean state = true;

    private RunnableCallback callback;

    private final String hostPort;

    public static final String NIMBUS_DIFFER_COUNT_ZK = "nimbus.differ.count.zk";

    public static final Integer SLAVE_NIMBUS_WAIT_TIME = 120;

    @SuppressWarnings("unchecked")
    public FollowerRunnable(final NimbusData data, int sleepTime) {
        this.data = data;
        this.sleepTime = sleepTime;

        if (!ConfigExtension.isNimbusUseIp(data.getConf())) {
            this.hostPort = NetWorkUtils.hostname() + ":" + String.valueOf(Utils.getInt(data.getConf().get(Config.NIMBUS_THRIFT_PORT)));
        } else {
            this.hostPort = NetWorkUtils.ip() + ":" + String.valueOf(Utils.getInt(data.getConf().get(Config.NIMBUS_THRIFT_PORT)));
        }
        try {

            String[] hostfigs = this.hostPort.split(":");
            boolean isLocaliP = false;
            if (hostfigs.length > 0) {
                isLocaliP = hostfigs[0].equals("127.0.0.1");
            }
            if (isLocaliP) {
                throw new Exception("the hostname which Nimbus get is localhost");
            }
        } catch (Exception e1) {
            LOG.error("get nimbus host error!", e1);
            throw new RuntimeException(e1);
        }

        try {
            data.getStormClusterState().update_nimbus_slave(hostPort, data.uptime());
            data.getStormClusterState().update_nimbus_detail(hostPort, null);
        } catch (Exception e) {
            LOG.error("register nimbus host fail!", e);
            throw new RuntimeException();
        }
        try{
            update_nimbus_detail();
        }catch (Exception e){
            LOG.error("register detail of nimbus fail!", e);
            throw new RuntimeException();
        }
        try {
            this.tryToBeLeader(data.getConf());
        } catch (Exception e1) {
            try {
                data.getStormClusterState().unregister_nimbus_host(hostPort);
                data.getStormClusterState().unregister_nimbus_detail(hostPort);
            }catch (Exception e2){
                LOG.info("due to task errors, so remove register nimbus infomation" );
            }finally {
                // TODO Auto-generated catch block
                LOG.error("try to be leader error.", e1);
                throw new RuntimeException(e1);
            }
        }
        callback = new RunnableCallback() {
            @Override
            public void run() {
                if (!data.isLeader())
                    check();
            }
        };
    }

    public boolean isLeader(String zkMaster) {
        if (StringUtils.isBlank(zkMaster) == true) {
            return false;
        }

        if (hostPort.equalsIgnoreCase(zkMaster) == true) {
            return true;
        }

        // Two nimbus running on the same node isn't allowed
        // so just checks ip is enough here
        String[] part = zkMaster.split(":");
        return NetWorkUtils.equals(part[0], NetWorkUtils.ip());
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        LOG.info("Follower Thread starts!");
        while (state) {
            StormClusterState zkClusterState = data.getStormClusterState();
            try {
                Thread.sleep(sleepTime);
                if (!zkClusterState.leader_existed()) {
                    this.tryToBeLeader(data.getConf());
                    continue;
                }

                String master = zkClusterState.get_leader_host();
                boolean isZkLeader = isLeader(master);
                if (data.isLeader() == true) {
                    if (isZkLeader == false) {
                        LOG.info("New ZK master is " + master);
                        JStormUtils.halt_process(1, "Lose ZK master node, halt process");
                        return;
                    }
                }

                if (isZkLeader == true) {
                    zkClusterState.unregister_nimbus_host(hostPort);
                    zkClusterState.unregister_nimbus_detail(hostPort);
                    data.setLeader(true);
                    continue;
                }

                check();
                zkClusterState.update_nimbus_slave(hostPort, data.uptime());
                update_nimbus_detail();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                continue;
            } catch (Exception e) {
                if (state) {
                    LOG.error("Unknow exception ", e);
                }
            }
        }
        LOG.info("Follower Thread has closed!");
    }

    public void clean() {
        state = false;
    }

    private synchronized void check() {

        StormClusterState clusterState = data.getStormClusterState();

        try {
            String master_stormdist_root = StormConfig.masterStormdistRoot(data.getConf());

            List<String> code_ids = PathUtils.read_dir_contents(master_stormdist_root);

            List<String> assignments_ids = clusterState.assignments(callback);

            Map<String, Assignment> assignmentMap = new HashMap<String, Assignment>();
            List<String> update_ids = new ArrayList<String>();
            for (String id : assignments_ids) {
                Assignment assignment = clusterState.assignment_info(id, null);
                Long localCodeDownTS;
                try {
                    Long tmp = StormConfig.read_nimbus_topology_timestamp(data.getConf(), id);
                    localCodeDownTS = (tmp == null ? 0L : tmp);
                } catch (FileNotFoundException e) {
                    localCodeDownTS = 0L;
                }
                if (assignment != null && assignment.isTopologyChange(localCodeDownTS.longValue())) {
                    update_ids.add(id);
                }
                assignmentMap.put(id, assignment);
            }

            List<String> done_ids = new ArrayList<String>();

            for (String id : code_ids) {
                if (assignments_ids.contains(id)) {
                    done_ids.add(id);
                }
            }

            for (String id : done_ids) {
                assignments_ids.remove(id);
                code_ids.remove(id);
            }

            //redownload  topologyid which hava been updated;
            assignments_ids.addAll(update_ids);

            for (String topologyId : code_ids) {
                deleteLocalTopology(topologyId);
            }

            for (String id : assignments_ids) {
                downloadCodeFromMaster(assignmentMap.get(id), id);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error("Get stormdist dir error!", e);
            return;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            LOG.error("Check error!", e);
            return;
        }
    }

    private void deleteLocalTopology(String topologyId) throws IOException {
        String dir_to_delete = StormConfig.masterStormdistRoot(data.getConf(), topologyId);
        try {
            PathUtils.rmr(dir_to_delete);
            LOG.info("delete:" + dir_to_delete + "successfully!");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error("delete:" + dir_to_delete + "fail!", e);
        }
    }

    private void downloadCodeFromMaster(Assignment assignment, String topologyId) throws IOException, TException {
        try {
            String localRoot = StormConfig.masterStormdistRoot(data.getConf(), topologyId);
            String tmpDir = StormConfig.masterInbox(data.getConf()) + "/" + UUID.randomUUID().toString();
            String masterCodeDir = assignment.getMasterCodeDir();
            JStormServerUtils.downloadCodeFromMaster(data.getConf(), tmpDir, masterCodeDir, topologyId, false);

            File srcDir = new File(tmpDir);
            File destDir = new File(localRoot);
            try {
                FileUtils.moveDirectory(srcDir, destDir);
            } catch (FileExistsException e) {
                FileUtils.copyDirectory(srcDir, destDir);
                FileUtils.deleteQuietly(srcDir);
            }
            // Update downloadCode timeStamp
            StormConfig.write_nimbus_topology_timestamp(data.getConf(), topologyId, System.currentTimeMillis());
        } catch (TException e) {
            // TODO Auto-generated catch block
            LOG.error(e + " downloadStormCode failed " + "topologyId:" + topologyId + "masterCodeDir:" + assignment.getMasterCodeDir());
            throw e;
        }
        LOG.info("Finished downloading code for topology id " + topologyId + " from " + assignment.getMasterCodeDir());
    }

    private void tryToBeLeader(final Map conf) throws Exception {
        boolean allowed = check_nimbus_priority();
        
        if (allowed){
            RunnableCallback masterCallback = new RunnableCallback() {
                @Override
                public void run() {
                    try {
                        tryToBeLeader(conf);
                    } catch (Exception e) {
                        LOG.error("To be master error", e);
                        JStormUtils.halt_process(30, "Cant't to be master" + e.getMessage());
                    }
                }
            };
            LOG.info("This nimbus can be  leader");
            data.getStormClusterState().try_to_be_leader(Cluster.MASTER_SUBTREE, hostPort, masterCallback);
        }else {
        	LOG.info("This nimbus can't be leader");
        }
    }
    /**
     * Compared with other nimbus ,get priority of this nimbus
     *
     * @throws Exception
     */
    private  boolean check_nimbus_priority() throws Exception {
    	
    	int gap = update_nimbus_detail();
    	if (gap == 0) {
    		return true;
    	}
    	
    	int left = SLAVE_NIMBUS_WAIT_TIME;
        while(left > 0) {
        	LOG.info( "After " + left + " seconds, nimbus will try to be Leader!");
            Thread.sleep(10 * 1000);
            left -= 10;
        }

        StormClusterState zkClusterState = data.getStormClusterState();

        List<String> followers = zkClusterState.list_dirs(Cluster.NIMBUS_SLAVE_DETAIL_SUBTREE, false);
        if (followers == null || followers.size() == 0) {
            return false;
        }

        for (String follower : followers) {
            if (follower != null && !follower.equals(hostPort)) {
                Map bMap = zkClusterState.get_nimbus_detail(follower, false);
                if (bMap != null){
                    Object object = bMap.get(NIMBUS_DIFFER_COUNT_ZK);
                    if (object != null && (JStormUtils.parseInt(object)) < gap){
                    	LOG.info("Current node can't be leader, due to {} has higher priority", follower);
                        return false;
                    }
                }
            }
        }
        
        
        
        return true;
    }
    private int update_nimbus_detail() throws Exception {

        //update count = count of zk's binary files - count of nimbus's binary files
        StormClusterState zkClusterState = data.getStormClusterState();
        String master_stormdist_root = StormConfig.masterStormdistRoot(data.getConf());
        List<String> code_ids = PathUtils.read_dir_contents(master_stormdist_root);
        List<String> assignments_ids = data.getStormClusterState().assignments(callback);
        assignments_ids.removeAll(code_ids);

        Map mtmp = zkClusterState.get_nimbus_detail(hostPort, false);
        if (mtmp == null){
            mtmp = new HashMap();
        }
        mtmp.put(NIMBUS_DIFFER_COUNT_ZK, assignments_ids.size());
        zkClusterState.update_nimbus_detail(hostPort, mtmp);
        LOG.debug("update nimbus's detail " + mtmp);
        return assignments_ids.size();
    }
    /**
     * Check whether current node is master or not
     * 
     * @throws Exception
     */
    private void checkOwnMaster() throws Exception {
        int retry_times = 10;

        StormClusterState zkClient = data.getStormClusterState();
        for (int i = 0; i < retry_times; i++, JStormUtils.sleepMs(sleepTime)) {

            if (zkClient.leader_existed() == false) {
                continue;
            }

            String zkHost = zkClient.get_leader_host();
            if (hostPort.equals(zkHost) == true) {
                // current process own master
                return;
            }
            LOG.warn("Current Nimbus has start thrift, but fail to own zk master :" + zkHost);
        }

        // current process doesn't own master
        String err = "Current Nimubs fail to own nimbus_master, should halt process";
        LOG.error(err);
        JStormUtils.halt_process(0, err);

    }

}

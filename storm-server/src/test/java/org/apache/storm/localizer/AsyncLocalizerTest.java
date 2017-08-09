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
package org.apache.storm.localizer;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.storm.DaemonConfig;
import org.apache.storm.daemon.supervisor.AdvancedFSOps;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ServerUtils;
import org.apache.storm.utils.ReflectionUtils;
import org.junit.Test;

import org.apache.storm.Config;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.security.auth.DefaultPrincipalToLocal;

public class AsyncLocalizerTest {

    @Test
    public void testRequestDownloadBaseTopologyBlobs() throws Exception {
        final String topoId = "TOPO";
        final String user = "user";
        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);
        la.set_owner(user);
        ExecutorInfo ei = new ExecutorInfo();
        ei.set_task_start(1);
        ei.set_task_end(1);
        la.add_to_executors(ei);
        final int port = 8080;
        final String jarKey = topoId + "-stormjar.jar";
        final String codeKey = topoId + "-stormcode.ser";
        final String confKey = topoId + "-stormconf.ser";
        final String stormLocal = "/tmp/storm-local/";
        final String stormRoot = stormLocal+topoId+"/";
        final File fStormRoot = new File(stormRoot);
        ClientBlobStore blobStore = mock(ClientBlobStore.class);
        Map<String, Object> conf = new HashMap<>();
        conf.put(DaemonConfig.SUPERVISOR_BLOBSTORE, ClientBlobStore.class.getName());
        conf.put(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN, DefaultPrincipalToLocal.class.getName());
        conf.put(Config.STORM_CLUSTER_MODE, "distributed");
        conf.put(Config.STORM_LOCAL_DIR, stormLocal);
        Localizer localizer = mock(Localizer.class);
        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        ConfigUtils mockedCU = mock(ConfigUtils.class);
        ReflectionUtils mockedRU = mock(ReflectionUtils.class);
        ServerUtils mockedU = mock(ServerUtils.class);
        
        Map<String, Object> topoConf = new HashMap<>(conf);
        
        AsyncLocalizer al = new AsyncLocalizer(conf, localizer, ops);
        ConfigUtils orig = ConfigUtils.setInstance(mockedCU);
        ReflectionUtils origRU = ReflectionUtils.setInstance(mockedRU);
        ServerUtils origUtils = ServerUtils.setInstance(mockedU);
        try {
            when(mockedCU.supervisorStormDistRootImpl(conf, topoId)).thenReturn(stormRoot);
            when(mockedCU.supervisorLocalDirImpl(conf)).thenReturn(stormLocal);
            when(mockedRU.newInstanceImpl(ClientBlobStore.class)).thenReturn(blobStore);
            when(mockedCU.readSupervisorStormConfImpl(conf, topoId)).thenReturn(topoConf);

            Future<Void> f = al.requestDownloadBaseTopologyBlobs(la, port);
            f.get(20, TimeUnit.SECONDS);
            // We should be done now...
            
            verify(blobStore).prepare(conf);
            verify(mockedU).downloadResourcesAsSupervisorImpl(eq(jarKey), startsWith(stormLocal), eq(blobStore));
            verify(mockedU).downloadResourcesAsSupervisorImpl(eq(codeKey), startsWith(stormLocal), eq(blobStore));
            verify(mockedU).downloadResourcesAsSupervisorImpl(eq(confKey), startsWith(stormLocal), eq(blobStore));
            verify(blobStore).shutdown();
            //Extracting the dir from the jar
            verify(mockedU).extractDirFromJarImpl(endsWith("stormjar.jar"), eq("resources"), any(File.class));
            verify(ops).moveDirectoryPreferAtomic(any(File.class), eq(fStormRoot));
            verify(ops).setupStormCodeDir(user, fStormRoot);
            
            verify(ops, never()).deleteIfExists(any(File.class));
        } finally {
            al.shutdown();
            ConfigUtils.setInstance(orig);
            ReflectionUtils.setInstance(origRU);
            ServerUtils.setInstance(origUtils);
        }
    }

    @Test
    public void testRequestDownloadTopologyBlobs() throws Exception {
        final String topoId = "TOPO-12345";
        final String user = "user";
        LocalAssignment la = new LocalAssignment();
        la.set_topology_id(topoId);
        la.set_owner(user);
        ExecutorInfo ei = new ExecutorInfo();
        ei.set_task_start(1);
        ei.set_task_end(1);
        la.add_to_executors(ei);
        final String topoName = "TOPO";
        final int port = 8080;
        final String simpleLocalName = "simple.txt";
        final String simpleKey = "simple";
        
        final String stormLocal = "/tmp/storm-local/";
        final File userDir = new File(stormLocal, user);
        final String stormRoot = stormLocal+topoId+"/";
        
        final String localizerRoot = "/tmp/storm-localizer/";
        final String simpleLocalFile = localizerRoot + user + "/simple";
        final String simpleCurrentLocalFile = localizerRoot + user + "/simple.current";
       
        final StormTopology st = new StormTopology();
        st.set_spouts(new HashMap<>());
        st.set_bolts(new HashMap<>());
        st.set_state_spouts(new HashMap<>());
 
        Map<String, Map<String, Object>> topoBlobMap = new HashMap<>();
        Map<String, Object> simple = new HashMap<>();
        simple.put("localname", simpleLocalName);
        simple.put("uncompress", false);
        topoBlobMap.put(simpleKey, simple);
        
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.STORM_LOCAL_DIR, stormLocal);
        Localizer localizer = mock(Localizer.class);
        AdvancedFSOps ops = mock(AdvancedFSOps.class);
        ConfigUtils mockedCU = mock(ConfigUtils.class);

        Map<String, Object> topoConf = new HashMap<>(conf);
        topoConf.put(Config.TOPOLOGY_BLOBSTORE_MAP, topoBlobMap);
        topoConf.put(Config.TOPOLOGY_NAME, topoName);
        
        List<LocalizedResource> localizedList = new ArrayList<>();
        LocalizedResource simpleLocal = new LocalizedResource(simpleKey, simpleLocalFile, false);
        localizedList.add(simpleLocal);
        
        AsyncLocalizer al = new AsyncLocalizer(conf, localizer, ops);
        ConfigUtils orig = ConfigUtils.setInstance(mockedCU);
        try {
            when(mockedCU.supervisorStormDistRootImpl(conf, topoId)).thenReturn(stormRoot);
            when(mockedCU.readSupervisorStormConfImpl(conf, topoId)).thenReturn(topoConf);
            when(mockedCU.readSupervisorTopologyImpl(conf, topoId, ops)).thenReturn(st);
            
            when(localizer.getLocalUserFileCacheDir(user)).thenReturn(userDir);
            
            when(localizer.getBlobs(any(List.class), eq(user), eq(topoName), eq(userDir))).thenReturn(localizedList);
            
            Future<Void> f = al.requestDownloadTopologyBlobs(la, port);
            f.get(20, TimeUnit.SECONDS);
            // We should be done now...
            
            verify(localizer).getLocalUserFileCacheDir(user);
            verify(ops).fileExists(userDir);
            verify(ops).forceMkdir(userDir);
            
            verify(localizer).getBlobs(any(List.class), eq(user), eq(topoName), eq(userDir));

            verify(ops).createSymlink(new File(stormRoot, simpleLocalName), new File(simpleCurrentLocalFile));
        } finally {
            al.shutdown();
            ConfigUtils.setInstance(orig);
        }
    }

}

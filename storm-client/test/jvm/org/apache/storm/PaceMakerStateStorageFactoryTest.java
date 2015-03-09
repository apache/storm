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

package org.apache.storm;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.storm.cluster.PaceMakerStateStorage;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBMessageData;
import org.apache.storm.generated.HBNodes;
import org.apache.storm.generated.HBPulse;
import org.apache.storm.generated.HBServerMessageType;
import org.apache.storm.pacemaker.PacemakerClient;
import org.apache.storm.pacemaker.PacemakerClientPool;
import org.apache.storm.pacemaker.PacemakerConnectionException;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PaceMakerStateStorageFactoryTest {
    
    @Captor
    private ArgumentCaptor<HBMessage> hbMessageCaptor;
    
    @Mock
    private PacemakerClient clientMock;
    private PacemakerClientPoolProxy clientPoolProxy;
    private PaceMakerStateStorage stateStorage;

    public void createPaceMakerStateStorage(HBServerMessageType messageType, HBMessageData messageData) throws Exception {
        HBMessage response = new HBMessage(messageType, messageData);
        when(clientMock.send(any())).thenReturn(response);
        clientPoolProxy = new PacemakerClientPoolProxy();
        stateStorage = new PaceMakerStateStorage(clientPoolProxy, null);
    }

    @Test
    public void testSetWorkerHb() throws Exception {
        createPaceMakerStateStorage(HBServerMessageType.SEND_PULSE_RESPONSE, null);
        stateStorage.set_worker_hb("/foo", "data".getBytes("UTF-8"), null);
        verify(clientMock).send(hbMessageCaptor.capture());
        HBMessage sent = hbMessageCaptor.getValue();
        HBPulse pulse = sent.get_data().get_pulse();
        Assert.assertEquals(HBServerMessageType.SEND_PULSE, sent.get_type());
        Assert.assertEquals("/foo", pulse.get_id());
        Assert.assertEquals("data", new String(pulse.get_details(), "UTF-8"));
    }

    @Test(expected = RuntimeException.class)
    public void testSetWorkerHbResponseType() throws Exception {
        createPaceMakerStateStorage(HBServerMessageType.SEND_PULSE, null);
        stateStorage.set_worker_hb("/foo", "data".getBytes("UTF-8"), null);
    }

    @Test
    public void testDeleteWorkerHb() throws Exception {
        createPaceMakerStateStorage(HBServerMessageType.DELETE_PATH_RESPONSE, null);
        stateStorage.delete_worker_hb("/foo/bar");
        verify(clientMock).send(hbMessageCaptor.capture());
        HBMessage sent = hbMessageCaptor.getValue();
        Assert.assertEquals(HBServerMessageType.DELETE_PATH, sent.get_type());
        Assert.assertEquals("/foo/bar", sent.get_data().get_path());
    }

    @Test(expected = RuntimeException.class)
    public void testDeleteWorkerHbResponseType() throws Exception {
        createPaceMakerStateStorage(HBServerMessageType.DELETE_PATH, null);
        stateStorage.delete_worker_hb("/foo/bar");
    }

    @Test
    public void testGetWorkerHb() throws Exception {
        HBPulse hbPulse = new HBPulse();
        hbPulse.set_id("/foo");
        ClusterWorkerHeartbeat cwh = new ClusterWorkerHeartbeat("some-storm-id", new HashMap(), 1, 1);
        hbPulse.set_details(Utils.serialize(cwh));
        createPaceMakerStateStorage(HBServerMessageType.GET_PULSE_RESPONSE, HBMessageData.pulse(hbPulse));
        stateStorage.get_worker_hb("/foo", false);
        verify(clientMock).send(hbMessageCaptor.capture());
        HBMessage sent = hbMessageCaptor.getValue();
        Assert.assertEquals(HBServerMessageType.GET_PULSE, sent.get_type());
        Assert.assertEquals("/foo", sent.get_data().get_path());
    }

    @Test(expected = RuntimeException.class)
    public void testGetWorkerHbBadResponse() throws Exception {
        createPaceMakerStateStorage(HBServerMessageType.GET_PULSE, null);
        stateStorage.get_worker_hb("/foo", false);
    }

    @Test(expected = RuntimeException.class)
    public void testGetWorkerHbBadData() throws Exception {
        createPaceMakerStateStorage(HBServerMessageType.GET_PULSE_RESPONSE, null);
        stateStorage.get_worker_hb("/foo", false);
    }

    @Test
    public void testGetWorkerHbChildren() throws Exception {
        createPaceMakerStateStorage(HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE, HBMessageData.nodes(new HBNodes()));
        stateStorage.get_worker_hb_children("/foo", false);
        verify(clientMock).send(hbMessageCaptor.capture());
        HBMessage sent = hbMessageCaptor.getValue();
        Assert.assertEquals(HBServerMessageType.GET_ALL_NODES_FOR_PATH, sent.get_type());
        Assert.assertEquals("/foo", sent.get_data().get_path());
    }

    @Test(expected = RuntimeException.class)
    public void testGetWorkerHbChildrenBadData() throws Exception {
        createPaceMakerStateStorage(HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE, null);
        stateStorage.get_worker_hb_children("/foo", false);
    }

    private class PacemakerClientPoolProxy extends PacemakerClientPool {
        public PacemakerClientPoolProxy() {
            super(new HashMap<>());
        }

        public PacemakerClient getWriteClient() {
            return clientMock;
        }

        @Override
        public HBMessage send(HBMessage m) throws PacemakerConnectionException, InterruptedException {
            return clientMock.send(m);
        }

        @Override
        public List<HBMessage> sendAll(HBMessage m) throws PacemakerConnectionException, InterruptedException {
            List<HBMessage> response = new ArrayList<>();
            response.add(clientMock.send(m));
            return response;
        }
    }

}

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

package org.apache.storm;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.storm.generated.HBMessage;
import org.apache.storm.generated.HBMessageData;
import org.apache.storm.generated.HBPulse;
import org.apache.storm.generated.HBServerMessageType;
import org.apache.storm.metric.StormMetricsRegistry;
import org.apache.storm.pacemaker.Pacemaker;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PacemakerTest {

    private HBMessage hbMessage;
    private int mid;
    private Random random;
    private Pacemaker handler;

    @BeforeEach
    public void init() {
        random = new Random(100);
        handler = new Pacemaker(new ConcurrentHashMap<>(), new StormMetricsRegistry());
    }

    @Test
    public void testServerCreatePath() {
        messageWithRandId(HBServerMessageType.CREATE_PATH, HBMessageData.path("/testpath"));
        HBMessage response = handler.handleMessage(hbMessage, true);
        assertEquals(mid, response.get_message_id());
        assertEquals(HBServerMessageType.CREATE_PATH_RESPONSE, response.get_type());
        assertNull(response.get_data());
    }

    @Test
    public void testServerExistsFalse() {
        messageWithRandId(HBServerMessageType.EXISTS, HBMessageData.path("/testpath"));
        HBMessage badResponse = handler.handleMessage(hbMessage, false);
        HBMessage goodResponse = handler.handleMessage(hbMessage, true);
        assertEquals(mid, badResponse.get_message_id());
        assertEquals(HBServerMessageType.NOT_AUTHORIZED, badResponse.get_type());

        assertEquals(mid, goodResponse.get_message_id());
        assertEquals(HBServerMessageType.EXISTS_RESPONSE, goodResponse.get_type());
        assertFalse(goodResponse.get_data().get_boolval());
    }

    @Test
    public void testServerExistsTrue() {
        String path = "/exists_path";
        String dataString = "pulse data";
        HBPulse hbPulse = new HBPulse();
        hbPulse.set_id(path);
        hbPulse.set_details(Utils.javaSerialize(dataString));
        messageWithRandId(HBServerMessageType.SEND_PULSE, HBMessageData.pulse(hbPulse));
        handler.handleMessage(hbMessage, true);

        messageWithRandId(HBServerMessageType.EXISTS, HBMessageData.path(path));
        HBMessage badResponse = handler.handleMessage(hbMessage, false);
        HBMessage goodResponse = handler.handleMessage(hbMessage, true);
        assertEquals(mid, badResponse.get_message_id());
        assertEquals(HBServerMessageType.NOT_AUTHORIZED, badResponse.get_type());

        assertEquals(mid, goodResponse.get_message_id());
        assertEquals(HBServerMessageType.EXISTS_RESPONSE, goodResponse.get_type());
        assertTrue(goodResponse.get_data().get_boolval());
    }

    @Test
    public void testServerSendPulseGetPulse() {
        String path = "/pulsepath";
        String dataString = "pulse data";
        HBPulse hbPulse = new HBPulse();
        hbPulse.set_id(path);
        hbPulse.set_details(dataString.getBytes(StandardCharsets.UTF_8));
        messageWithRandId(HBServerMessageType.SEND_PULSE, HBMessageData.pulse(hbPulse));
        HBMessage sendResponse = handler.handleMessage(hbMessage, true);
        assertEquals(mid, sendResponse.get_message_id());
        assertEquals(HBServerMessageType.SEND_PULSE_RESPONSE, sendResponse.get_type());
        assertNull(sendResponse.get_data());

        messageWithRandId(HBServerMessageType.GET_PULSE, HBMessageData.path(path));
        HBMessage response = handler.handleMessage(hbMessage, true);
        assertEquals(mid, response.get_message_id());
        assertEquals(HBServerMessageType.GET_PULSE_RESPONSE, response.get_type());
        assertEquals(dataString, new String(response.get_data().get_pulse().get_details(), StandardCharsets.UTF_8));
    }

    @Test
    public void testServerGetAllPulseForPath() {
        messageWithRandId(HBServerMessageType.GET_ALL_PULSE_FOR_PATH, HBMessageData.path("/testpath"));
        HBMessage badResponse = handler.handleMessage(hbMessage, false);
        HBMessage goodResponse = handler.handleMessage(hbMessage, true);
        assertEquals(mid, badResponse.get_message_id());
        assertEquals(HBServerMessageType.NOT_AUTHORIZED, badResponse.get_type());

        assertEquals(mid, goodResponse.get_message_id());
        assertEquals(HBServerMessageType.GET_ALL_PULSE_FOR_PATH_RESPONSE, goodResponse.get_type());
        assertNull(goodResponse.get_data());
    }

    @Test
    public void testServerGetAllNodesForPath() throws UnsupportedEncodingException {
        makeNode(handler, "/some-root-path/foo");
        makeNode(handler, "/some-root-path/bar");
        makeNode(handler, "/some-root-path/baz");
        makeNode(handler, "/some-root-path/boo");
        messageWithRandId(HBServerMessageType.GET_ALL_NODES_FOR_PATH, HBMessageData.path("/some-root-path"));
        HBMessage badResponse = handler.handleMessage(hbMessage, false);
        HBMessage goodResponse = handler.handleMessage(hbMessage, true);
        List<String> pulseIds = goodResponse.get_data().get_nodes().get_pulseIds();

        assertEquals(mid, badResponse.get_message_id());
        assertEquals(HBServerMessageType.NOT_AUTHORIZED, badResponse.get_type());

        assertEquals(mid, goodResponse.get_message_id());
        assertEquals(HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE, goodResponse.get_type());

        assertTrue(pulseIds.contains("foo"));
        assertTrue(pulseIds.contains("bar"));
        assertTrue(pulseIds.contains("baz"));
        assertTrue(pulseIds.contains("boo"));

        makeNode(handler, "/some/deeper/path/foo");
        makeNode(handler, "/some/deeper/path/bar");
        makeNode(handler, "/some/deeper/path/baz");
        messageWithRandId(HBServerMessageType.GET_ALL_NODES_FOR_PATH, HBMessageData.path("/some/deeper/path"));
        badResponse = handler.handleMessage(hbMessage, false);
        goodResponse = handler.handleMessage(hbMessage, true);
        pulseIds = goodResponse.get_data().get_nodes().get_pulseIds();

        assertEquals(mid, badResponse.get_message_id());
        assertEquals(HBServerMessageType.NOT_AUTHORIZED, badResponse.get_type());

        assertEquals(mid, goodResponse.get_message_id());
        assertEquals(HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE, goodResponse.get_type());

        assertTrue(pulseIds.contains("foo"));
        assertTrue(pulseIds.contains("bar"));
        assertTrue(pulseIds.contains("baz"));
    }

    @Test
    public void testServerGetPulse() throws UnsupportedEncodingException {
        makeNode(handler, "/some-root/GET_PULSE");
        messageWithRandId(HBServerMessageType.GET_PULSE, HBMessageData.path("/some-root/GET_PULSE"));
        HBMessage badResponse = handler.handleMessage(hbMessage, false);
        HBMessage goodResponse = handler.handleMessage(hbMessage, true);
        HBPulse goodPulse = goodResponse.get_data().get_pulse();
        assertEquals(mid, badResponse.get_message_id());
        assertEquals(HBServerMessageType.NOT_AUTHORIZED, badResponse.get_type());
        assertNull(badResponse.get_data());

        assertEquals(mid, goodResponse.get_message_id());
        assertEquals(HBServerMessageType.GET_PULSE_RESPONSE, goodResponse.get_type());
        assertEquals("/some-root/GET_PULSE", goodPulse.get_id());
        assertEquals("nothing", new String(goodPulse.get_details(), StandardCharsets.UTF_8));
    }

    @Test
    public void testServerDeletePath() throws UnsupportedEncodingException {
        makeNode(handler, "/some-root/DELETE_PATH/foo");
        makeNode(handler, "/some-root/DELETE_PATH/bar");
        makeNode(handler, "/some-root/DELETE_PATH/baz");
        makeNode(handler, "/some-root/DELETE_PATH/boo");

        messageWithRandId(HBServerMessageType.DELETE_PATH, HBMessageData.path("/some-root/DELETE_PATH"));
        HBMessage response = handler.handleMessage(hbMessage, true);
        assertEquals(mid, response.get_message_id());
        assertEquals(HBServerMessageType.DELETE_PATH_RESPONSE, response.get_type());
        assertNull(response.get_data());

        messageWithRandId(HBServerMessageType.GET_ALL_NODES_FOR_PATH, HBMessageData.path("/some-root/DELETE_PATH"));
        response = handler.handleMessage(hbMessage, true);
        List<String> pulseIds = response.get_data().get_nodes().get_pulseIds();
        assertEquals(mid, response.get_message_id());
        assertEquals(HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE, response.get_type());
        assertTrue(pulseIds.isEmpty());
    }

    @Test
    public void testServerDeletePulseId() throws UnsupportedEncodingException {
        makeNode(handler, "/some-root/DELETE_PULSE_ID/foo");
        makeNode(handler, "/some-root/DELETE_PULSE_ID/bar");
        makeNode(handler, "/some-root/DELETE_PULSE_ID/baz");
        makeNode(handler, "/some-root/DELETE_PULSE_ID/boo");

        messageWithRandId(HBServerMessageType.DELETE_PULSE_ID, HBMessageData.path("/some-root/DELETE_PULSE_ID/foo"));
        HBMessage response = handler.handleMessage(hbMessage, true);
        assertEquals(mid, response.get_message_id());
        assertEquals(HBServerMessageType.DELETE_PULSE_ID_RESPONSE, response.get_type());
        assertNull(response.get_data());

        messageWithRandId(HBServerMessageType.GET_ALL_NODES_FOR_PATH, HBMessageData.path("/some-root/DELETE_PULSE_ID"));
        response = handler.handleMessage(hbMessage, true);
        List<String> pulseIds = response.get_data().get_nodes().get_pulseIds();
        assertEquals(mid, response.get_message_id());
        assertEquals(HBServerMessageType.GET_ALL_NODES_FOR_PATH_RESPONSE, response.get_type());
        assertFalse(pulseIds.contains("foo"));
    }

    private void messageWithRandId(HBServerMessageType type, HBMessageData data) {
        mid = random.nextInt();
        hbMessage = new HBMessage(type, data);
        hbMessage.set_message_id(mid);
    }

    private HBMessage makeNode(Pacemaker handler, String path) throws UnsupportedEncodingException {
        HBPulse hbPulse = new HBPulse();
        hbPulse.set_id(path);
        hbPulse.set_details("nothing".getBytes(StandardCharsets.UTF_8));
        HBMessage message = new HBMessage(HBServerMessageType.SEND_PULSE, HBMessageData.pulse(hbPulse));
        return handler.handleMessage(message, true);
    }
}

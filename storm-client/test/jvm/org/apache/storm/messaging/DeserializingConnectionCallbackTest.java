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

package org.apache.storm.messaging;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.daemon.worker.WorkerState;
import org.apache.storm.serialization.KryoTupleDeserializer;
import org.apache.storm.serialization.KryoTupleSerializer;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeserializingConnectionCallbackTest {
    private static final byte[] messageBytes = new byte[3];
    private static TaskMessage message;

    private static final String SOURCE_COMPONENT = "1";
    private static final String DEST_COMPONENT = "2";
    private static final int SOURCE_TASK_ID = 1;
    private static final int DEST_TASK_ID = 2;
    private static final byte[] JAVA_STREAM_HEADER = {(byte) 0xAC, (byte) 0xED, 0x00, 0x05};

    private GeneralTopologyContext context;

    @BeforeEach
    public void setUp() throws Exception {
        // Setup a test message
        message = mock(TaskMessage.class);
        when(message.task()).thenReturn(456); // destination taskId
        when(message.message()).thenReturn(messageBytes);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SOURCE_COMPONENT, new TestWordSpout(true), 1);
        builder.setBolt(DEST_COMPONENT, new TestWordCounter(), 1).fieldsGrouping(SOURCE_COMPONENT, new Fields("word"));
        context = mock(GeneralTopologyContext.class);
        when(context.getRawTopology()).thenReturn(builder.createTopology());
        when(context.getComponentId(SOURCE_TASK_ID)).thenReturn(SOURCE_COMPONENT);
    }


    @Test
    public void testUpdateMetricsConfigOff() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_SERIALIZED_MESSAGE_SIZE_METRICS, Boolean.FALSE);
        DeserializingConnectionCallback withoutMetrics =
            new DeserializingConnectionCallback(config, mock(GeneralTopologyContext.class), mock(
                WorkerState.ILocalTransferCallback.class));

        // Metrics are off, verify null
        assertNull(withoutMetrics.getValueAndReset());

        // Add our messages and verify no metrics are recorded  
        withoutMetrics.updateMetrics(123, message);
        assertNull(withoutMetrics.getValueAndReset());
    }

    @Test
    public void testUpdateMetricsConfigOn() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_SERIALIZED_MESSAGE_SIZE_METRICS, Boolean.TRUE);
        DeserializingConnectionCallback withMetrics =
            new DeserializingConnectionCallback(config, mock(GeneralTopologyContext.class), mock(
                WorkerState.ILocalTransferCallback.class));

        // Starting empty
        Object metrics = withMetrics.getValueAndReset();
        assertTrue(metrics instanceof Map);
        assertTrue(((Map<?,?>) metrics).isEmpty());

        // Add messages
        withMetrics.updateMetrics(123, message);
        withMetrics.updateMetrics(123, message);

        // Verify recorded messages size metrics 
        metrics = withMetrics.getValueAndReset();
        assertTrue(metrics instanceof Map);
        assertEquals(6L, ((Map<?, ?>) metrics).get("123-456"));
    }

    @Test
    public void testBatchWithCorruptMessageDropsOnlyCorruptMessage() {
        Map<String, Object> conf = baseConf();
        byte[] corrupt = new byte[]{1, 2, 3};
        assertThrows(RuntimeException.class,
                     () -> new KryoTupleDeserializer(conf, context).deserialize(corrupt));

        assertBatchDeliversOnlyValidMessages(conf, corrupt);
    }

    @Test
    public void testTruncatedKryoPayloadDroppedAndBatchContinues() {
        Map<String, Object> conf = baseConf();
        byte[] full = serializedTuple(conf, new Values("a-string-long-enough-to-survive-truncation", 7));
        byte[] truncated = Arrays.copyOf(full, full.length - 10);

        assertThrows(KryoException.class, () -> new KryoTupleDeserializer(conf, context).deserialize(truncated));

        assertBatchDeliversOnlyValidMessages(conf, truncated);
    }

    @Test
    public void testUnknownSourceTaskDroppedAndBatchContinues() {
        Map<String, Object> conf = baseConf();
        Output out = new Output(16, 32);
        out.writeInt(9999, true); // source task that does not exist in the topology
        out.writeInt(1, true);    // default stream id
        byte[] unknownTask = out.toBytes();

        assertThrows(IllegalArgumentException.class, () -> new KryoTupleDeserializer(conf, context).deserialize(unknownTask));

        assertBatchDeliversOnlyValidMessages(conf, unknownTask);
    }

    @Test
    public void testJavaFallbackMissingClassDroppedAndBatchContinues() {
        Map<String, Object> conf = baseConf();
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);
        byte[] bytes = serializedTuple(conf, Collections.singletonList(new JavaSerializedValue()));
        byte[] missingClass = replaceAll(bytes, "JavaSerializedValue", "JavaSerializedValuf");

        RuntimeException thrown = assertThrows(RuntimeException.class,
                                               () -> new KryoTupleDeserializer(conf, context).deserialize(missingClass));
        assertTrue(Utils.exceptionCauseIsInstanceOf(ClassNotFoundException.class, thrown),
                   "expected a ClassNotFoundException in the cause chain but was: " + thrown);

        assertBatchDeliversOnlyValidMessages(conf, missingClass);
    }

    @Test
    public void testJavaFallbackNegativeLengthDroppedAndBatchContinues() {
        Map<String, Object> conf = baseConf();
        conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, true);
        byte[] bytes = serializedTuple(conf, Collections.singletonList(new JavaSerializedValue()));

        // SerializableSerializer writes the java-serialization byte count right before the stream header;
        // an all-bits-set count makes it allocate a negative-length array.
        int headerIdx = indexOf(bytes, JAVA_STREAM_HEADER, 0);
        assertTrue(headerIdx >= 4, "java serialization header not found in tuple payload");
        for (int i = 1; i <= 4; i++) {
            bytes[headerIdx - i] = (byte) 0xFF;
        }

        assertThrows(NegativeArraySizeException.class, () -> new KryoTupleDeserializer(conf, context).deserialize(bytes));

        assertBatchDeliversOnlyValidMessages(conf, bytes);
    }

    @Test
    public void testIoExceptionFailureDroppedAndBatchContinues() {
        Map<String, Object> conf = baseConf();
        conf.put(Config.TOPOLOGY_TUPLE_COMPRESSION_ENABLE, true);
        byte[] fakeZstd = {(byte) 0x28, (byte) 0xB5, (byte) 0x2F, (byte) 0xFD, 0x00, 0x01, 0x02, 0x03};

        RuntimeException thrown = assertThrows(RuntimeException.class,
                                               () -> new KryoTupleDeserializer(conf, context).deserialize(fakeZstd.clone()));
        assertTrue(Utils.exceptionCauseIsInstanceOf(IOException.class, thrown),
                   "expected an IOException in the cause chain but was: " + thrown);

        assertBatchDeliversOnlyValidMessages(conf, fakeZstd);
    }

    @Test
    public void testFailuresCountedSeparatelyFromSizeMetrics() {
        Map<String, Object> conf = baseConf();
        conf.put(Config.TOPOLOGY_SERIALIZED_MESSAGE_SIZE_METRICS, Boolean.TRUE);
        WorkerState.ILocalTransferCallback transfer = mock(WorkerState.ILocalTransferCallback.class);
        DeserializingConnectionCallback callback = new DeserializingConnectionCallback(conf, context, transfer);

        callback.recv(Arrays.asList(
            taskMessage(serializedTuple(conf, new Values("nathan", 1))),
            taskMessage(new byte[]{1, 2, 3})));

        Object metrics = callback.getValueAndReset();
        assertTrue(metrics instanceof Map);
        assertEquals(1, ((Map<?, ?>) metrics).size());
        assertTrue(((Map<?, ?>) metrics).containsKey("1-2"));

        assertEquals(1L, callback.getAndResetDeserializationFailures());
        assertEquals(0L, callback.getAndResetDeserializationFailures());
    }

    @Test
    public void testNonToleratedExceptionPropagates() throws Exception {
        WorkerState.ILocalTransferCallback transfer = mock(WorkerState.ILocalTransferCallback.class);
        DeserializingConnectionCallback callback = new DeserializingConnectionCallback(baseConf(), context, transfer);
        KryoTupleDeserializer failing = mock(KryoTupleDeserializer.class);
        when(failing.deserialize(any(byte[].class))).thenThrow(new IllegalStateException("injected"));
        callback.setDeserializer(failing);

        assertThrows(IllegalStateException.class,
                     () -> callback.recv(Collections.singletonList(taskMessage(new byte[]{1}))));

        verify(transfer, never()).transfer(any());
        assertEquals(0L, callback.getAndResetDeserializationFailures());
        assertNull(callback.getValueAndReset());
    }

    private void assertBatchDeliversOnlyValidMessages(Map<String, Object> conf, byte[] badPayload) {
        WorkerState.ILocalTransferCallback transfer = mock(WorkerState.ILocalTransferCallback.class);
        DeserializingConnectionCallback callback = new DeserializingConnectionCallback(conf, context, transfer);

        callback.recv(Arrays.asList(
            taskMessage(serializedTuple(conf, new Values("nathan", 1))),
            taskMessage(badPayload),
            taskMessage(serializedTuple(conf, new Values("golda", 2)))));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ArrayList<AddressedTuple>> captor = ArgumentCaptor.forClass(ArrayList.class);
        verify(transfer).transfer(captor.capture());
        List<AddressedTuple> delivered = captor.getValue();
        assertEquals(2, delivered.size());
        assertEquals(DEST_TASK_ID, delivered.get(0).getDest());
        assertEquals(new Values("nathan", 1), delivered.get(0).getTuple().getValues());
        assertEquals(DEST_TASK_ID, delivered.get(1).getDest());
        assertEquals(new Values("golda", 2), delivered.get(1).getTuple().getValues());

        assertEquals(1L, callback.getAndResetDeserializationFailures());
        assertNull(callback.getValueAndReset());
    }

    private Map<String, Object> baseConf() {
        Map<String, Object> conf = new HashMap<>(Utils.readStormConfig());
        return conf;
    }

    private byte[] serializedTuple(Map<String, Object> conf, List<Object> values) {
        TupleImpl tuple = new TupleImpl(context, values, SOURCE_COMPONENT, SOURCE_TASK_ID,
                                        Utils.DEFAULT_STREAM_ID, MessageId.makeUnanchored());
        return new KryoTupleSerializer(conf, context).serialize(tuple);
    }

    private static TaskMessage taskMessage(byte[] payload) {
        return new TaskMessage(DEST_TASK_ID, payload);
    }

    private static byte[] replaceAll(byte[] src, String from, String to) {
        byte[] out = src.clone();
        byte[] fromBytes = from.getBytes(StandardCharsets.US_ASCII);
        byte[] toBytes = to.getBytes(StandardCharsets.US_ASCII);
        int idx = indexOf(out, fromBytes, 0);
        while (idx >= 0) {
            System.arraycopy(toBytes, 0, out, idx, toBytes.length);
            idx = indexOf(out, fromBytes, idx + toBytes.length);
        }
        return out;
    }

    private static int indexOf(byte[] src, byte[] pattern, int from) {
        outer:
        for (int i = from; i <= src.length - pattern.length; i++) {
            for (int j = 0; j < pattern.length; j++) {
                if (src[i + j] != pattern[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static class JavaSerializedValue implements Serializable {
    }
}

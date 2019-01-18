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

package org.apache.storm.redis.state;

import com.google.common.primitives.UnsignedBytes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.storm.redis.common.commands.RedisCommands;
import org.apache.storm.redis.common.container.RedisCommandsInstanceContainer;
import org.apache.storm.state.DefaultStateSerializer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import redis.clients.util.SafeEncoder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link RedisKeyValueState}
 *
 * NOTE: The type of key for mockMap is String, which should be byte[],
 * since but byte[] doesn't implement equals() so taking workaround to make life happier.
 * It shouldn't make issues on Redis side, since raw type of Redis is binary.
 */
public class RedisKeyValueStateTest {
    RedisCommandsInstanceContainer mockContainer;
    RedisCommands mockCommands;
    RedisKeyValueState<String, String> keyValueState;

    @Before
    public void setUp() {
        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> mockMap =
            new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());
        mockContainer = Mockito.mock(RedisCommandsInstanceContainer.class);
        mockCommands = Mockito.mock(RedisCommands.class);
        Mockito.when(mockContainer.getInstance()).thenReturn(mockCommands);
        ArgumentCaptor<String> stringArgumentCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> stringArgumentCaptor2 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map> mapArgumentCaptor = ArgumentCaptor.forClass(Map.class);

        Mockito.when(mockCommands.exists(Mockito.any(byte[].class)))
               .thenAnswer(new Answer<Boolean>() {
                   @Override
                   public Boolean answer(InvocationOnMock invocation) throws Throwable {
                       Object[] args = invocation.getArguments();
                       return exists(mockMap, (byte[]) args[0]);
                   }
               });

        Mockito.when(mockCommands.del(Mockito.any(byte[].class)))
               .thenAnswer(new Answer<Long>() {
                   @Override
                   public Long answer(InvocationOnMock invocation) throws Throwable {
                       Object[] args = invocation.getArguments();
                       return del(mockMap, (byte[]) args[0]);
                   }
               });

        Mockito.when(mockCommands.hmset(Mockito.any(byte[].class), Mockito.anyMap()))
               .thenAnswer(new Answer<String>() {
                   @Override
                   public String answer(InvocationOnMock invocation) throws Throwable {
                       Object[] args = invocation.getArguments();
                       return hmset(mockMap, (byte[]) args[0], (Map<byte[], byte[]>) args[1]);
                   }
               });

        Mockito.when(mockCommands.hget(Mockito.any(byte[].class), Mockito.any(byte[].class)))
               .thenAnswer(new Answer<byte[]>() {
                   @Override
                   public byte[] answer(InvocationOnMock invocation) throws Throwable {
                       Object[] args = invocation.getArguments();
                       return hget(mockMap, (byte[]) args[0], (byte[]) args[1]);
                   }
               });

        Mockito.when(mockCommands.hdel(Mockito.any(byte[].class), Mockito.<byte[]>anyVararg()))
               .thenAnswer(new Answer<Long>() {
                   @Override
                   public Long answer(InvocationOnMock invocation) throws Throwable {
                       Object[] args = invocation.getArguments();
                       int argsSize = args.length;
                       byte[][] fields = Arrays.asList(args).subList(1, argsSize).toArray(new byte[argsSize - 1][]);
                       return hdel(mockMap, (byte[]) args[0], fields);
                   }
               });

        Mockito.when(mockCommands.exists(Mockito.anyString()))
               .thenAnswer(new Answer<Boolean>() {
                   @Override
                   public Boolean answer(InvocationOnMock invocation) throws Throwable {
                       Object[] args = invocation.getArguments();
                       return exists(mockMap, (String) args[0]);
                   }
               });

        Mockito.when(mockCommands.hmset(Mockito.anyString(), Mockito.anyMap()))
               .thenAnswer(new Answer<String>() {
                   @Override
                   public String answer(InvocationOnMock invocation) throws Throwable {
                       Object[] args = invocation.getArguments();
                       return hmset(mockMap, (String) args[0], (Map<String, String>) args[1]);
                   }
               });

        Mockito.when(mockCommands.hgetAll(Mockito.anyString()))
               .thenAnswer(new Answer<Map<String, String>>() {
                   @Override
                   public Map<String, String> answer(InvocationOnMock invocation) throws Throwable {
                       Object[] args = invocation.getArguments();
                       return hgetAll(mockMap, (String) args[0]);
                   }
               });

        keyValueState = new RedisKeyValueState<String, String>("test", mockContainer, new DefaultStateSerializer<String>(),
                                                               new DefaultStateSerializer<String>());
    }

    @Test
    public void testPutAndGet() throws Exception {
        keyValueState.put("a", "1");
        keyValueState.put("b", "2");
        assertEquals("1", keyValueState.get("a"));
        assertEquals("2", keyValueState.get("b"));
        assertEquals(null, keyValueState.get("c"));
    }

    @Test
    public void testPutAndDelete() throws Exception {
        keyValueState.put("a", "1");
        keyValueState.put("b", "2");
        assertEquals("1", keyValueState.get("a"));
        assertEquals("2", keyValueState.get("b"));
        assertEquals(null, keyValueState.get("c"));
        assertEquals("1", keyValueState.delete("a"));
        assertEquals(null, keyValueState.get("a"));
        assertEquals("2", keyValueState.get("b"));
        assertEquals(null, keyValueState.get("c"));
    }

    @Test
    public void testPrepareCommitRollback() throws Exception {
        keyValueState.put("a", "1");
        keyValueState.put("b", "2");
        keyValueState.prepareCommit(1);
        keyValueState.put("c", "3");
        assertArrayEquals(new String[]{ "1", "2", "3" }, getValues());
        keyValueState.rollback();
        assertArrayEquals(new String[]{ null, null, null }, getValues());
        keyValueState.put("a", "1");
        keyValueState.put("b", "2");
        keyValueState.prepareCommit(1);
        keyValueState.commit(1);
        keyValueState.put("c", "3");
        assertArrayEquals(new String[]{ "1", "2", "3" }, getValues());
        keyValueState.rollback();
        assertArrayEquals(new String[]{ "1", "2", null }, getValues());
        keyValueState.put("c", "3");
        assertEquals("2", keyValueState.delete("b"));
        assertEquals("3", keyValueState.delete("c"));
        assertArrayEquals(new String[]{ "1", null, null }, getValues());
        keyValueState.prepareCommit(2);
        assertArrayEquals(new String[]{ "1", null, null }, getValues());
        keyValueState.commit(2);
        assertArrayEquals(new String[]{ "1", null, null }, getValues());
        keyValueState.put("b", "2");
        keyValueState.prepareCommit(3);
        keyValueState.put("c", "3");
        assertArrayEquals(new String[]{ "1", "2", "3" }, getValues());
        keyValueState.rollback();
        assertArrayEquals(new String[]{ "1", null, null }, getValues());
    }

    private String[] getValues() {
        return new String[]{
            keyValueState.get("a"),
            keyValueState.get("b"),
            keyValueState.get("c")
        };
    }

    private Boolean exists(NavigableMap<byte[], NavigableMap<byte[], byte[]>> mockMap, byte[] key) {
        return mockMap.containsKey(key);
    }

    private String hmset(NavigableMap<byte[], NavigableMap<byte[], byte[]>> mockMap, byte[] key, Map<byte[], byte[]> value) {
        NavigableMap<byte[], byte[]> currentValue = mockMap.get(key);
        if (currentValue == null) {
            currentValue = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        }

        for (Map.Entry<byte[], byte[]> entry : value.entrySet()) {
            currentValue.put(entry.getKey(), entry.getValue());
        }

        mockMap.put(key, currentValue);
        return "";
    }

    private Long del(NavigableMap<byte[], NavigableMap<byte[], byte[]>> mockMap, byte[] key) {
        if (mockMap.remove(key) == null) {
            return 0L;
        } else {
            return 1L;
        }
    }

    private byte[] hget(NavigableMap<byte[], NavigableMap<byte[], byte[]>> mockMap, byte[] namespace, byte[] key) {
        if (mockMap.containsKey(namespace)) {
            return mockMap.get(namespace).get(key);
        }
        return null;
    }

    private Long hdel(NavigableMap<byte[], NavigableMap<byte[], byte[]>> mockMap, byte[] namespace, byte[]... keys) {
        Long count = 0L;
        for (byte[] key : keys) {
            if (mockMap.get(namespace).remove(key) != null) count++;
        }
        return count;
    }

    private Boolean exists(NavigableMap<byte[], NavigableMap<byte[], byte[]>> mockMap, String key) {
        return mockMap.containsKey(SafeEncoder.encode(key));
    }


    private String hmset(NavigableMap<byte[], NavigableMap<byte[], byte[]>> mockMap, String key, Map<String, String> value) {
        NavigableMap<byte[], byte[]> currentValue = mockMap.get(SafeEncoder.encode(key));
        if (currentValue == null) {
            currentValue = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
        }

        for (Map.Entry<String, String> entry : value.entrySet()) {
            currentValue.put(SafeEncoder.encode(entry.getKey()), SafeEncoder.encode(entry.getValue()));
        }

        mockMap.put(SafeEncoder.encode(key), currentValue);
        return "";
    }

    private Map<String, String> hgetAll(NavigableMap<byte[], NavigableMap<byte[], byte[]>> mockMap, String key) {
        Map<byte[], byte[]> currentValue = mockMap.get(SafeEncoder.encode(key));

        Map<String, String> converted = new HashMap<>(currentValue.size());
        for (Map.Entry<byte[], byte[]> entry : currentValue.entrySet()) {
            converted.put(SafeEncoder.encode(entry.getKey()), SafeEncoder.encode(entry.getValue()));
        }

        return converted;
    }
}

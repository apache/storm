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

package org.apache.storm.messaging.netty;

import java.io.IOException;
import java.util.Arrays;
import org.apache.storm.serialization.KryoValuesSerializer;
import org.apache.storm.shade.io.netty.buffer.ByteBuf;
import org.apache.storm.shade.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.storm.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BackPressureStatusTest {

    @Test
    void bufferTest() throws IOException {
        UnpooledByteBufAllocator alloc = new UnpooledByteBufAllocator(false);
        KryoValuesSerializer ser = new KryoValuesSerializer(Utils.readStormConfig());

        BackPressureStatus status = new BackPressureStatus("test-worker", Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6));
        status.buffer(alloc, ser).release();
    }
}
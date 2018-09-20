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

package org.apache.storm.rocketmq.spout.scheme;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class StringScheme implements Scheme {
    public static final String STRING_SCHEME_KEY = "str";

    @Override
    public List<Object> deserialize(ByteBuffer bytes) {
        return new Values(deserializeString(bytes));
    }

    /**
     * Deserialize ByteBuffer to String.
     * @param byteBuffer input ByteBuffer
     * @return deserialized string
     */
    public static String deserializeString(ByteBuffer byteBuffer) {
        if (byteBuffer.hasArray()) {
            int base = byteBuffer.arrayOffset();
            return new String(byteBuffer.array(), base + byteBuffer.position(), byteBuffer.remaining(),
                StandardCharsets.UTF_8);
        } else {
            return new String(Utils.toByteArray(byteBuffer), StandardCharsets.UTF_8);
        }
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(STRING_SCHEME_KEY);
    }
}

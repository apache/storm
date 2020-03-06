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

package org.apache.storm.rocketmq;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import org.apache.rocketmq.common.message.Message;
import org.apache.storm.rocketmq.spout.scheme.KeyValueScheme;
import org.apache.storm.spout.Scheme;

public final class RocketMqUtils {

    public static int getInteger(Properties props, String key, int defaultValue) {
        return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static long getLong(Properties props, String key, long defaultValue) {
        return Long.parseLong(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        return Boolean.parseBoolean(props.getProperty(key, String.valueOf(defaultValue)));
    }

    /**
     * Create Scheme by Properties.
     * @param props Properties
     * @return Scheme
     */
    public static Scheme createScheme(Properties props) {
        String schemeString = props.getProperty(SpoutConfig.SCHEME, SpoutConfig.DEFAULT_SCHEME);
        Scheme scheme;
        try {
            Class clazz = Class.forName(schemeString);
            scheme = (Scheme) clazz.newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot create Scheme for " + schemeString
                    + " due to " + e.getMessage());
        }
        return scheme;
    }

    /**
     * Generate Storm tuple values by Message and Scheme.
     * @param msg RocketMQ Message
     * @param scheme Scheme for deserializing
     * @return tuple values
     */
    public static List<Object> generateTuples(Message msg, Scheme scheme) {
        List<Object> tup;
        String rawKey = msg.getKeys();
        ByteBuffer body = ByteBuffer.wrap(msg.getBody());
        if (rawKey != null && scheme instanceof KeyValueScheme) {
            ByteBuffer key = ByteBuffer.wrap(rawKey.getBytes(StandardCharsets.UTF_8));
            tup = ((KeyValueScheme) scheme).deserializeKeyAndValue(key, body);
        } else {
            tup = scheme.deserialize(body);
        }
        return tup;
    }
}

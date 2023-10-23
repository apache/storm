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

package org.apache.storm.trident.state;

import java.nio.charset.StandardCharsets;

import org.apache.storm.shade.net.minidev.json.JSONValue;
import org.apache.storm.shade.net.minidev.json.parser.ParseException;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JSONNonTransactionalSerializer implements Serializer {

    @Override
    public byte[] serialize(Object obj) {
        return JSONValue.toJSONString(obj).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object deserialize(byte[] b) {
        try {
            return JSONValue.parseWithException(new String(b, StandardCharsets.UTF_8));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

}

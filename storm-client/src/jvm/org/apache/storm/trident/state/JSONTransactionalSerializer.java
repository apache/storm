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
import java.util.ArrayList;
import java.util.List;
import org.apache.storm.shade.net.minidev.json.JSONValue;
import org.apache.storm.shade.net.minidev.json.parser.ParseException;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JSONTransactionalSerializer implements Serializer<TransactionalValue> {
    @Override
    public byte[] serialize(TransactionalValue obj) {
        List toSer = new ArrayList(2);
        toSer.add(obj.getTxid());
        toSer.add(obj.getVal());
        return JSONValue.toJSONString(toSer).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TransactionalValue deserialize(byte[] b) {
        try {
            String s = new String(b, StandardCharsets.UTF_8);
            List deser = (List) JSONValue.parseWithException(s);
            return new TransactionalValue(((Number) deser.get(0)).longValue(), deser.get(1));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

}

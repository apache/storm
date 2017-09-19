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

package org.apache.storm.rocketmq.common.mapper;

import org.apache.storm.rocketmq.DefaultMessageBodySerializer;
import org.apache.storm.rocketmq.MessageBodySerializer;
import org.apache.storm.tuple.ITuple;

public class FieldNameBasedTupleToMessageMapper implements TupleToMessageMapper {
    public static final String BOLT_KEY = "key";
    public static final String BOLT_MESSAGE = "message";
    public String boltKeyField;
    public String boltMessageField;
    private MessageBodySerializer messageBodySerializer;

    public FieldNameBasedTupleToMessageMapper() {
        this(BOLT_KEY, BOLT_MESSAGE);
    }

    /**
     * FieldNameBasedTupleToMessageMapper Constructor.
     * @param boltKeyField tuple field for selecting the key
     * @param boltMessageField  tuple field for selecting the value
     */
    public FieldNameBasedTupleToMessageMapper(String boltKeyField, String boltMessageField) {
        this.boltKeyField = boltKeyField;
        this.boltMessageField = boltMessageField;
        this.messageBodySerializer = new DefaultMessageBodySerializer();
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField(boltKeyField);
    }

    @Override
    public byte[] getValueFromTuple(ITuple tuple) {
        Object obj = tuple.getValueByField(boltMessageField);
        if (obj == null) {
            return null;
        }
        return messageBodySerializer.serialize(obj);
    }

    /**
     * using this method can override the default  MessageBodySerializer.
     * @param serializer MessageBodySerializer
     * @return this object
     */
    public FieldNameBasedTupleToMessageMapper withMessageBodySerializer(MessageBodySerializer serializer) {
        this.messageBodySerializer = serializer;
        return this;
    }
}

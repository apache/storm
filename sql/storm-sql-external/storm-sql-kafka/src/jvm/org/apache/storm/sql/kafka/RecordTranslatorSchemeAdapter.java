/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.sql.kafka;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.RecordTranslator;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;

/**
 * RecordTranslator that delegates to a Scheme. 
 */
public class RecordTranslatorSchemeAdapter implements RecordTranslator<ByteBuffer, ByteBuffer> {

    private static final long serialVersionUID = 1L;
    private final Scheme delegate;

    public RecordTranslatorSchemeAdapter(Scheme delegate) {
        this.delegate = delegate;
    }

    @Override
    public List<Object> apply(ConsumerRecord<ByteBuffer, ByteBuffer> record) {
        return this.delegate.deserialize(record.value());
    }

    @Override
    public Fields getFieldsFor(String stream) {
        //We ensure there is only the default stream when configuring the spout, so it should be safe to ignore the parameter here.
        return this.delegate.getOutputFields();
    }
}

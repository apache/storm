/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.kinesis.spout.samples;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.tuple.Fields;

import com.amazonaws.services.kinesis.model.Record;
import org.apache.storm.kinesis.spout.IKinesisRecordScheme;

/**
 * Sample scheme for emitting Kinesis records as tuples. It emits a tuple of (partitionKey, sequenceNumber, and data).
 */
public class SampleKinesisRecordScheme implements IKinesisRecordScheme {
    private static final long serialVersionUID = 1L;
    /**
     * Name of the (partition key) value in the tuple.
     */
    public static final String FIELD_PARTITION_KEY = "partitionKey";
    
    /**
     * Name of the sequence number value in the tuple.
     */
    public static final String FIELD_SEQUENCE_NUMBER = "sequenceNumber";
    
    /**
     * Name of the Kinesis record data value in the tuple.
     */
    public static final String FIELD_RECORD_DATA = "recordData";

    /**
     * Constructor.
     */
    public SampleKinesisRecordScheme() {
    }


    @Override
    public List<Object> deserialize(Record record) {
        final List<Object> l = new ArrayList<>();
        l.add(record.getPartitionKey());
        l.add(record.getSequenceNumber());
        l.add(record.getData().array());
        return l;
    }


    @Override
    public Fields getOutputFields() {
        return new Fields(FIELD_PARTITION_KEY, FIELD_SEQUENCE_NUMBER, FIELD_RECORD_DATA);
    }
}

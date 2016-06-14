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

package org.apache.storm.kinesis.spout;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.tuple.Fields;

import com.amazonaws.services.kinesis.model.Record;

/**
 * Default scheme for emitting Kinesis records as tuples. It emits a tuple of (partitionKey, record).
 */
public class DefaultKinesisRecordScheme implements IKinesisRecordScheme {
    private static final long serialVersionUID = 1L;
    /**
     * Name of the (partition key) value in the tuple.
     */
    public static final String FIELD_PARTITION_KEY = "partitionKey";
    /**
     * Name of the (Kinesis record) value in the tuple.
     */
    public static final String FIELD_RECORD = "record";

    /**
     * Constructor.
     */
    public DefaultKinesisRecordScheme() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.amazonaws.services.kinesis.stormspout.IKinesisRecordScheme#deserialize(com.amazonaws.services.kinesis.model
     * .Record)
     */
    @Override
    public List<Object> deserialize(Record record) {
        final List<Object> l = new ArrayList<>();
        l.add(record.getPartitionKey());
        l.add(record);
        return l;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.amazonaws.services.kinesis.stormspout.IKinesisRecordScheme#getOutputFields()
     */
    @Override
    public Fields getOutputFields() {
        return new Fields(FIELD_PARTITION_KEY, FIELD_RECORD);
    }
}

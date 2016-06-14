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


import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import com.amazonaws.services.kinesis.model.Record;

public class SampleBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 177788290277634253L;
    private static final Logger LOG = LoggerFactory.getLogger(SampleBolt.class);
    private transient CharsetDecoder decoder;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        decoder = Charset.forName("UTF-8").newDecoder();
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String partitionKey = (String)input.getValueByField(SampleKinesisRecordScheme.FIELD_PARTITION_KEY);
        String sequenceNumber = (String)input.getValueByField(SampleKinesisRecordScheme.FIELD_SEQUENCE_NUMBER);
        byte[] payload = (byte[])input.getValueByField(SampleKinesisRecordScheme.FIELD_RECORD_DATA);
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        String data = null; 
        try {
            data = decoder.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            LOG.error("Exception when decoding record ", e);
        }
        LOG.info("SampleBolt got record: partitionKey=" + partitionKey + ", " + " sequenceNumber=" + sequenceNumber
                + ", data=" + data);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}

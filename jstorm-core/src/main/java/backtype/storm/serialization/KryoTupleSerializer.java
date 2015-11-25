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
package backtype.storm.serialization;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.BatchTuple;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleExt;
import backtype.storm.utils.Utils;

import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class KryoTupleSerializer implements ITupleSerializer {
    KryoValuesSerializer _kryo;
    SerializationFactory.IdDictionary _ids;
    Output _kryoOut;

    public KryoTupleSerializer(final Map conf, final GeneralTopologyContext context) {
        _kryo = new KryoValuesSerializer(conf);
        _kryoOut = new Output(2000, 2000000000);
        _ids = new SerializationFactory.IdDictionary(context.getRawTopology());
    }

    public byte[] serialize(Tuple tuple) {
        _kryoOut.clear();
        serializeTuple(_kryoOut, tuple);
        return _kryoOut.toBytes();
    }
    /**
     * @@@ in the furture, it will skill serialize 'targetTask' through check some flag
     * @see ITupleSerializer#serialize(int, Tuple)
     */
    private void serializeTuple(Output output, Tuple tuple) {
        try {
            if (tuple instanceof TupleExt) {
                output.writeInt(((TupleExt) tuple).getTargetTaskId());
                output.writeLong(((TupleExt) tuple).getCreationTimeStamp());
            }

            output.writeInt(tuple.getSourceTask(), true);
            output.writeInt(
                    _ids.getStreamId(tuple.getSourceComponent(),
                            tuple.getSourceStreamId()), true);
            tuple.getMessageId().serialize(output);
            _kryo.serializeInto(tuple.getValues(), output);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] serializeBatch(BatchTuple batch) {
        if (batch == null || batch.currBatchSize() == 0)
            return null;

        _kryoOut.clear();
        for (Tuple tuple : batch.getTuples()) {
            /* 
             * byte structure: 
             * 1st tuple: length + tuple bytes
             * 2nd tuple: length + tuple bytes
             * ......
             */
            int startPos = _kryoOut.position();
            
            // Set initial value of tuple length, which will be updated accordingly after serialization
            _kryoOut.writeInt(0);
            
            serializeTuple(_kryoOut, tuple);
            
            // Update the tuple length
            int endPos = _kryoOut.position();
            _kryoOut.setPosition(startPos);
            _kryoOut.writeInt(endPos - startPos - 4);
            _kryoOut.setPosition(endPos);
        }
        return _kryoOut.toBytes();
    }

    public static byte[] serialize(int targetTask) {
        ByteBuffer buff = ByteBuffer.allocate((Integer.SIZE / 8));
        buff.putInt(targetTask);
        byte[] rtn = buff.array();
        return rtn;
    }

    // public long crc32(Tuple tuple) {
    // try {
    // CRC32OutputStream hasher = new CRC32OutputStream();
    // _kryo.serializeInto(tuple.getValues(), hasher);
    // return hasher.getValue();
    // } catch (IOException e) {
    // throw new RuntimeException(e);
    // }
    // }
}

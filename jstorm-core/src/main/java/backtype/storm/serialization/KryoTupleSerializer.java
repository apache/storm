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
    
    /**
     * @@@ in the furture, it will skill serialize 'targetTask' through check some flag
     * @see backtype.storm.serialization.ITupleSerializer#serialize(int, backtype.storm.tuple.Tuple)
     */
    public byte[] serialize(Tuple tuple) {
        try {
            
            _kryoOut.clear();
            if (tuple instanceof TupleExt) {
                _kryoOut.writeInt(((TupleExt) tuple).getTargetTaskId());
            }
            
            _kryoOut.writeInt(tuple.getSourceTask(), true);
            _kryoOut.writeInt(_ids.getStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId()), true);
            tuple.getMessageId().serialize(_kryoOut);
            _kryo.serializeInto(tuple.getValues(), _kryoOut);
            return _kryoOut.toBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] serializeBatch(BatchTuple batch) {
        if (batch == null || batch.currBatchSize() == 0)
            return null;

        byte[][] bytes = new byte[batch.currBatchSize()][];
        int i = 0, len = 0;
        for (Tuple tuple : batch.getTuples()) {
            /* byte structure: 
             * 1st tuple: length + tuple bytes
             * 2nd tuple: length + tuple bytes
             * ......
             */
            bytes[i] = serialize(tuple);
            len += bytes[i].length;
            // add length bytes (int)
            len += 4;
            i++;
        }

        byte[] ret = new byte[len];
        int index = 0;
        for (i = 0; i < bytes.length; i++) {
            Utils.writeIntToByteArray(ret, index, bytes[i].length);
            index += 4;
            for (int j = 0; j < bytes[i].length; j++) {
                ret[index++] = bytes[i][j];
            }
        }
        return ret;
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

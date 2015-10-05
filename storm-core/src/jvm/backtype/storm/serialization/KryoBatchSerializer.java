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
import backtype.storm.tuple.Batch;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import com.esotericsoftware.kryo.io.Output;

import java.io.IOException;
import java.util.Map;

public class KryoBatchSerializer implements IBatchSerializer {
    KryoValuesSerializer _kryo;
    SerializationFactory.IdDictionary _ids;
    Output _kryoOut;

    public KryoBatchSerializer(final Map conf, final GeneralTopologyContext context) {
        _kryo = new KryoValuesSerializer(conf);
        _kryoOut = new Output(2000, 2000000000);
        _ids = new SerializationFactory.IdDictionary(context.getRawTopology());
    }

    public byte[] serialize(Batch batch) {
        final int size = batch.tupleBuffer.size();
        try {
            _kryoOut.clear();
            _kryoOut.writeByte('B');
            _kryoOut.writeInt(size);
            _kryoOut.writeInt(batch.sourceTaskId, true);
            _kryoOut.writeInt(_ids.getStreamId(batch.sourceComponent, batch.streamId), true);
            for(int i = 0; i < size; ++i) {
                batch.idBuffer.get(i).serialize(_kryoOut);
                _kryo.serializeInto(batch.tupleBuffer.get(i), _kryoOut);
            }
            return _kryoOut.toBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

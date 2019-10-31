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

package org.apache.storm.serialization;

import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import java.util.Map;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.Tuple;

public class KryoTupleSerializer implements ITupleSerializer {
    private KryoValuesSerializer kryo;
    private SerializationFactory.IdDictionary ids;
    private Output kryoOut;

    public KryoTupleSerializer(final Map<String, Object> conf, final GeneralTopologyContext context) {
        kryo = new KryoValuesSerializer(conf);
        kryoOut = new Output(2000, 2000000000);
        ids = new SerializationFactory.IdDictionary(context.getRawTopology());
    }

    @Override
    public byte[] serialize(Tuple tuple) {
        try {

            kryoOut.clear();
            kryoOut.writeInt(tuple.getSourceTask(), true);
            kryoOut.writeInt(ids.getStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId()), true);
            tuple.getMessageId().serialize(kryoOut);
            kryo.serializeInto(tuple.getValues(), kryoOut);
            return kryoOut.toBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

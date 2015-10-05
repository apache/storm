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
import java.util.Map;

public class KryoTupleBatchSerializer implements ITupleBatchSerializer {
    KryoTupleSerializer _kryoTuple;
    KryoBatchSerializer _kryoBatch;

    public KryoTupleBatchSerializer(final Map conf, final GeneralTopologyContext context) {
        _kryoTuple = new KryoTupleSerializer(conf, context);
        _kryoBatch = new KryoBatchSerializer(conf, context);
    }

    public byte[] serialize(TupleImpl tuple) {
        return _kryoTuple.serialize(tuple);
    }

    public byte[] serialize(Batch batch) {
        return _kryoBatch.serialize(batch);
    }

}

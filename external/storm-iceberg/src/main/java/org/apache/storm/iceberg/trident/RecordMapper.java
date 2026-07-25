/*
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

package org.apache.storm.iceberg.trident;

import java.io.Serializable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Converts a {@link TridentTuple} into an Iceberg {@link Record} matching the target table schema.
 * Implementations must be serializable: they are shipped with the topology.
 */
public interface RecordMapper extends Serializable {

    /**
     * Convert a tuple to a record for the given table schema.
     *
     * @param tuple  the input tuple
     * @param schema the current schema of the target Iceberg table
     * @return the record to write; never null
     */
    Record map(TridentTuple tuple, Schema schema);
}

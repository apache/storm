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
 *
 */

package org.apache.storm.sql.runtime;

import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;

/**
 * An ISqlStreamsDataSource specifies how an external data source produces and consumes data.
 */
public interface ISqlStreamsDataSource {
    /**
     * Provides instance of IRichSpout which can be used as producer in topology.
     *
     * @see org.apache.storm.topology.IRichSpout
     */
    IRichSpout getProducer();

    /**
     * Provides instance of IRichBolt which can be used as consumer in topology.
     */
    IRichBolt getConsumer();
}
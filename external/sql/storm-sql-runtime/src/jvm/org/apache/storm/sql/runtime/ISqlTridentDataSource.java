/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.runtime;

import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateUpdater;

/**
 * A ISqlTridentDataSource specifies how an external data source produces and consumes data.
 */
public interface ISqlTridentDataSource {
  /**
   * SqlTridentConsumer is a data structure containing StateFactory and StateUpdater for consuming tuples with State.
   *
   * Please note that StateFactory and StateUpdater should use same class which implements State.
   *
   * @see org.apache.storm.trident.state.StateFactory
   * @see org.apache.storm.trident.state.StateUpdater
   */
  interface SqlTridentConsumer {
    StateFactory getStateFactory();
    StateUpdater getStateUpdater();
  }

  /**
   * Provides instance of ITridentDataSource which can be used as producer in Trident.
   *
   * Since ITridentDataSource is a marker interface for Trident Spout interfaces, this method should effectively
   * return an instance of one of these interfaces (can be changed if Trident API evolves) or descendants:
   * - IBatchSpout
   * - ITridentSpout
   * - IPartitionedTridentSpout
   * - IOpaquePartitionedTridentSpout
   *
   * @see org.apache.storm.trident.spout.ITridentDataSource
   * @see org.apache.storm.trident.spout.IBatchSpout
   * @see org.apache.storm.trident.spout.ITridentSpout
   * @see org.apache.storm.trident.spout.IPartitionedTridentSpout
   * @see org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout
   */
  ITridentDataSource getProducer();

  /**
   * Provides instance of SqlTridentConsumer which can be used as consumer (State) in Trident.
   *
   * @see SqlTridentConsumer
   */
  SqlTridentConsumer getConsumer();
}

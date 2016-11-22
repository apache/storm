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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.cassandra.executor;

import com.datastax.driver.core.ResultSet;

import java.io.Serializable;

/**
 * Default handler for batch asynchronous execution.
 */
public interface AsyncResultSetHandler<T> extends Serializable {

    public static final AsyncResultSetHandler NO_OP_HANDLER = new AsyncResultSetHandler() {
        @Override
        public void failure(Throwable t, Object inputs) {
            /** no-operation **/
        }

        @Override
        public void success(Object inputs, ResultSet resultSet) {
            /** no-operation **/
        }

    };

    /**
     * This method is responsible for failing specified inputs.
     *
     * @param t The cause the failure.
     * @param inputs The input tuple proceed.
     */
    void failure(Throwable t, T inputs);

    /**
     * This method is responsible for acknowledging specified inputs.
     *
     * @param inputs The input tuple proceed.
     */
    void success(T inputs, ResultSet resultSet) ;

}

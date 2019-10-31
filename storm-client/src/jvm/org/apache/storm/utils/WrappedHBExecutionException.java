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

package org.apache.storm.utils;

import org.apache.storm.generated.HBExecutionException;

/**
 * Wraps the generated TException to allow getMessage() to return a valid string.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class WrappedHBExecutionException extends HBExecutionException {
    public WrappedHBExecutionException(String msg) {
        super(msg);
    }

    @Override
    public String getMessage() {
        return this.get_msg();
    }
}

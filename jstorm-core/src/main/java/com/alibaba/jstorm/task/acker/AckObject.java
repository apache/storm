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
package com.alibaba.jstorm.task.acker;

import com.alibaba.jstorm.utils.JStormUtils;

public class AckObject {
    public Long val = null;
    public Integer spout_task = null;
    public boolean failed = false;

    // val xor value
    public void update_ack(Object value) {
        synchronized (this) {
            if (val == null) {
                val = Long.valueOf(0);
            }
            val = JStormUtils.bit_xor(val, value);
        }
    }
}

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

package org.apache.storm.rocketmq;

import org.apache.storm.rocketmq.spout.scheme.StringScheme;

public class SpoutConfig extends RocketMqConfig {
    public static final String QUEUE_SIZE = "spout.queue.size";

    public static final String MESSAGES_MAX_RETRY = "spout.messages.max.retry";
    public static final int DEFAULT_MESSAGES_MAX_RETRY = 3;

    public static final String MESSAGES_TTL = "spout.messages.ttl";
    public static final int DEFAULT_MESSAGES_TTL = 300000;  // 5min

    public static final String SCHEME = "spout.scheme";
    public static final String DEFAULT_SCHEME = StringScheme.class.getName();

}

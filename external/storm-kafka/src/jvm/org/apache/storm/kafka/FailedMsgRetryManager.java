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
package org.apache.storm.kafka;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public interface FailedMsgRetryManager extends Serializable {

    /**
     * Initialization
     */
    void prepare(SpoutConfig spoutConfig, Map stormConf);

    /**
     * Message corresponding to the offset failed in kafka spout.
     * @param offset
     */
    void failed(Long offset);

    /**
     * Message corresponding to the offset, was acked to kafka spout.
     * @param offset
     */
    void acked(Long offset);

    /**
     * Message corresponding to the offset, has been re-emitted and under transit.
     * @param offset
     */
    void retryStarted(Long offset);

    /**
     * The offset of message, which is to be re-emitted. Spout will fetch messages starting from this offset
     * and resend them, except completed messages.
     * @return
     */
    Long nextFailedMessageToRetry();

    /**
     * @param offset
     * @return True if the message corresponding to the offset should be emitted NOW. False otherwise.
     */
    boolean shouldReEmitMsg(Long offset);

    /**
     * Spout will clean up the state for this offset if false is returned.
     * @param offset
     * @return True if the message will be retried again. False otherwise.
     */
    boolean retryFurther(Long offset);

    /**
     * Clear any offsets before kafkaOffset. These offsets are no longer available in kafka.
     * @param kafkaOffset
     * @return Set of offsets removed.
     */
    Set<Long> clearOffsetsBefore(Long kafkaOffset);
}

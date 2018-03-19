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

/**
 * A spout config for {@link ExponentialBackoffMsgRetryManager}.
 * @author richter
 */
public class ExponentialBackoffMsgRetryManagerSpoutConfig extends SpoutConfig {
    private static final long serialVersionUID = 1L;
    /**
     * The initial retry delay in milliseconds default.
     */
    private static final long RETRY_INITIAL_DELAY_MS_DEFAULT = 0;
    /**
     * The retry delay multiplier default.
     */
    private static final double RETRY_DELAY_MULTIPLIER_DEFAULT = 1.0;
    /**
     * The maximum retry delay default.
     */
    private static final long RETRY_DELAY_MAX_MS_DEFAULT = 60000;
    /**
     * The retry limit default.
     */
    private static final int RETRY_LIMIT_DEFAULT = -1;
    /**
     * The initial retry delay in milliseconds.
     */
    private final long retryInitialDelayMs;
    /**
     * The retry delay multiplier.
     */
    private final double retryDelayMultiplier;
    /**
     * The maximum retry delay.
     */
    private final long retryDelayMaxMs;
    /**
     * The retry limit.
     */
    private final int retryLimit;

    /**
     * Creates a new spout config.
     * @param hosts the hosts
     * @param topic the topic
     * @param zkRoot the Zookeeper root
     * @param id the id
     */
    public ExponentialBackoffMsgRetryManagerSpoutConfig(final BrokerHosts hosts,
            final String topic,
            final String zkRoot,
            final String id) {
//        this(RETRY_INITIAL_DELAY_MS_DEFAULT,
//                RETRY_DELAY_MULTIPLIER_DEFAULT,
//                RETRY_DELAY_MAX_MS_DEFAULT,
//                RETRY_LIMIT_DEFAULT,
//                hosts,
//                topic,
//                zkRoot,
//                id);
        super(hosts,
                topic,
                zkRoot,
                id);
        this.retryInitialDelayMs = RETRY_INITIAL_DELAY_MS_DEFAULT;
        this.retryDelayMultiplier = RETRY_DELAY_MULTIPLIER_DEFAULT;
        this.retryDelayMaxMs = RETRY_DELAY_MAX_MS_DEFAULT;
        this.retryLimit = RETRY_LIMIT_DEFAULT;
    }

    /**
     * Creates a new spout config.
     * @param hosts the hosts
     * @param topic the topic
     * @param clientId the client id
     * @param zkRoot the Zookeeper root
     * @param id the id
     */
    public ExponentialBackoffMsgRetryManagerSpoutConfig(final BrokerHosts hosts,
            final String topic,
            final String clientId,
            final String zkRoot,
            final String id) {
//        this(RETRY_INITIAL_DELAY_MS_DEFAULT,
//                RETRY_DELAY_MULTIPLIER_DEFAULT,
//                RETRY_DELAY_MAX_MS_DEFAULT,
//                RETRY_LIMIT_DEFAULT,
//                hosts,
//                topic,
//                clientId,
//                zkRoot,
//                id);
        super(hosts,
                topic,
                clientId,
                zkRoot,
                id);
        this.retryInitialDelayMs = RETRY_INITIAL_DELAY_MS_DEFAULT;
        this.retryDelayMultiplier = RETRY_DELAY_MULTIPLIER_DEFAULT;
        this.retryDelayMaxMs = RETRY_DELAY_MAX_MS_DEFAULT;
        this.retryLimit = RETRY_LIMIT_DEFAULT;
    }

    /**
     * Creates a new spout config.
     * @param retryInitialDelayMsArg the initial retry delay in milliseconds
     * @param retryDelayMultiplierArg the retry delay multiplier
     * @param retryDelayMaxMsArg the maximum retry delay
     * @param retryLimitArg the retry limit
     */
    public ExponentialBackoffMsgRetryManagerSpoutConfig(
            final long retryInitialDelayMsArg,
            final double retryDelayMultiplierArg,
            final long retryDelayMaxMsArg,
            final int retryLimitArg) {
        super(null,
                null,
                null,
                null);
        this.retryInitialDelayMs = retryInitialDelayMsArg;
        this.retryDelayMultiplier = retryDelayMultiplierArg;
        this.retryDelayMaxMs = retryDelayMaxMsArg;
        this.retryLimit = retryLimitArg;
    }

//    /**
//     * Creates a new spout config.
//     * @param retryInitialDelayMsArg the initial retry delay in milliseconds
//     * @param retryDelayMultiplierArg the retry delay multiplier
//     * @param retryDelayMaxMsArg the maximum retry delay
//     * @param retryLimitArg the retry limit
//     * @param hosts the hosts
//     * @param topic the topic
//     * @param zkRoot the Zookeeper root
//     * @param id the id
//     */
//    public ExponentialBackoffMsgRetryManagerSpoutConfig(
//            final long retryInitialDelayMsArg,
//            final double retryDelayMultiplierArg,
//            final long retryDelayMaxMsArg,
//            final int retryLimitArg,
//            final BrokerHosts hosts,
//            final String topic,
//            final String zkRoot,
//            final String id) {
//        super(hosts,
//                topic,
//                zkRoot,
//                id);
//        this.retryInitialDelayMs = retryInitialDelayMsArg;
//        this.retryDelayMultiplier = retryDelayMultiplierArg;
//        this.retryDelayMaxMs = retryDelayMaxMsArg;
//        this.retryLimit = retryLimitArg;
//    }
//
//    /**
//     * Creates a new spout config.
//     * @param retryInitialDelayMsArg the initial retry delay in milliseconds
//     * @param retryDelayMultiplierArg the retry delay multiplier
//     * @param retryDelayMaxMsArg the maximum retry delay
//     * @param retryLimitArg the retry limit
//     * @param hosts the hosts
//     * @param topic the topic
//     * @param clientId the client id
//     * @param zkRoot the Zookeeper root
//     * @param id the id
//     */
//    public ExponentialBackoffMsgRetryManagerSpoutConfig(
//            final long retryInitialDelayMsArg,
//            final double retryDelayMultiplierArg,
//            final long retryDelayMaxMsArg,
//            final int retryLimitArg,
//            final BrokerHosts hosts,
//            final String topic,
//            final String clientId,
//            final String zkRoot,
//            final String id) {
//        super(hosts,
//                topic,
//                clientId,
//                zkRoot,
//                id);
//        this.retryInitialDelayMs = retryInitialDelayMsArg;
//        this.retryDelayMultiplier = retryDelayMultiplierArg;
//        this.retryDelayMaxMs = retryDelayMaxMsArg;
//        this.retryLimit = retryLimitArg;
//    }

    /**
     * Gets the initial retry delay in milliseconds.
     * @return the retryInitialDelayMs
     */
    public long getRetryInitialDelayMs() {
        return retryInitialDelayMs;
    }

    /**
     * Gets the retry delay multiplier.
     * @return the retryDelayMultiplier
     */
    public double getRetryDelayMultiplier() {
        return retryDelayMultiplier;
    }

    /**
     * Gets the maximum retry delay in milliseconds.
     * @return the retryDelayMaxMs
     */
    public long getRetryDelayMaxMs() {
        return retryDelayMaxMs;
    }

    /**
     * Gets the retry limit.
     * @return the retryLimit
     */
    public int getRetryLimit() {
        return retryLimit;
    }
}

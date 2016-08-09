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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.druid.bolt;

import com.metamx.tranquility.tranquilizer.Tranquilizer;

import java.io.Serializable;

public class DruidConfig implements Serializable {

    public static final String DEFAULT_DISCARD_STREAM_ID = "druid-discard-stream";

    //Tranquilizer configs for DruidBeamBolt
    private int maxBatchSize;
    private int maxPendingBatches;
    private long lingerMillis;
    private boolean blockOnFull;
    private String discardStreamId;

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public int getMaxPendingBatches() {
        return maxPendingBatches;
    }

    public long getLingerMillis() {
        return lingerMillis;
    }

    public boolean isBlockOnFull() {
        return blockOnFull;
    }

    public String getDiscardStreamId() {
        return discardStreamId;
    }

    private DruidConfig(Builder builder) {
        this.maxBatchSize = builder.maxBatchSize;
        this.maxPendingBatches = builder.maxPendingBatches;
        this.lingerMillis = builder.lingerMillis;
        this.blockOnFull = builder.blockOnFull;
        this.discardStreamId = builder.discardStreamId;
    }

    public static DruidConfig.Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private int maxBatchSize = Tranquilizer.DefaultMaxBatchSize();
        private int maxPendingBatches = Tranquilizer.DefaultMaxPendingBatches();
        private long lingerMillis = Tranquilizer.DefaultLingerMillis();
        private boolean blockOnFull =  Tranquilizer.DefaultBlockOnFull();
        private String discardStreamId = null;

        public Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder maxPendingBatches(int maxPendingBatches) {
            this.maxPendingBatches = maxPendingBatches;
            return this;
        }

        public Builder lingerMillis(int lingerMillis) {
            this.lingerMillis = lingerMillis;
            return this;
        }

        public Builder blockOnFull(boolean blockOnFull) {
            this.blockOnFull = blockOnFull;
            return this;
        }

        public Builder discardStreamId(String discardStreamId) {
            this.discardStreamId = discardStreamId;
            return this;
        }

        public DruidConfig build() {
            return new DruidConfig(this);
        }
    }
}

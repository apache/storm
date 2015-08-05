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
package com.alibaba.jstorm.batch.util;

public class BatchDef {
    public static final String COMPUTING_STREAM_ID = "batch/compute-stream";

    public static final String PREPARE_STREAM_ID = "batch/parepare-stream";

    public static final String COMMIT_STREAM_ID = "batch/commit-stream";

    public static final String REVERT_STREAM_ID = "batch/revert-stream";

    public static final String POST_STREAM_ID = "batch/post-stream";

    public static final String SPOUT_TRIGGER = "spout_trigger";

    public static final String BATCH_ZK_ROOT = "batch";

    public static final String ZK_COMMIT_DIR = "/commit";

    public static final int ZK_COMMIT_RESERVER_NUM = 3;

    public static final String ZK_SEPERATOR = "/";

}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.localizer;

/**
 * Local Resource requested by the topology.
 */
public class LocalResource {
    private final boolean needsCallback;
    private final String blobKey;
    private final boolean uncompress;

    /**
     * Constructor.
     * @param keyname the key of the blob to download.
     * @param uncompress should the blob be uncompressed or not.
     * @param needsCallback if the blobs changes should a callback happen so the worker is restarted.
     */
    public LocalResource(String keyname, boolean uncompress, boolean needsCallback) {
        blobKey = keyname;
        this.uncompress = uncompress;
        this.needsCallback = needsCallback;
    }

    public String getBlobName() {
        return blobKey;
    }

    public boolean shouldUncompress() {
        return uncompress;
    }

    public boolean needsCallback() {
        return needsCallback;
    }

    @Override
    public String toString() {
        return "Key: " + blobKey + " uncompress: " + uncompress;
    }
}

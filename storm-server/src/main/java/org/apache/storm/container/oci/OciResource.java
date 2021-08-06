/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.storm.container.oci;

public class OciResource {
    private final String path;
    private final String fileName;
    private final long size;
    private final long timestamp;
    private final OciResourceType type;

    /**
     * Constructor.
     * @param path the path to the resource
     * @param fileName the filename of the resource
     * @param size the size of the resource
     * @param timestamp the modification time of the resource
     * @param type the type of the resource
     */
    public OciResource(String path, String fileName, long size, long timestamp, OciResourceType type) {
        this.path = path;
        this.fileName = fileName;
        this.size = size;
        this.timestamp = timestamp;
        this.type = type;
    }

    public String getPath() {
        return path;
    }

    public String getFileName() {
        return fileName;
    }

    public long getSize() {
        return size;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public OciResourceType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "OciResource{"
            + "Path=" + path
            + ", fileName='" + fileName + '\''
            + ", size=" + size
            + ", timestamp=" + timestamp
            + ", type=" + type
            + '}';
    }

    enum OciResourceType {
        LAYER, CONFIG
    }
}

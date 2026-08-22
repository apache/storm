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

package org.apache.storm.dependency;

import java.util.UUID;
import org.apache.storm.shade.com.google.common.io.Files;
import org.apache.storm.shade.org.apache.commons.lang3.StringUtils;

public class DependencyBlobStoreUtils {

    /**
     * The prefix every blob key holding a topology dependency starts with.
     */
    public static final String BLOB_DEPENDENCIES_PREFIX = "dep-";

    public static String generateDependencyBlobKey(String key) {
        return BLOB_DEPENDENCIES_PREFIX + key;
    }

    /**
     * Tell whether a blob key names a topology dependency, i.e. whether it could have been produced by
     * {@link #generateDependencyBlobKey(String)}. Keys that a topology only refers to, rather than owns, must be
     * checked with this before they are acted upon, because the dependency lists of a submitted topology are filled
     * in by the client and can name any blob at all.
     *
     * @param key the blob key to check, may be null
     * @return true if the key is a dependency blob key
     */
    public static boolean isDependencyBlobKey(String key) {
        return key != null && key.startsWith(BLOB_DEPENDENCIES_PREFIX);
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static String applyUUIDToFileName(String fileName) {
        String fileNameWithExt = Files.getNameWithoutExtension(fileName);
        String ext = Files.getFileExtension(fileName);
        if (StringUtils.isEmpty(ext)) {
            fileName = fileName + "-" + UUID.randomUUID();
        } else {
            fileName = fileNameWithExt + "-" + UUID.randomUUID() + "." + ext;
        }
        return fileName;
    }
}

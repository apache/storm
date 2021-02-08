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
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

public class DependencyBlobStoreUtils {

    private static final String BLOB_DEPENDENCIES_PREFIX = "dep-";

    public static String generateDependencyBlobKey(String key) {
        return BLOB_DEPENDENCIES_PREFIX + key;
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

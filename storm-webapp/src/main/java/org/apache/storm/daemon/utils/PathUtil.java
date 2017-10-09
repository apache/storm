/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.daemon.utils;

import java.nio.file.Path;

/**
 * Utility functions to make Path manipulation slightly less verbose.
 */
public class PathUtil {

    /**
     * Truncates path to the last numElements.
     * @param path The path to truncate.
     * @param numElements The number of elements to preserve at the end of the path.
     * @return The truncated path.
     */
    public static Path truncatePathToLastElements(Path path, int numElements) {
        if (path.getNameCount() <= numElements) {
            return path;
        }
        return path.subpath(path.getNameCount() - numElements, path.getNameCount());
    }
    
}

/*
 * Copyright 2018 The Apache Software Foundation.
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

package org.apache.storm.kafka.migration;

import java.util.Map;

public class MapUtil {

    /**
     * Get value for key. Error if value is null or not the expected type.
     */
    public static <T> T getOrError(Map<String, Object> conf, String key) {
        T ret = (T) conf.get(key);
        if (ret == null) {
            throw new RuntimeException(key + " cannot be null");
        }
        return ret;
    }
    
}

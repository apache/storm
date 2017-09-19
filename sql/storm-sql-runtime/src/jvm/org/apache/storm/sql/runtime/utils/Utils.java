/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.sql.runtime.utils;

import java.util.LinkedList;
import java.util.List;

public final class Utils {

    /**
     * This method for splitting string into parts by a delimiter.
     * It has higher performance than String.split(String regex).
     *
     * @param data need split string
     * @param delimiter the delimiter
     * @return string list
     */
    public static List<String> split(String data, char delimiter) {
        List<String> list = new LinkedList<>();
        //do not use .toCharArray avoid system copy
        StringBuilder sb = new StringBuilder(512);
        int len = data.length();
        for (int i = 0; i < len; i++) {
            char ch = data.charAt(i);
            if (ch == delimiter) {
                list.add(sb.toString());
                sb.setLength(0);
                if (i == len - 1) {
                    list.add("");
                }
            } else {
                sb.append(ch);
            }
        }
        if (sb.length() > 0) {
            list.add(sb.toString());
        }
        return list;
    }
}

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

package org.apache.storm.daemon.utils;

import java.util.List;
import java.util.stream.Collectors;

public class ListFunctionalSupport {
    public static <T> T first(List<T> list) {
        if (list == null || list.size() <= 0) {
            return null;
        }

        return list.get(0);
    }

    public static <T> List<T> takeLast(List<T> list, int count) {
        if (list == null) {
            return null;
        }

        if (list.size() <= count) {
            return list;
        } else {
            return list.stream()
                    .skip(list.size() - count)
                    .limit(count)
                    .collect(Collectors.toList());
        }
    }

    public static <T> List<T> drop(List<T> list, int count) {
        if (list == null) {
            return null;
        }

        return list.stream()
                .skip(count)
                .collect(Collectors.toList());
    }

    public static <T> List<T> rest(List<T> list) {
        return drop(list, 1);
    }

    public static <T> T last(List<T> list) {
        if (list == null || list.size() <= 0) {
            return null;
        }

        return list.get(list.size() - 1);
    }
}

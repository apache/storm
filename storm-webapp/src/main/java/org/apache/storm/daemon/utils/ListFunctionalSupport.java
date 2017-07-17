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

/**
 * Implements missing sequence functions in Java compared to Clojure.
 * To make thing simpler, it only supports List type.
 */
public class ListFunctionalSupport {
    /**
     * Get the first element in list.
     *
     * @param list list to get
     * @return the first element. null if list is null or empty.
     */
    public static <T> T first(List<T> list) {
        if (list == null || list.size() <= 0) {
            return null;
        }

        return list.get(0);
    }

    /**
     * get the last element in list.
     *
     * @param list list to get
     * @return the last element. null if list is null or empty.
     */
    public static <T> T last(List<T> list) {
        if (list == null || list.size() <= 0) {
            return null;
        }

        return list.get(list.size() - 1);
    }

    /**
     * Get the last N elements as a new list.
     *
     * @param list list to get
     * @param count element count to get
     * @return the first element. null if list is null.
     *         elements in a new list may be less than count if there're not enough elements in the list.
     */
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

    /**
     * Drop the first N elements and create a new list.
     *
     * @param list the list
     * @param count element count to drop
     * @return newly created sublist that drops the first N elements from origin list. null if list is null.
     */
    public static <T> List<T> drop(List<T> list, int count) {
        if (list == null) {
            return null;
        }

        return list.stream()
                .skip(count)
                .collect(Collectors.toList());
    }

    /**
     * Drop the only first element and create a new list. equivalent to drop(list, 1).
     *
     * @see {@link ListFunctionalSupport#drop(List, int)}
     * @param list the list
     * @return newly created sublist that drops the first element from origin list. null if list is null.
     */
    public static <T> List<T> rest(List<T> list) {
        return drop(list, 1);
    }

}

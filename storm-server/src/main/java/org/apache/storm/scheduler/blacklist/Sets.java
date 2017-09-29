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

package org.apache.storm.scheduler.blacklist;

import java.util.HashSet;
import java.util.Set;

public class Sets {

    /**
     * Calculate union of both sets.
     *
     * @param setA parameter 1
     * @param setB parameter 2
     * @param <T> generic type of Set elements.
     * @return the Set which is union of both Sets.
     */
    public static <T> Set<T> union(Set<T> setA, Set<T> setB) {
        Set<T> result = new HashSet<T>(setA);
        result.addAll(setB);
        return result;
    }

    /**
     * Calculate intersection of both sets.
     *
     * @param setA parameter 1
     * @param setB parameter 2
     * @param <T> generic type of Set elements.
     * @return the Set which is intersection of both Sets.
     */
    public static <T> Set<T> intersection(Set<T> setA, Set<T> setB) {
        Set<T> result = new HashSet<T>(setA);
        result.retainAll(setB);
        return result;
    }

    /**
     * Calculate difference of difference of two sets.
     *
     * @param setA parameter 1
     * @param setB parameter 2
     * @param <T> generic type of Set elements.
     * @return the Set which is difference of two sets.
     */
    public static <T> Set<T> difference(Set<T> setA, Set<T> setB) {
        Set<T> result = new HashSet<T>(setA);
        result.removeAll(setB);
        return result;
    }

    /**
     * Calculate symmetric difference of two sets.
     *
     * @param setA parameter 1
     * @param setB parameter 2
     * @param <T> generic type of Set elements.
     * @return the Set which is symmetric difference of two sets.
     */
    public static <T> Set<T> symDifference(Set<T> setA, Set<T> setB) {
        Set<T> union = union(setA, setB);
        Set<T> intersection = intersection(setA, setB);
        return difference(union, intersection);
    }

    /**
     * Check whether a set is a subset of another set.
     *
     * @param setA parameter 1
     * @param setB parameter 2
     * @param <T> generic type of Set elements.
     * @return true when setB is a subset of setA, false otherwise.
     */
    public static <T> boolean isSubset(Set<T> setA, Set<T> setB) {
        return setB.containsAll(setA);
    }

    /**
     * Check whether a set is a superset of another set.
     *
     * @param setA parameter 1
     * @param setB parameter 2
     * @param <T> generic type of Set elements.
     * @return true when setA is a superset of setB, false otherwise.
     */
    public static <T> boolean isSuperset(Set<T> setA, Set<T> setB) {
        return setA.containsAll(setB);
    }

}

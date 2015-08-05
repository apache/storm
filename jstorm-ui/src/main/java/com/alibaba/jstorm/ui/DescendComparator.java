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
package com.alibaba.jstorm.ui;

import java.util.Comparator;

public class DescendComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {

        if (o1 instanceof Double && o2 instanceof Double) {
            Double i1 = (Double) o1;
            Double i2 = (Double) o2;
            return -i1.compareTo(i2);
        } else if (o1 instanceof Integer && o2 instanceof Integer) {
            Integer i1 = (Integer) o1;
            Integer i2 = (Integer) o2;
            return -i1.compareTo(i2);
        } else {
            Double i1 = Double.valueOf(String.valueOf(o1));
            Double i2 = Double.valueOf(String.valueOf(o2));

            return -i1.compareTo(i2);
        }
    }
}

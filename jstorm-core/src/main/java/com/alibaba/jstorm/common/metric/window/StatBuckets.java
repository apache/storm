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
package com.alibaba.jstorm.common.metric.window;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class StatBuckets {

    public static final Integer NUM_STAT_BUCKETS = 20;

    public static final Integer MINUTE_WINDOW = 600;
    public static final Integer HOUR_WINDOW = 10800;
    public static final Integer DAY_WINDOW = 86400;
    public static final Integer ALL_TIME_WINDOW = 0;
    public static Set<Integer> TIME_WINDOWS = new TreeSet<Integer>();
    static {
        TIME_WINDOWS.add(ALL_TIME_WINDOW);
        TIME_WINDOWS.add(MINUTE_WINDOW);
        TIME_WINDOWS.add(HOUR_WINDOW);
        TIME_WINDOWS.add(DAY_WINDOW);
    }

    public static final String MINUTE_WINDOW_STR = "0d0h10m0s";
    public static final String HOUR_WINDOW_STR = "0d3h0m0s";
    public static final String DAY_WINDOW_STR = "1d0h0m0s";
    public static final String ALL_WINDOW_STR = "All-time";

    public static Integer[] STAT_BUCKETS = { MINUTE_WINDOW / NUM_STAT_BUCKETS,
            HOUR_WINDOW / NUM_STAT_BUCKETS, DAY_WINDOW / NUM_STAT_BUCKETS };

    private static final String[][] PRETTYSECDIVIDERS = {
            new String[] { "s", "60" }, new String[] { "m", "60" },
            new String[] { "h", "24" }, new String[] { "d", null } };

    /**
     * Service b
     * 
     * @param key
     * @return
     */
    public static String parseTimeKey(Integer key) {
        if (key == 0) {
            return ALL_WINDOW_STR;
        } else {
            return String.valueOf(key);
        }
    }

    /**
     * 
     * Default is the latest result
     * 
     * @param showKey
     * @return
     */
    public static Integer getTimeKey(String showKey) {
        Integer window = null;
        if (showKey == null) {
            window = (MINUTE_WINDOW);
        } else if (showKey.equals(MINUTE_WINDOW_STR)) {
            window = (MINUTE_WINDOW);
        } else if (showKey.equals(HOUR_WINDOW_STR)) {
            window = (HOUR_WINDOW);
        } else if (showKey.equals(DAY_WINDOW_STR)) {
            window = (DAY_WINDOW);
        } else if (showKey.equals(ALL_WINDOW_STR)) {
            window = ALL_TIME_WINDOW;
        } else {
            window = MINUTE_WINDOW;
        }

        return window;
    }

    /**
     * Default is the latest result
     * 
     * @param showStr
     * @return
     */
    public static String getShowTimeStr(Integer time) {
        if (time == null) {
            return MINUTE_WINDOW_STR;
        } else if (time.equals(MINUTE_WINDOW)) {
            return MINUTE_WINDOW_STR;
        } else if (time.equals(HOUR_WINDOW)) {
            return HOUR_WINDOW_STR;
        } else if (time.equals(DAY_WINDOW)) {
            return DAY_WINDOW_STR;
        } else if (time.equals(ALL_TIME_WINDOW)) {
            return ALL_WINDOW_STR;
        } else {
            return MINUTE_WINDOW_STR;
        }

    }

    /**
     * seconds to string like 1d20h30m40s
     * 
     * @param secs
     * @return
     */
    public static String prettyUptimeStr(int secs) {
        int diversize = PRETTYSECDIVIDERS.length;

        List<String> tmp = new ArrayList<String>();
        int div = secs;
        for (int i = 0; i < diversize; i++) {
            if (PRETTYSECDIVIDERS[i][1] != null) {
                Integer d = Integer.parseInt(PRETTYSECDIVIDERS[i][1]);
                tmp.add(div % d + PRETTYSECDIVIDERS[i][0]);
                div = div / d;
            } else {
                tmp.add(div + PRETTYSECDIVIDERS[i][0]);
            }
        }

        String rtn = "";
        int tmpSzie = tmp.size();
        for (int j = tmpSzie - 1; j > -1; j--) {
            rtn += tmp.get(j);
        }
        return rtn;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}

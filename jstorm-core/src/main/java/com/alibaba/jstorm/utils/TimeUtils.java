/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.utils;

import backtype.storm.utils.Time;
import com.alibaba.jstorm.metric.AsmWindow;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Time utils
 *
 * @author yannian
 */
public class TimeUtils {

    public static final long NS_PER_MS = 1000000L;

    /**
     * Take care of int overflow
     */
    public static int current_time_secs() {
        return (int) (Time.currentTimeMillis() / 1000);
    }

    /**
     * Take care of int overflow
     */
    public static int time_delta(int time_secs) {
        return current_time_secs() - time_secs;
    }

    public static long time_delta_ms(long time_ms) {
        return System.currentTimeMillis() - time_ms;
    }

    public static final long NS_PER_US = 1000l;

    public static final int ONE_SEC = 1;
    public static final int SEC_PER_MIN = 60;
    public static final int SEC_PER_DAY = 86400;

    public static boolean isTimeAligned() {
        return current_time_secs() % SEC_PER_DAY % SEC_PER_MIN == 0;
    }

    public static int secOffset() {
        return current_time_secs() % SEC_PER_DAY % SEC_PER_MIN;
    }

    public static int secOffset(long ts) {
        return (int) (ts % SEC_PER_DAY % SEC_PER_MIN);
    }

    public static int winSecOffset(long ts, int window) {
        return (int) (ts / 1000 % SEC_PER_DAY % window);
    }

    public static long alignTimeToWin(long ts, int win) {
        if (win != AsmWindow.D1_WINDOW) {
            long curTimeSec = ts / 1000;
            return (curTimeSec - curTimeSec % win) * 1000;
        } else {
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(ts);
            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONTH);
            int day = cal.get(Calendar.DAY_OF_MONTH);
            int hour = cal.get(Calendar.HOUR);
            int min = cal.get(Calendar.MINUTE);
            int sec = cal.get(Calendar.SECOND);
            if (sec + min + hour > 0) {
                cal.set(year, month, day + 1, 0, 0, 0);
            }
            return cal.getTimeInMillis();
        }
    }

    public static long alignTimeToMin(long ts) {
        return alignTimeToWin(ts, AsmWindow.M1_WINDOW);
    }

    public static String toTimeStr(Date time) {
        int hour = time.getHours();
        int min = time.getMinutes();
        StringBuilder sb = new StringBuilder();
        if (hour < 10) {
            sb.append(0).append(hour);
        } else {
            sb.append(hour);
        }
        sb.append(":");
        if (min < 10) {
            sb.append(0).append(min);
        } else {
            sb.append(min);
        }
        return sb.toString();
    }

    public static String format(int curTimeSec) {
        return format(new Date(curTimeSec * 1000L), "yyyy-MM-dd HH:mm:ss");
    }

    public static String format(Date time, String fmt) {
        SimpleDateFormat df = new SimpleDateFormat(fmt);
        return df.format(time);
    }


    public static void main(String[] args) throws Exception {
        System.out.println(new Date(alignTimeToWin(System.currentTimeMillis(), AsmWindow.M1_WINDOW)));
        System.out.println(new Date(alignTimeToWin(System.currentTimeMillis(), AsmWindow.M10_WINDOW)));
        System.out.println(new Date(alignTimeToWin(System.currentTimeMillis(), AsmWindow.H2_WINDOW)));
        System.out.println(new Date(alignTimeToWin(System.currentTimeMillis(), AsmWindow.D1_WINDOW)));

        Calendar cal = Calendar.getInstance();
        cal.set(2015, 6, 23, 0, 0, 0);
        System.out.println(new Date(alignTimeToWin(cal.getTimeInMillis(), AsmWindow.D1_WINDOW)));

        System.out.println(format(TimeUtils.current_time_secs()));
    }
}

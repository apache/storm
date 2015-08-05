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
package com.alibaba.jstorm.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author longda
 * 
 */
public class TimeFormat {
    public static Logger log = LoggerFactory.getLogger(TimeFormat.class);

    public static final long ONE_SECOND_MILLISECONDS = 1000;

    public static final long ONE_MINUTE_SECONDS = 60;

    public static final long ONE_HOUR_MINUTES = 60;

    public static final long ONE_DAY_HOURS = 24;

    public static final long ONE_MINUTE_MILLISECONDS = ONE_MINUTE_SECONDS
            * ONE_SECOND_MILLISECONDS;

    public static final long ONE_HOUR_MILLISECONDS = ONE_HOUR_MINUTES
            * ONE_MINUTE_MILLISECONDS;

    public static final long ONE_DAY_MILLISECONDS = ONE_DAY_HOURS
            * ONE_HOUR_MILLISECONDS;

    public static Date convertDate(String dateStr, String format) {
        Date date = null;
        try {
            if (format != null) {
                SimpleDateFormat simpleDateFormat =
                        new SimpleDateFormat(format);
                date = simpleDateFormat.parse(dateStr);
            } else {
                date = new Date(dateStr);
            }

        } catch (Exception ex) {
            log.error("Failed to convert " + dateStr + " to Date, format:"
                    + format);
            return null;
        }
        return date;
    }

    public static String convertStr(Date date, String format) {
        String ret = null;
        try {

            SimpleDateFormat sdf = new SimpleDateFormat(format);

            ret = sdf.format(date);

        } catch (Exception e) {
            log.error("Failed to convert " + date + " to String, format:"
                    + format);
            return null;
        }
        return ret;
    }

    public static Date getYear(String dateStr) {
        return convertDate(dateStr, "yyyy");
    }

    public static String getYear(Date date) {
        return convertStr(date, "yyyy");
    }

    public static Date getMonth(String dateStr) {
        return convertDate(dateStr, "yyyyMM");
    }

    public static String getMonth(Date date) {
        return convertStr(date, "yyyyMM");
    }

    public static Date getDay(String dateStr) {
        return convertDate(dateStr, "yyyyMMdd");
    }

    public static String getDay(Date date) {
        return convertStr(date, "yyyyMMdd");
    }

    public static Date getHour(String dateStr) {
        return convertDate(dateStr, "yyyyMMddHH");
    }

    public static String getHour(Date date) {
        return convertStr(date, "yyyyMMddHH");
    }

    public static Date getMinute(String dateStr) {
        return convertDate(dateStr, "yyyyMMddHHmm");
    }

    public static String getMinute(Date date) {
        return convertStr(date, "yyyyMMddHHmm");
    }

    public static Date getSecond(String dateStr) {
        return convertDate(dateStr, "yyyyMMddHHmmss");
    }

    public static String getSecond(Date date) {
        return convertStr(date, "yyyyMMddHHmmss");
    }

    public static String getHourMin(String dateStr) {
        Date date = convertDate(dateStr, null);
        if (date == null) {
            return null;
        }

        return getHourMin(date);
    }

    public static String getHourMin(Date date) {
        String output = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm");
            output = sdf.format(date);
        } catch (Exception e) {
            return null;
        }
        return output;
    }

    public static Date getToday() {
        Date now = new Date();

        String todayStr = getDay(now);

        return getDay(todayStr);
    }

    public static Date getYesterday() {
        Date now = new Date();

        Calendar yesterdayCal = Calendar.getInstance();
        yesterdayCal.setTime(now);
        yesterdayCal.add(Calendar.DATE, -1);

        String yesterdayStr = getDay(yesterdayCal.getTime());

        return getDay(yesterdayStr);
    }

    /**
     * get the days number pass from 1970-00-00
     * 
     * @return
     */
    public static long getDayNum(Date date) {
        long passMs = date.getTime() + (8 * 1000 * 60 * 60);

        return (passMs / 1000 / 60 / 60 / 24);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        Date date = new Date();

        String dateStr = getDay(date);

        Date newDate = getDay(dateStr);

        System.out.println("new date:" + newDate);

        Date current = new Date();
        Calendar tomorrow = Calendar.getInstance();

        tomorrow.setTime(current);
        tomorrow.add(Calendar.DATE, 1);
        tomorrow.set(Calendar.AM_PM, Calendar.AM);
        tomorrow.set(Calendar.HOUR, 2);
        tomorrow.set(Calendar.MINUTE, 0);
        Date startTime = tomorrow.getTime();

        long hourdiff =
                (startTime.getTime() - current.getTime())
                        / ONE_HOUR_MILLISECONDS;

        System.out.println("Current:" + current + ", tomorrow" + startTime
                + ", diff hour" + hourdiff);

    }

}

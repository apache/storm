/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.st.utils;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TimeUtil {
    private static Logger log = LoggerFactory.getLogger(TimeUtil.class);

    public static void sleepSec(int sec) {
        try {
            TimeUnit.SECONDS.sleep(sec);
        } catch (InterruptedException e) {
            log.warn("Caught exception: " + ExceptionUtils.getFullStackTrace(e));
        }
    }
    public static void sleepMilliSec(int milliSec) {
        try {
            TimeUnit.MILLISECONDS.sleep(milliSec);
        } catch (InterruptedException e) {
            log.warn("Caught exception: " + ExceptionUtils.getFullStackTrace(e));
        }
    }

    public static DateTime floor(DateTime dateTime, int sec) {
        long modValue = dateTime.getMillis() % (1000 * sec);
        return dateTime.minus(modValue);
    }

    public static DateTime ceil(DateTime dateTime, int sec) {
        long modValue = dateTime.getMillis() % (1000 * sec);
        return dateTime.minus(modValue).plusSeconds(sec);
    }
}

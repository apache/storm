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

package org.apache.storm.st.wrapper;

import org.apache.storm.st.utils.AssertUtil;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.apache.storm.st.utils.StringDecorator;

import java.util.Arrays;
import java.util.List;

/**
 * Convenience class splitting log lines decorated with {@link StringDecorator}.
 * This provides easy access to the log line timestamp and data.
 */
public class DecoratedLogLine implements Comparable<DecoratedLogLine> {
    private final DateTime logDate;
    private final String data;
    private static final int DATE_LEN = "2016-05-04 23:38:10.702".length(); //format of date in worker logs
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public DecoratedLogLine(String logLine) {
        final List<String> splitOnDecorator = Arrays.asList(StringDecorator.split2(StringUtils.strip(logLine)));
        AssertUtil.assertTwoElements(splitOnDecorator);
        this.logDate = DATE_FORMAT.parseDateTime(splitOnDecorator.get(0).substring(0, DATE_LEN));
        this.data = splitOnDecorator.get(1);
    }

    @Override
    public String toString() {
        return "LogData{" +
                "logDate=" + DATE_FORMAT.print(logDate) +
                ", data='" + getData() + '\'' +
                '}';
    }

    @Override
    public int compareTo(DecoratedLogLine that) {
        return this.logDate.compareTo(that.logDate);
    }

    public String getData() {
        return data;
    }

    public DateTime getLogDate() {
        return logDate;
    }
}

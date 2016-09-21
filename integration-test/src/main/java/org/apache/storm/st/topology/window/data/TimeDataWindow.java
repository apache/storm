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

package org.apache.storm.st.topology.window.data;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.gson.reflect.TypeToken;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class TimeDataWindow extends ArrayList<TimeData> implements FromJson<TimeDataWindow> {
    public static final TimeDataWindow CLS = new TimeDataWindow();
    private static final Type listType = new TypeToken<List<TimeData>>() {}.getType();

    private TimeDataWindow() {
    }

    private TimeDataWindow(List<TimeData> data) {
        super(data);
    }

    public static TimeDataWindow newInstance(Collection<TimeData> data) {
        final List<TimeData> dataCopy = new ArrayList<>(data);
        Collections.sort(dataCopy);
        return new TimeDataWindow(dataCopy);
    }

    public static TimeDataWindow newInstance(Collection<TimeData> data, Predicate<TimeData> predicate) {
        return newInstance(Collections2.filter(data, predicate));
    }

    public static TimeDataWindow newInstance(Collection<TimeData> data, final DateTime fromDate, final DateTime toDate) {
        return TimeDataWindow.newInstance(data, new Predicate<TimeData>() {
            @Override
            public boolean apply(@Nullable TimeData input) {
                if (input == null) {
                    return false;
                }
                final DateTime inputDate = new DateTime(input.getDate());
                return inputDate.isAfter(fromDate) && inputDate.isBefore(toDate.plusMillis(1));
            }
        });
    }

    public TimeData first() {
        return get(0);
    }

    public TimeData last() {
        return get(size()-1);
    }

    public String getDescription() {
        final int size = size();
        if (size > 0) {
            final TimeData first = first();
            final TimeData last = last();
            return "Total " + size + " items: " + first + " to " + last;
        }
        return "Total " + size + " items.";
    }

    public TimeDataWindow fromJson(String jsonStr) {
        final List<TimeData> dataList = TimeData.gson.fromJson(jsonStr, listType);
        Collections.sort(dataList);
        return TimeDataWindow.newInstance(dataList);
    }


}

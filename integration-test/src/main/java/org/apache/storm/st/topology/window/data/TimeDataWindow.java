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

import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class TimeDataWindow {
    private static final Type LIST_TYPE = new TypeToken<List<TimeData>>() {}.getType(); 
    
    private final TreeSet<TimeData> data;

    public TimeDataWindow(List<TimeData> data) {
        this.data = new TreeSet<>(data);
    }

    public String getDescription() {
        final int size = data.size();
        if (size > 0) {
            final TimeData first = data.first();
            final TimeData last = data.last();
            return "Total " + size + " items: " + first + " to " + last;
        }
        return "Total " + size + " items.";
    }
    
    public TreeSet<TimeData> getTimeData() {
        return data;
    }

    public static TimeDataWindow fromJson(String jsonStr) {
        final List<TimeData> dataList = TimeData.GSON.fromJson(jsonStr, LIST_TYPE);
        return new TimeDataWindow(dataList);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
            .append(data)
            .toString();
    }
}

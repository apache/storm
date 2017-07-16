/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.sql.runtime.calcite;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Holder;

/**
 * This is based on SlimDataContext in Calcite, and borrow some from DataContextImpl in Calcite.
 */
public class StormDataContext implements DataContext, Serializable {
    private final ImmutableMap<Object, Object> map;

    /**
     * StormDataContext Constructor.
     */
    public StormDataContext() {
        // Store the time at which the query started executing. The SQL
        // standard says that functions such as CURRENT_TIMESTAMP return the
        // same value throughout the query.

        final Holder<Long> timeHolder = Holder.of(System.currentTimeMillis());

        // Give a hook chance to alter the clock.
        Hook.CURRENT_TIME.run(timeHolder);
        final long time = timeHolder.get();
        final TimeZone timeZone = Calendar.getInstance().getTimeZone();
        final long localOffset = timeZone.getOffset(time);
        final long currentOffset = localOffset;

        ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
        builder.put(Variable.UTC_TIMESTAMP.camelName, time)
                .put(Variable.CURRENT_TIMESTAMP.camelName, time + currentOffset)
                .put(Variable.LOCAL_TIMESTAMP.camelName, time + localOffset)
                .put(Variable.TIME_ZONE.camelName, timeZone);
        map = builder.build();
    }

    @Override
    public SchemaPlus getRootSchema() {
        return null;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return null;
    }

    @Override
    public QueryProvider getQueryProvider() {
        return null;
    }

    @Override
    public Object get(String name) {
        return map.get(name);
    }
}

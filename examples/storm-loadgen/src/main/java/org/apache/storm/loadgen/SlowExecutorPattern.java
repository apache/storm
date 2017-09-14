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

package org.apache.storm.loadgen;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.storm.utils.ObjectReader;

/**
 * A repeating pattern of skewedness in processing times.  This is used to simulate an executor that slows down.
 */
public class SlowExecutorPattern implements Serializable {
    private static final Pattern PARSER = Pattern.compile("\\s*(?<slowness>[^:]+)\\s*(?::\\s*(?<count>[0-9]+))?\\s*");
    public final double maxSlownessMs;
    public final int count;

    /**
     * Parses a string (command line) representation of "&lt;SLOWNESS&gt;(:&lt;COUNT&gt;)?".
     * @param strRepresentation the string representation to parse
     * @return the corresponding SlowExecutorPattern.
     */
    public static SlowExecutorPattern fromString(String strRepresentation) {
        Matcher m = PARSER.matcher(strRepresentation);
        if (!m.matches()) {
            throw new IllegalArgumentException(strRepresentation + " is not in the form <SLOWNESS>(:<COUNT>)?");
        }
        double slownessMs = Double.valueOf(m.group("slowness"));
        String c = m.group("count");
        int count = c == null ? 1 : Integer.valueOf(c);
        return new SlowExecutorPattern(slownessMs, count);
    }

    /**
     * Creates a SlowExecutorPattern from a Map config.
     * @param conf the conf to parse.
     * @return the corresponding SlowExecutorPattern.
     */
    public static SlowExecutorPattern fromConf(Map<String, Object> conf) {
        double slowness = ObjectReader.getDouble(conf.get("slownessMs"), 0.0);
        int count = ObjectReader.getInt(conf.get("count"), 1);
        return new SlowExecutorPattern(slowness, count);
    }

    /**
     * Convert this to a Config map.
     * @return the corresponding Config map to this.
     */
    public Map<String, Object> toConf() {
        Map<String, Object> ret = new HashMap<>();
        ret.put("slownessMs", maxSlownessMs);
        ret.put("count", count);
        return ret;
    }

    public SlowExecutorPattern(double maxSlownessMs, int count) {
        this.count = count;
        this.maxSlownessMs = maxSlownessMs;
    }

    public double getExtraSlowness(int index) {
        return (index >= count) ? 0 : maxSlownessMs;
    }

}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.command;

import java.util.Map;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SetLogLevelTest {

    @Test
    public void testUpdateLogLevelParser() {
        SetLogLevel.LogLevelsParser logLevelsParser = new SetLogLevel.LogLevelsParser(LogLevelAction.UPDATE);
        LogLevel logLevel = ((Map<String, LogLevel>) logLevelsParser.parse("com.foo.one=warn")).get("com.foo.one");
        assertEquals(0, logLevel.get_reset_log_level_timeout_secs());
        assertEquals("WARN", logLevel.get_target_log_level());

        logLevel = ((Map<String, LogLevel>) logLevelsParser.parse("com.foo.two=DEBUG:10")).get("com.foo.two");
        assertEquals(10, logLevel.get_reset_log_level_timeout_secs());
        assertEquals("DEBUG", logLevel.get_target_log_level());
    }

    @Test
    public void testInvalidTimeout() {
        SetLogLevel.LogLevelsParser logLevelsParser = new SetLogLevel.LogLevelsParser(LogLevelAction.UPDATE);
        assertThrows(NumberFormatException.class, () -> logLevelsParser.parse("com.foo.bar=warn:NaN"));
    }

    @Test
    public void testInvalidLogLevel() {
        SetLogLevel.LogLevelsParser logLevelsParser = new SetLogLevel.LogLevelsParser(LogLevelAction.UPDATE);
        assertThrows(IllegalArgumentException.class, () -> logLevelsParser.parse("com.foo.bar=CRITICAL"));
    }

}

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

package org.apache.storm.trident.operation.builtin;

import java.util.Date;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter for debugging purposes. The `isKeep()` method simply prints the tuple to `System.out` and returns `true`.
 */
public class Debug extends BaseFilter {
    private static final Logger LOG = LoggerFactory.getLogger(Debug.class);

    private final String name;
    private boolean useLogger;

    public Debug() {
        this(false);
    }

    public Debug(boolean useLogger) {
        this.useLogger = useLogger;
        this.name = "DEBUG: ";
    }

    /**
     * Creates a `Debug` filter with a string identifier.
     */
    public Debug(String name) {
        this.name = "DEBUG(" + name + "): ";
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        if (useLogger) {
            LOG.debug(tuple.toString());
        } else {
            System.out.println("<" + new Date() + "> " + name + tuple.toString());
        }
        return true;
    }
}

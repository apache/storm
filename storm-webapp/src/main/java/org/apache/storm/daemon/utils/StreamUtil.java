/*
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

package org.apache.storm.daemon.utils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Utility class for (Input/Output)Stream.
 */
public class StreamUtil {
    private StreamUtil() {
    }

    /**
     * Skips over and discards N bytes of data from the input stream.
     * <p/>
     * FileInputStream#skip may not work the first time, so ensure it successfully skips the given number of bytes.
     *
     * @see {@link java.io.FileInputStream#skip(long)}
     * @param stream the stream to skip
     * @param n bytes to skip
     */
    public static void skipBytes(InputStream stream, Integer n) throws IOException {
        long skipped = 0;
        do {
            skipped = skipped + stream.skip(n - skipped);
        } while (skipped < n);
    }

}

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

package org.apache.storm.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class UtilsTest {

    private void doParseJvmHeapMemByChildOptsTest(String message, String opt, double expected) {
        doParseJvmHeapMemByChildOptsTest(message, Arrays.asList(opt), expected);
    }
    
    private void doParseJvmHeapMemByChildOptsTest(String message, List<String> opts, double expected) {
        Assert.assertEquals(
                message,
                Utils.parseJvmHeapMemByChildOpts(opts, 123.0).doubleValue(), expected, 0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestK() {
        doParseJvmHeapMemByChildOptsTest("Xmx1024k results in 1 MB", "Xmx1024k", 1.0);
        doParseJvmHeapMemByChildOptsTest("Xmx1024K results in 1 MB", "Xmx1024K", 1.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx1024k results in 1 MB", "-Xmx1024k", 1.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestM() {
        doParseJvmHeapMemByChildOptsTest("Xmx100M results in 100 MB", "Xmx100m", 100.0);
        doParseJvmHeapMemByChildOptsTest("Xmx100m results in 100 MB", "Xmx100M", 100.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx100M results in 100 MB", "-Xmx100m", 100.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestG() {
        doParseJvmHeapMemByChildOptsTest("Xmx1g results in 1024 MB", "Xmx1g", 1024.0);
        doParseJvmHeapMemByChildOptsTest("Xmx1G results in 1024 MB", "Xmx1G", 1024.0);
        doParseJvmHeapMemByChildOptsTest("-Xmx1g results in 1024 MB", "-Xmx1g", 1024.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestNoMatch() {
        doParseJvmHeapMemByChildOptsTest("Unmatched unit results in default", "Xmx1t", 123.0);
        doParseJvmHeapMemByChildOptsTest("Unmatched option results in default", "Xms1g", 123.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestNulls() {
        doParseJvmHeapMemByChildOptsTest("Null value results in default", (String) null, 123.0);
        doParseJvmHeapMemByChildOptsTest("Null list results in default", (List<String>) null, 123.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestExtraChars() {
        doParseJvmHeapMemByChildOptsTest("Leading characters are ignored", "---Xmx1g", 1024.0);
        doParseJvmHeapMemByChildOptsTest("Trailing characters are ignored", "Xmx1ggggg", 1024.0);
    }
    
    @Test
    public void parseJvmHeapMemByChildOptsTestFirstMatch() {
        doParseJvmHeapMemByChildOptsTest("First valid match is used", 
                Arrays.asList(null, "Xmx1t", "Xmx1g", "Xms1024k Xmx1024k", "Xmx100m"), 
                1024.0);
    }
}
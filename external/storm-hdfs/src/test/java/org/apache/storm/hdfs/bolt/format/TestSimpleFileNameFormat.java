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

package org.apache.storm.hdfs.bolt.format;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

public class TestSimpleFileNameFormat {

    @Test
    public void testDefaults() {
        Map<String, Object> topoConf = new HashMap();
        SimpleFileNameFormat format = new SimpleFileNameFormat();
        format.prepare(null, createTopologyContext(topoConf));
        long now = System.currentTimeMillis();
        String path = format.getPath();
        String name = format.getName(1, now);

        Assert.assertEquals("/storm", path);
        String time = new SimpleDateFormat("yyyyMMddHHmmss").format(now);
        Assert.assertEquals(time + ".1.txt", name);
    }

    @Test
    public void testParameters() {
        SimpleFileNameFormat format = new SimpleFileNameFormat()
            .withName("$TIME.$HOST.$COMPONENT.$TASK.$NUM.txt")
            .withPath("/mypath")
            .withTimeFormat("yyyy-MM-dd HH:mm:ss");
        Map<String, Object> topoConf = new HashMap();
        format.prepare(null, createTopologyContext(topoConf));
        long now = System.currentTimeMillis();
        String path = format.getPath();
        String name = format.getName(1, now);

        Assert.assertEquals("/mypath", path);
        String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(now);
        String host = null;
        try {
            host = Utils.localHostname();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(time + "." + host + ".Xcom.7.1.txt", name);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTimeFormat() {
        Map<String, Object> topoConf = new HashMap();
        SimpleFileNameFormat format = new SimpleFileNameFormat()
            .withTimeFormat("xyz");
        format.prepare(null, createTopologyContext(topoConf));
    }

    private TopologyContext createTopologyContext(Map<String, Object> topoConf) {
        Map<Integer, String> taskToComponent = new HashMap<>();
        taskToComponent.put(7, "Xcom");
        return new TopologyContext(null, topoConf, taskToComponent, null, null, null, null, null, null, 7, 6703, null, null, null, null,
                                   null, null, null);
    }
}

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
import org.apache.storm.generated.Nimbus;
import org.apache.storm.utils.NimbusClient;

public class Monitor {

    public static void main(String[] args) throws Exception {
        Map<String, Object> cl = CLI.opt("i", "interval", 4, CLI.AS_INT)
                                    .opt("m", "component", null)
                                    .opt("s", "stream", "default")
                                    .opt("w", "watch", "emitted")
                                    .arg("topologyName", CLI.FIRST_WINS)
                                    .parse(args);
        final org.apache.storm.utils.Monitor monitor = new org.apache.storm.utils.Monitor();
        Integer interval = (Integer) cl.get("i");
        if (null != interval) {
            monitor.setInterval(interval);
        }
        String component = (String) cl.get("m");
        if (null != component) {
            monitor.setComponent(component);
        }
        String stream = (String) cl.get("s");
        if (null != stream) {
            monitor.setStream(stream);
        }
        String watch = (String) cl.get("w");
        if (null != watch) {
            monitor.setWatch(watch);
        }
        String topologyName = (String) cl.get("topologyName");
        if (null != topologyName) {
            monitor.setTopology(topologyName);
        }

        NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
            @Override
            public void run(Nimbus.Iface nimbus) throws Exception {
                monitor.metrics(nimbus);
            }
        });
    }
}

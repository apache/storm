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
package com.alipay.dw.jstorm.example.drpc;

import java.util.Map;

import backtype.storm.utils.DRPCClient;
import backtype.storm.utils.Utils;


public class TestReachTopology {
    
    /**
     * @param args
     * @throws DRPCExecutionException 
     * @throws TException 
     */
    public static void main(String[] args) throws Exception {
        
        if (args.length < 1) {
            throw new IllegalArgumentException("Invalid parameter");
        }
        Map conf = Utils.readStormConfig();
        //"foo.com/blog/1" "engineering.twitter.com/blog/5"
        DRPCClient client = new DRPCClient(conf, args[0], 4772);
        String result = client.execute(ReachTopology.TOPOLOGY_NAME, "tech.backtype.com/blog/123");
        
        System.out.println("\n!!! Drpc result:" + result);
    }
    
}

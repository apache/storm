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
package backtype.storm;

/**
 * A tool abstract class that supports handling of generic command-line options.
 * 
 * <p>
 * Here is how a typical <code>Tool</code> is implemented:
 * </p>
 * <p>
 * <blockquote>
 * 
 * <pre>
 *     public class TopologyApp extends Tool {
 *         {@literal @}Override
 *         public int run(String[] args) throws Exception {
 *             // Config processed by ToolRunner
 *             Config conf = getConf();
 * 
 *             // Other setups go here
 *             String name = "topology";
 *             StormTopology topology = buildTopology(args);
 *             StormSubmitter.submitTopology(name, conf, topology);
 *             return 0;
 *         }
 * 
 *         StormTopology buildTopology(String[] args) { ... }
 * 
 *         public static void main(String[] args) throws Exception {
 *             // Use ToolRunner to handle generic command-line options
 *             ToolRunner.run(new TopologyApp(), args);
 *         }
 *     }
 * </pre>
 * 
 * </blockquote>
 * </p>
 * 
 * @see GenericOptionsParser
 * @see ToolRunner
 */

public abstract class Tool {
    Config config;
    
    public abstract int run(String[] args) throws Exception;
    
    public Config getConf() {
        return config;
    }
    
    public void setConf(Config config) {
        this.config = config;
    }
}

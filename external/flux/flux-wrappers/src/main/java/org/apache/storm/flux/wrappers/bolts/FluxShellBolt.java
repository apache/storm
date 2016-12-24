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
package org.apache.storm.flux.wrappers.bolts;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A generic `ShellBolt` implementation that allows you specify output fields
 * and even streams without having to subclass `ShellBolt` to do so.
 *
 */
public class FluxShellBolt extends ShellBolt implements IRichBolt{
    private Map<String, String[]> outputFields;
    private Map<String, Object> componentConfig;
    
    /**
     * Create a ShellBolt with command line arguments
     * @param command Command line arguments for the bolt
     */
    public FluxShellBolt(String[] command){
        super(command);
        this.outputFields = new HashMap<String, String[]>();
    }

    /**
     * Create a ShellBolt with command line arguments and output fields
     * 
     * Keep this constructor for backward compatibility.
     * 
     * @param command Command line arguments for the bolt
     * @param outputFields Names of fields the bolt will emit (if any).
     */
    public FluxShellBolt(String[] command, String[] outputFields){
        this(command);
        this.setDefaultStream(outputFields);
    }

    /**
     * Add configuration for this bolt. This method is called from YAML file:
     *
     * ```
     * className: "org.apache.storm.flux.wrappers.bolts.FluxShellBolt"
     * constructorArgs:
     * # command line
     * - ["python", "splitsentence.py"]
     * # output fields
     * - ["word"]
     * configMethods:
     * - name: "addComponentConfig"
     *   args: ["publisher.data_paths", "actions"]
     * ```
     *
     * @param key
     * @param value
     */
    public void addComponentConfig(String key, Object value) {
        if (this.componentConfig == null) {
            this.componentConfig = new HashMap<String, Object>();
        }
        this.componentConfig.put(key, value);
    }

    /**
     * Add configuration for this bolt. This method is called from YAML file:
     *
     * ```
     * className: "org.apache.storm.flux.wrappers.bolts.FluxShellBolt"
     * constructorArgs:
     * # command line
     * - ["python", "splitsentence.py"]
     * # output fields
     * - ["word"]
     * configMethods:
     * - name: "addComponentConfig"
     *   args:
     *   - "publisher.data_paths"
     *   - ["actions"]
     * ```
     *
     * @param key
     * @param values
     */
    public void addComponentConfig(String key, List<Object> values) {
        if (this.componentConfig == null) {
            this.componentConfig = new HashMap<String, Object>();
        }
        this.componentConfig.put(key, values);
    }

    /**
     * Set default stream outputFields, this method is called from YAML file:
     * 
     * ```
     * bolts:
     * - className: org.apache.storm.flux.wrappers.bolts.FluxShellBolt
     *   id: my_bolt
     *   constructorArgs:
     *   - [python, my_bolt.py]
     *   configMethods:
     *   - name: setDefaultStream
     *     args:
     *     - [word, count]
     * ```
     * 
     * @param outputFields Names of fields the bolt will emit (if any) in default stream.
     */
    public void setDefaultStream(String[] outputFields) {
        this.setNamedStream("default", outputFields);
    }

    /**
     * Set custom *named* stream outputFields, this method is called from YAML file:
     * 
     * ```
     * bolts:
     * - className: org.apache.storm.flux.wrappers.bolts.FluxShellBolt
     *   id: my_bolt
     *   constructorArgs:
     *   - [python, my_bolt.py]
     *   configMethods:
     *   - name: setNamedStream
     *     args:
     *     - first
     *     - [word, count]
     * ```
     * @param name Name of stream the bolt will emit into.
     * @param outputFields Names of fields the bolt will emit in custom *named* stream.
     */
    public void setNamedStream(String name, String[] outputFields) {
        this.outputFields.put(name, outputFields);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Iterator it = this.outputFields.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entryTuple = (Map.Entry)it.next();
            String key = (String)entryTuple.getKey();
            String[] value = (String[])entryTuple.getValue();
            if(key.equals("default")) {
                declarer.declare(new Fields(value));
            } else {
                declarer.declareStream(key, new Fields(value));
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return this.componentConfig;
    }
}

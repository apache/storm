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

package org.apache.storm.topology;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.shade.org.yaml.snakeyaml.constructor.SafeConstructor;
import org.apache.storm.utils.Utils;

/**
 * Extensions of this class takes a reference to one or more configuration files. The main() method should call ConfigurableTopology.start()
 * and it must instantiate a TopologyBuilder in the run() method.
 *
 * <pre>
 * {
 *    public class MyTopology extends ConfigurableTopology {
 *
 *   public static void main(String[] args) throws Exception {
 *       ConfigurableTopology.start(new MyTopology(), args);
 *   }
 *
 *   &#64;Override
 *   protected int run(String[] args) {
 *       TopologyBuilder builder = new TopologyBuilder();
 *
 *       // build topology as usual
 *
 *       return submit("crawl", conf, builder);
 *   }
 * }
 * </pre>
 **/
public abstract class ConfigurableTopology {

    protected Config conf = new Config();

    public static void start(ConfigurableTopology topology, String args[]) {
        String[] remainingArgs = topology.parse(args);
        try {
            topology.run(remainingArgs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Config loadConf(String resource, Config conf)
        throws IOException {
        Yaml yaml = new Yaml(new SafeConstructor());
        try (Reader reader = Files.newBufferedReader(Paths.get(resource), Charset.defaultCharset())) {
            Map<String, Object> ret = (Map<String, Object>) yaml.load(reader);
            if (ret == null) {
                ret = new HashMap<>();
            } // If the config consists of a single key 'config', its values are used
            // instead. This means that the same config files can be used with Flux
            // and the ConfigurableTopology.
            else {
                if (ret.size() == 1) {
                    Object confNode = ret.get("config");
                    if (confNode != null && confNode instanceof Map) {
                        ret = (Map<String, Object>) confNode;
                    }
                }
            }
            conf.putAll(ret);
            return conf;
        }
    }

    protected Config getConf() {
        return conf;
    }

    protected abstract int run(String args[]) throws Exception;

    /**
     * Submits the topology with the name taken from the configuration
     **/
    protected int submit(Config conf, TopologyBuilder builder) {
        String name = (String) Utils.get(conf, Config.TOPOLOGY_NAME, null);
        if (StringUtils.isBlank(name)) {
            throw new RuntimeException(
                "No value found for " + Config.TOPOLOGY_NAME);
        }
        return submit(name, conf, builder);
    }

    /**
     * Submits the topology under a specific name
     **/
    protected int submit(String name, Config conf, TopologyBuilder builder) {
        try {
            StormSubmitter.submitTopology(name, conf,
                                          builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    private String[] parse(String args[]) {

        List<String> newArgs = new ArrayList<>();
        Collections.addAll(newArgs, args);

        Iterator<String> iter = newArgs.iterator();
        while (iter.hasNext()) {
            String param = iter.next();
            if (param.equals("-conf")) {
                if (!iter.hasNext()) {
                    throw new RuntimeException("Conf file not specified");
                }
                iter.remove();
                String resource = iter.next();
                try {
                    loadConf(resource, conf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                iter.remove();
            }
        }

        return newArgs.toArray(new String[newArgs.size()]);
    }
}

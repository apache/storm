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

package org.apache.storm.flux.parser;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.storm.flux.model.BoltDef;
import org.apache.storm.flux.model.IncludeDef;
import org.apache.storm.flux.model.SpoutDef;
import org.apache.storm.flux.model.TopologyDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

/**
 * Static utility methods for parsing flux YAML.
 */
public class FluxParser {
    private static final Logger LOG = LoggerFactory.getLogger(FluxParser.class);
    private static final Pattern propertyPattern =
            Pattern.compile(".*\\$\\{(?<var>ENV-(?<envVar>.+)|(?<list>.+)\\[(?<listIndex>\\d+)]|.+)}.*");

    private FluxParser() {
    }

    /**
     * Parse a flux topology definition.
     *
     * @param inputFile       source YAML file
     * @param dumpYaml        if true, dump the parsed YAML to stdout
     * @param processIncludes whether or not to process includes
     * @param properties      properties file for variable substitution
     * @param envSub          whether or not to perform environment variable substitution
     * @return resulting topologuy definition
     * @throws IOException if there is a problem reading file(s)
     */
    public static TopologyDef parseFile(String inputFile, boolean dumpYaml, boolean processIncludes,
                                        Properties properties, boolean envSub) throws IOException {
        FileInputStream in = new FileInputStream(inputFile);
        TopologyDef topology = parseInputStream(in, dumpYaml, processIncludes, properties, envSub);
        in.close();

        return topology;
    }

    /**
     * Parse a flux topology definition from a classpath resource..
     *
     * @param resource        YAML resource
     * @param dumpYaml        if true, dump the parsed YAML to stdout
     * @param processIncludes whether or not to process includes
     * @param properties      properties file for variable substitution
     * @param envSub          whether or not to perform environment variable substitution
     * @return resulting topologuy definition
     * @throws IOException if there is a problem reading file(s)
     */
    public static TopologyDef parseResource(String resource, boolean dumpYaml, boolean processIncludes,
                                            Properties properties, boolean envSub) throws IOException {
        InputStream in = FluxParser.class.getResourceAsStream(resource);
        TopologyDef topology = parseInputStream(in, dumpYaml, processIncludes, properties, envSub);
        in.close();

        return topology;
    }

    /**
     * Parse a flux topology definition.
     *
     * @param inputStream     InputStream representation of YAML file
     * @param dumpYaml        if true, dump the parsed YAML to stdout
     * @param processIncludes whether or not to process includes
     * @param properties      properties file for variable substitution
     * @param envSub          whether or not to perform environment variable substitution
     * @return resulting topology definition
     * @throws IOException if there is a problem reading file(s)
     */
    public static TopologyDef parseInputStream(InputStream inputStream, boolean dumpYaml, boolean processIncludes,
                                               Properties properties, boolean envSub) throws IOException {
        Yaml yaml = yaml();

        if (inputStream == null) {
            LOG.error("Unable to load input stream");
            System.exit(1);
        }

        TopologyDef topology = loadYaml(yaml, inputStream, properties, envSub);

        if (dumpYaml) {
            dumpYaml(topology, yaml);
        }

        if (processIncludes) {
            return processIncludes(yaml, topology, properties, envSub);
        } else {
            return topology;
        }
    }

    /**
     * Parse filter properties file.
     *
     * @param propertiesFile properties file for variable substitution
     * @param resource       whether or not to load properties file from classpath
     * @return resulting filter properties
     * @throws IOException if there is a problem reading file
     */
    public static Properties parseProperties(String propertiesFile, boolean resource) throws IOException {
        Properties properties = null;

        if (propertiesFile != null) {
            properties = new Properties();
            InputStream in = null;
            if (resource) {
                in = FluxParser.class.getResourceAsStream(propertiesFile);
            } else {
                in = new FileInputStream(propertiesFile);
            }
            properties.load(in);
            in.close();
        }

        return properties;
    }

    private static TopologyDef loadYaml(Yaml yaml, InputStream in, Properties properties, boolean envSubstitution) throws IOException {
        LOG.info("loading YAML from input stream...");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String conf = reader.lines().map(line -> {
                Matcher m = propertyPattern.matcher(line);
                return m.find()
                        ? getPropertyReplacement(properties, m, envSubstitution)
                        .map(propValue -> line.replace("${" + m.group("var") + "}", propValue))
                        .orElseGet(() -> {
                            LOG.warn("Could not find replacement for property: " + m.group("var"));
                            return line;
                        })
                        : line;
            }).collect(Collectors.joining(System.lineSeparator()));

            return (TopologyDef) yaml.load(conf);
        }
    }

    private static Optional<String> getPropertyReplacement(Properties properties, Matcher match, boolean envSubstitution) {
        if (match.group("listIndex") != null) {
            String prop = properties.getProperty(match.group("list"));
            return Optional.of(parseListAndExtractElem(prop, match.group("listIndex")));
        } else if (envSubstitution && match.group("envVar") != null) {
            String envVar = System.getenv().get(match.group("envVar"));
            return Optional.ofNullable(envVar);
        } else {
            return Optional.ofNullable(properties.getProperty(match.group("var")));
        }
    }

    private static String parseListAndExtractElem(String strList, String index) {
        String[] listProp = strList.substring(1, strList.length() - 1).split(",");
        String listElem = listProp[Integer.parseInt(index)];

        // remove whitespaces and double quotes from beginning and end of a given string
        String trimmed = listElem.trim();
        return trimmed.substring(1, trimmed.length() - 1);
    }

    private static void dumpYaml(TopologyDef topology, Yaml yaml) {
        System.out.println("Configuration (interpreted): \n" + yaml.dump(topology));
    }

    private static Yaml yaml() {
        TypeDescription topologyDescription = new TypeDescription(TopologyDef.class);
        topologyDescription.putListPropertyType("spouts", SpoutDef.class);
        topologyDescription.putListPropertyType("bolts", BoltDef.class);
        topologyDescription.putListPropertyType("includes", IncludeDef.class);

        Constructor constructor = new Constructor(TopologyDef.class);
        constructor.addTypeDescription(topologyDescription);

        Yaml yaml = new Yaml(constructor);
        return yaml;
    }

    /**
     * Process includes contained within a yaml file.
     *
     * @param yaml        the yaml parser for parsing the include file(s)
     * @param topologyDef the topology definition containing (possibly zero) includes
     * @param properties  properties file for variable substitution
     * @param envSub      whether or not to perform environment variable substitution
     * @return The TopologyDef with includes resolved.
     */
    private static TopologyDef processIncludes(Yaml yaml, TopologyDef topologyDef, Properties properties, boolean envSub)
            throws IOException {
        //TODO support multiple levels of includes
        if (topologyDef.getIncludes() != null) {
            for (IncludeDef include : topologyDef.getIncludes()) {
                TopologyDef includeTopologyDef = null;
                if (include.isResource()) {
                    LOG.info("Loading includes from resource: {}", include.getFile());
                    includeTopologyDef = parseResource(include.getFile(), true, false, properties, envSub);
                } else {
                    LOG.info("Loading includes from file: {}", include.getFile());
                    includeTopologyDef = parseFile(include.getFile(), true, false, properties, envSub);
                }

                // if overrides are disabled, we won't replace anything that already exists
                boolean override = include.isOverride();
                // name
                if (includeTopologyDef.getName() != null) {
                    topologyDef.setName(includeTopologyDef.getName(), override);
                }

                // config
                if (includeTopologyDef.getConfig() != null) {
                    //TODO move this logic to the model class
                    Map<String, Object> config = topologyDef.getConfig();
                    Map<String, Object> includeConfig = includeTopologyDef.getConfig();
                    if (override) {
                        config.putAll(includeTopologyDef.getConfig());
                    } else {
                        for (String key : includeConfig.keySet()) {
                            if (config.containsKey(key)) {
                                LOG.warn("Ignoring attempt to set topology config property '{}' with override == false", key);
                            } else {
                                config.put(key, includeConfig.get(key));
                            }
                        }
                    }
                }

                //component overrides
                if (includeTopologyDef.getComponents() != null) {
                    topologyDef.addAllComponents(includeTopologyDef.getComponents(), override);
                }
                //bolt overrides
                if (includeTopologyDef.getBolts() != null) {
                    topologyDef.addAllBolts(includeTopologyDef.getBolts(), override);
                }
                //spout overrides
                if (includeTopologyDef.getSpouts() != null) {
                    topologyDef.addAllSpouts(includeTopologyDef.getSpouts(), override);
                }
                //stream overrides
                //TODO streams should be uniquely identifiable
                if (includeTopologyDef.getStreams() != null) {
                    topologyDef.addAllStreams(includeTopologyDef.getStreams(), override);
                }
            } // end include processing
        }
        return topologyDef;
    }
}

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class CLI {
    /**
     * Parse function to return an Integer.
     */
    public static final Parse AS_INT = value -> Integer.valueOf(value);

    /**
     * Noop parse function, returns the String.
     */
    public static final Parse AS_STRING = value -> value;

    /**
     * Last occurrence on the command line is the resulting value.
     */
    public static final Assoc LAST_WINS = (current, value) -> value;

    /**
     * First occurrence on the command line is the resulting value.
     */
    public static final Assoc FIRST_WINS = (current, value) -> current == null ? value : current;

    /**
     * All values are returned as a List.
     */
    public static final Assoc INTO_LIST = (current, value) -> {
        if (current == null) {
            current = new ArrayList<>();
        }
        ((List<Object>) current).add(value);
        return current;
    };

    /**
     * All values are returned as a map.
     */
    public static final Assoc INTO_MAP = (current, value) -> {
        if (null == current) {
            current = new HashMap<>();
        }
        ((Map<Object, Object>) current).putAll((Map<Object, Object>) value);
        return current;
    };
    private static final Logger LOG = LoggerFactory.getLogger(CLI.class);

    /**
     * Add an option to be parsed.
     * @param shortName the short single character name of the option (no `-` character proceeds it).
     * @param longName the multi character name of the option (no `--` characters proceed it).
     * @param defaultValue the value that will be returned of the command if none is given. null if none is given.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder opt(String shortName, String longName, Object defaultValue) {
        return new CLIBuilder().opt(shortName, longName, defaultValue);
    }

    /**
     * Add an option to be parsed.
     * @param shortName the short single character name of the option (no `-` character proceeds it).
     * @param longName the multi character name of the option (no `--` characters proceed it).
     * @param defaultValue the value that will be returned of the command if none is given. null if none is given.
     * @param parse an optional function to transform the string to something else. If null a NOOP is used.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder opt(String shortName, String longName, Object defaultValue, Parse parse) {
        return new CLIBuilder().opt(shortName, longName, defaultValue, parse);
    }

    /**
     * Add an option to be parsed.
     * @param shortName the short single character name of the option (no `-` character proceeds it).
     * @param longName the multi character name of the option (no `--` characters proceed it).
     * @param defaultValue the value that will be returned of the command if none is given. null if none is given.
     * @param parse an optional function to transform the string to something else. If null a NOOP is used.
     * @param assoc an association command to decide what to do if the option appears multiple times.  If null LAST_WINS is used.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder opt(String shortName, String longName, Object defaultValue, Parse parse, Assoc assoc) {
        return new CLIBuilder().opt(shortName, longName, defaultValue, parse, assoc);
    }

    /**
     * Add a boolean option that enables something.
     * @param shortName the short single character name of the option (no `-` character proceeds it).
     * @param longName the multi character name of the option (no `--` characters proceed it).
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder boolOpt(String shortName, String longName) {
        return new CLIBuilder().boolOpt(shortName, longName);
    }

    /**
     * Add a named argument.
     * @param name the name of the argument.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder arg(String name) {
        return new CLIBuilder().arg(name);
    }

    /**
     * Add a named argument.
     * @param name the name of the argument.
     * @param assoc an association command to decide what to do if the argument appears multiple times.  If null INTO_LIST is used.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder arg(String name, Assoc assoc) {
        return new CLIBuilder().arg(name, assoc);
    }

    /**
     * Add a named argument.
     * @param name the name of the argument.
     * @param parse an optional function to transform the string to something else. If null a NOOP is used.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder arg(String name, Parse parse) {
        return new CLIBuilder().arg(name, parse);
    }

    /**
     * Add a named argument.
     * @param name the name of the argument.
     * @param parse an optional function to transform the string to something else. If null a NOOP is used.
     * @param assoc an association command to decide what to do if the argument appears multiple times.  If null INTO_LIST is used.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder arg(String name, Parse parse, Assoc assoc) {
        return new CLIBuilder().arg(name, parse, assoc);
    }

    /**
     * Add a named argument that is optional.
     * @param name the name of the argument.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder optionalArg(String name) {
        return new CLIBuilder().optionalArg(name);
    }

    /**
     * Add a named argument that is optional.
     * @param name the name of the argument.
     * @param assoc an association command to decide what to do if the argument appears multiple times.  If null INTO_LIST is used.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder optionalArg(String name, Assoc assoc) {
        return new CLIBuilder().optionalArg(name, assoc);
    }

    /**
     * Add a named argument that is optional.
     * @param name the name of the argument.
     * @param parse an optional function to transform the string to something else. If null a NOOP is used.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder optionalArg(String name, Parse parse) {
        return new CLIBuilder().optionalArg(name, parse);
    }

    /**
     * Add a named argument that is optional.
     * @param name the name of the argument.
     * @param parse an optional function to transform the string to something else. If null a NOOP is used.
     * @param assoc an association command to decide what to do if the argument appears multiple times.  If null INTO_LIST is used.
     * @return a builder to be used to continue creating the command line.
     */
    public static CLIBuilder optionalArg(String name, Parse parse, Assoc assoc) {
        return new CLIBuilder().optionalArg(name, parse, assoc);
    }

    public interface Parse {
        /**
         * Parse a String to the type you want it to be.
         * @param value the String to parse
         * @return the parsed value
         */
        Object parse(String value);
    }

    public interface Assoc {
        /**
         * Associate a value into something else.
         * @param current what to put value into, will be null if no values have been added yet.
         * @param value what to add
         * @return the result of combining the two
         */
        Object assoc(Object current, Object value);
    }

    private static class Opt {
        final String shortName;
        final String longName;
        final Object defaultValue;
        final Parse parse;
        final Assoc assoc;
        final boolean noValue;

        Opt(String shortName, String longName, Object defaultValue, Parse parse, Assoc assoc, boolean noValue) {
            this.shortName = shortName;
            this.longName = longName;
            this.defaultValue = defaultValue;
            this.parse = parse == null ? AS_STRING : parse;
            this.assoc = assoc == null ? LAST_WINS : assoc;
            this.noValue = noValue;
        }

        public Object process(Object current, String value) {
            return assoc.assoc(current, parse.parse(value));
        }
    }

    private static class Arg {
        final String name;
        final Parse parse;
        final Assoc assoc;

        Arg(String name, Parse parse, Assoc assoc) {
            this.name = name;
            this.parse = parse == null ? AS_STRING : parse;
            this.assoc = assoc == null ? INTO_LIST : assoc;
        }

        public Object process(Object current, String value) {
            return assoc.assoc(current, parse.parse(value));
        }
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static class CLIBuilder {
        private final ArrayList<Opt> opts = new ArrayList<>();
        private final ArrayList<Arg> args = new ArrayList<>();
        private final ArrayList<Arg> optionalArgs = new ArrayList<>();

        /**
         * Add an option to be parsed.
         * @param shortName the short single character name of the option (no `-` character proceeds it).
         * @param longName the multi character name of the option (no `--` characters proceed it).
         * @param defaultValue the value that will be returned of the command if none is given. null if none is given.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder opt(String shortName, String longName, Object defaultValue) {
            return opt(shortName, longName, defaultValue, null, null);
        }

        /**
         * Add an option to be parsed.
         * @param shortName the short single character name of the option (no `-` character proceeds it).
         * @param longName the multi character name of the option (no `--` characters proceed it).
         * @param defaultValue the value that will be returned of the command if none is given. null if none is given.
         * @param parse an optional function to transform the string to something else. If null a NOOP is used.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder opt(String shortName, String longName, Object defaultValue, Parse parse) {
            return opt(shortName, longName, defaultValue, parse, null);
        }

        /**
         * Add an option to be parsed.
         * @param shortName the short single character name of the option (no `-` character proceeds it).
         * @param longName the multi character name of the option (no `--` characters proceed it).
         * @param defaultValue the value that will be returned of the command if none is given. null if none is given.
         * @param parse an optional function to transform the string to something else. If null a NOOP is used.
         * @param assoc an association command to decide what to do if the option appears multiple times.  If null LAST_WINS is used.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder opt(String shortName, String longName, Object defaultValue, Parse parse, Assoc assoc) {
            opts.add(new Opt(shortName, longName, defaultValue, parse, assoc, false));
            return this;
        }

        /**
         * Add a boolean option that enables something.
         * @param shortName the short single character name of the option (no `-` character proceeds it).
         * @param longName the multi character name of the option (no `--` characters proceed it).
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder boolOpt(String shortName, String longName) {
            opts.add(new Opt(shortName, longName, false, null, null, true));
            return this;
        }

        /**
         * Add a named argument.
         * @param name the name of the argument.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder arg(String name) {
            return arg(name, null, null);
        }

        /**
         * Add a named argument.
         * @param name the name of the argument.
         * @param assoc an association command to decide what to do if the argument appears multiple times.  If null INTO_LIST is used.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder arg(String name, Assoc assoc) {
            return arg(name, null, assoc);
        }

        /**
         * Add a named argument.
         * @param name the name of the argument.
         * @param parse an optional function to transform the string to something else. If null a NOOP is used.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder arg(String name, Parse parse) {
            return arg(name, parse, null);
        }

        /**
         * Add a named argument.
         * @param name the name of the argument.
         * @param parse an optional function to transform the string to something else. If null a NOOP is used.
         * @param assoc an association command to decide what to do if the argument appears multiple times.  If null INTO_LIST is used.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder arg(String name, Parse parse, Assoc assoc) {
            if (!optionalArgs.isEmpty()) {
                throw new IllegalStateException("Cannot have a required argument after adding in an optional argument");
            }
            args.add(new Arg(name, parse, assoc));
            return this;
        }

        /**
         * Add a named argument that is optional.
         * @param name the name of the argument.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder optionalArg(String name) {
            return optionalArg(name, null, null);
        }

        /**
         * Add a named argument that is optional.
         * @param name the name of the argument.
         * @param assoc an association command to decide what to do if the argument appears multiple times.  If null INTO_LIST is used.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder optionalArg(String name, Assoc assoc) {
            return optionalArg(name, null, assoc);
        }

        /**
         * Add a named argument that is optional.
         * @param name the name of the argument.
         * @param parse an optional function to transform the string to something else. If null a NOOP is used.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder optionalArg(String name, Parse parse) {
            return optionalArg(name, parse, null);
        }

        /**
         * Add a named argument that is optional.
         * @param name the name of the argument.
         * @param parse an optional function to transform the string to something else. If null a NOOP is used.
         * @param assoc an association command to decide what to do if the argument appears multiple times.  If null INTO_LIST is used.
         * @return a builder to be used to continue creating the command line.
         */
        public CLIBuilder optionalArg(String name, Parse parse, Assoc assoc) {
            optionalArgs.add(new Arg(name, parse, assoc));
            return this;
        }

        /**
         * Parse the command line arguments.
         * @param rawArgs the string arguments to be parsed.
         * @return The parsed command line.
         *     opts will be stored under the short argument name.
         *     args will be stored under the argument name, unless no arguments are configured, and then they will be stored under "ARGS".
         *     The last argument configured is greedy and is used to process all remaining command line arguments.
         * @throws Exception on any error.
         */
        public Map<String, Object> parse(String... rawArgs) throws Exception {
            Options options = new Options();
            for (Opt opt : opts) {
                if (opt.noValue) {
                    options.addOption(Option.builder(opt.shortName).longOpt(opt.longName).hasArg(false).build());
                } else {
                    options.addOption(Option.builder(opt.shortName).longOpt(opt.longName).hasArg().build());
                }
            }
            DefaultParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, rawArgs);
            HashMap<String, Object> ret = new HashMap<>();
            for (Opt opt : opts) {
                if (opt.noValue) {
                    ret.put(opt.shortName, cl.hasOption(opt.shortName));
                } else {
                    Object current = null;
                    String[] strings = cl.getOptionValues(opt.shortName);
                    if (strings != null) {
                        for (String val : strings) {
                            current = opt.process(current, val);
                        }
                    }
                    if (current == null) {
                        current = opt.defaultValue;
                    }
                    ret.put(opt.shortName, current);
                }
            }
            List<Arg> fullArgs = new ArrayList<>(args);
            fullArgs.addAll(optionalArgs);
            List<String> stringArgs = cl.getArgList();
            if (args.size() > stringArgs.size()) {
                throw new RuntimeException("Wrong number of arguments at least " + args.size()
                    + " expected, but only " + stringArgs.size() + " found");
            }

            int argIndex = 0;
            int stringArgIndex = 0;
            if (fullArgs.size() > 0) {
                while (argIndex < fullArgs.size()) {
                    Arg arg = fullArgs.get(argIndex);
                    boolean isLastArg = (argIndex == (fullArgs.size() - 1));
                    Object current = null;
                    int maxStringIndex = isLastArg ? stringArgs.size() : (stringArgIndex + 1);
                    for (; stringArgIndex < maxStringIndex; stringArgIndex++) {
                        current = arg.process(current, stringArgs.get(stringArgIndex));
                    }
                    ret.put(arg.name, current);
                    argIndex++;
                }
            } else {
                ret.put("ARGS", stringArgs);
            }
            return ret;
        }
    }
}

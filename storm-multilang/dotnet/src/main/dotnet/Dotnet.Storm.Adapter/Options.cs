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
using CommandLine;

namespace Dotnet.Storm.Adapter
{
    public class Options
    {

        [Option('c', "class", Required = true, HelpText = "Component class name to be instantiated.")]
        public string Class { get; set; }

        [Option('a', "assembly", Required = true, HelpText = "The assembly which contains the component class.")]
        public string Assembly { get; set; }

        [Option('p', "parameters", Required = false, HelpText = "Component command line parameters.")]
        public string Arguments { get; set; }

        [Option('l', "loglevel", Required = false, Default = "INFO", HelpText = "Log level restriction. Possible values (TRACE,DEBUG,INFO,WARN,ERROR).")]
        public string LogLevel { get; set; }

        [Option('s', "serializer", Required = false, Default = "json", HelpText = "Message serializer. Only JSON serializer is availiable now.")]
        public string Serializer { get; set; }

        [Option('h', "channel", Required = false, Default = "std", HelpText = "Message exchange channel. Only STD channel is availiable now.")]
        public string Channel { get; set; }
    }
}

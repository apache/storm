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
using System;
using System.IO;
using System.Reflection;
using CommandLine;
using Dotnet.Storm.Adapter.Channels;
using Dotnet.Storm.Adapter.Components;
using Dotnet.Storm.Adapter.Logging;
using Dotnet.Storm.Adapter.Serializers;
using log4net;
using log4net.Config;
using log4net.Core;

namespace Dotnet.Storm.Adapter
{
    class Program
    {
        private readonly static ILog Logger = LogManager.GetLogger(typeof(Program));

        static void Main(string[] args)
        {
            // Parse command line arguments
            var parser = new Parser(with => with.EnableDashDash = true).ParseArguments<Options>(args);

            string className = null;
            string assemblyName = null;
            string arguments = null;
            LogLevel level = LogLevel.INFO;
            string serializer;
            string channel;

            parser.WithParsed(options =>
            {
                className = options.Class;
                assemblyName = options.Assembly;
                arguments = options.Arguments;
                serializer = options.Serializer;
                channel = options.Channel;

                // by default TryParse will return TRACE level in case of error
                Enum.TryParse(options.LogLevel.ToUpper(), out level);

                // if user didn't set TRACE level but we TryParse returned TRACE = parsing exception
                if (!options.LogLevel.ToLower().Equals("trace") && level == LogLevel.TRACE)
                {
                    // reset default log level to INFO
                    level = LogLevel.INFO;
                }
            });

            // Configure logging
            var repository = LogManager.GetRepository(Assembly.GetEntryAssembly());

            if (File.Exists("log4net.config"))
            {
                XmlConfigurator.Configure(repository, new FileInfo("log4net.config"));
            }
            else
            {
                XmlConfigurator.Configure(repository);
            }

            // setting up log level
            Level newLevel = StormAppender.GetLogLevel(level);

            // now wlet's add StormApender and remove all console appenders
            // initially StormAppender i in desblem mode, we'llenable it later
            StormAppender.CreateAppender(newLevel);

            Logger.Debug($"Current working directory: {Environment.CurrentDirectory}.");

            // Instantiate component
            Type type = null;

            // className is required option so we don't need to check it for NULL
            if(!string.IsNullOrEmpty(assemblyName))
            {
                Logger.Debug($"Loading assembly: {assemblyName}.");
                AssemblyName name = new AssemblyName(assemblyName);
                string path = Path.Combine(Environment.CurrentDirectory, name.Name + ".dll");

                Assembly assembly = Assembly.Load(File.ReadAllBytes(path));
                type = assembly.GetType(className, true);
            }

            Component component = (Component)Activator.CreateInstance(type);

            if (arguments != null)
            {
                component.SetArguments(arguments);
            }

            // there is an idea to use shared memory channel
            Channel.Instance = new StandardChannel
            {
                // there is an idea to use ProtoBuffer serialization 
                Serializer = new JsonSerializer()
            };

            //handshake protocol
            component.Connect();

            // the connection is established!!! congratulations!!!
            // now we can enable storm logger
            StormAppender.Enable();

            //execution cicle
            component.Start();
        }
    }
}

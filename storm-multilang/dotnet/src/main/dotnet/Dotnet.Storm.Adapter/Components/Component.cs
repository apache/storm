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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Dotnet.Storm.Adapter.Channels;
using Dotnet.Storm.Adapter.Messaging;
using log4net;
using Newtonsoft.Json;

namespace Dotnet.Storm.Adapter.Components
{
    public abstract class Component
    {
        #region Component interface
        protected class Storm
        {
            public static void Sync()
            {
                Channel.Instance.Send(new SyncMessage());
            }

            public static void Error(string message)
            {
                Channel.Instance.Send(new ErrorMessage(message));
            }

            public static void Metrics(string name, object value)
            {
                Channel.Instance.Send(new MetricMessage(name, value));
            }

            public static VerificationResult VerifyInput(string component, string stream, List<object> tuple)
            {
                if(stream == "__heartbeat" || stream == "__tick")
                {
                    return new VerificationResult(false, "Input: OK");
                }
                if (string.IsNullOrEmpty(component))
                {
                    return new VerificationResult(true, "Input: component is null");
                }
                if (string.IsNullOrEmpty(stream))
                {
                    return new VerificationResult(true, "Input: stream is null");
                }
                if (tuple == null)
                {
                    return new VerificationResult(true, "Input: tuple is null");
                }
                if (!Context.SourceToStreamToFields.ContainsKey(component))
                {
                    return new VerificationResult(true, $"Input: component '{component}' is not defined as an input");
                }
                if (!Context.SourceToStreamToFields[component].ContainsKey(stream))
                { 
                    return new VerificationResult(true, $"Input: component '{component}' doesn't contain '{stream}' stream");
                }
                int count = Context.SourceToStreamToFields[component][stream].Count;
                if (count != tuple.Count)
                {
                    return new VerificationResult(true, $"Input: tuple contains [{tuple.Count}] fields but the {stream} stream can process only [{count}]");
                }
                return new VerificationResult(false, "Input: OK");
            }

            public static VerificationResult VerifyOutput(string stream, List<object> tuple)
            {
                if (string.IsNullOrEmpty(stream))
                {
                    return new VerificationResult(true, "Output: stream is null");
                }
                if (tuple == null)
                {
                    return new VerificationResult(true, "Output: tuple is null");
                }
                if (!Context.StreamToOputputFields.ContainsKey(stream))
                {
                    return new VerificationResult(true, $"Output: component doesn't contain {stream} stream");
                }
                int count = Context.StreamToOputputFields[stream].Count;
                if (count != tuple.Count)
                {
                    return new VerificationResult(true, $"Output: tuple contains [{tuple.Count}] fields but the {stream} stream can process only [{count}]");
                }
                return new VerificationResult(false, "Output: OK");
            }
        }

        protected readonly static ILog Logger = LogManager.GetLogger(typeof(Component));

        protected string[] Arguments { get; private set; }

        protected static IDictionary<string, object> Configuration { get; private set; }

        protected static StormContext Context { get; private set; }

        protected static bool IsGuaranteed
        {
            get
            {
                object number = Configuration["topology.acker.executors"];
                if (number == null)
                {
                    return true;
                }
                if (int.TryParse(number.ToString(), out int result))
                {
                    return result != 0;
                }
                return false;
            }
        }

        protected static int MessageTimeout
        {
            get
            {
                object number = Configuration["topology.message.timeout.secs"];
                if (number == null)
                {
                    return 30;
                }
                if (int.TryParse(number.ToString(), out int result))
                {
                    return result;
                }
                return 30;
            }
        }

        protected event EventHandler OnInitialized;
        #endregion

        internal void SetArguments(string line)
        {
            if(!string.IsNullOrEmpty(line))
            {
                Arguments = line.Split(new char[] { ' ' });
            }
        }

        internal void Connect()
        {
            // waiting for storm to send connect message
            Logger.Debug("Waiting for connect message.");
            ConnectMessage message = (ConnectMessage)Channel.Instance.Receive<ConnectMessage>();

            int pid = Process.GetCurrentProcess().Id;

            // storm requires to create empty file named with PID
            if (!string.IsNullOrEmpty(message.PidDir) && Directory.Exists(message.PidDir))
            {
                Logger.Debug($"Creating pid file. PidDir: {message.PidDir}; PID: {pid}");
                string path = Path.Combine(message.PidDir, pid.ToString());
                File.WriteAllText(path, "");
            }

            Logger.Debug($"Current context: {JsonConvert.SerializeObject(message.Context)}.");
            Context = message.Context;

            Logger.Debug($"Current config: {JsonConvert.SerializeObject(message.Configuration)}.");
            Configuration = message.Configuration;

            // send PID back to storm
            Channel.Instance.Send(new PidMessage(pid));

            // now let's notify the component the context is set and configuration is available
            OnInitialized?.Invoke(this, EventArgs.Empty);
        }

        internal abstract void Start();
    }
}

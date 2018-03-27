using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Dotnet.Storm.Adapter.Messaging;
using log4net;
using Newtonsoft.Json;

namespace Dotnet.Storm.Adapter.Components
{
    public abstract class Component
    {
        #region Component interface
        protected class LocalStorm
        {
            public void Sync()
            {
                GlobalStorm.Send(new SyncMessage());
            }

            public void Error(string message)
            {
                GlobalStorm.Send(new ErrorMessage(message));
            }

            public void Metrics(string name, object value)
            {
                GlobalStorm.Send(new MetricMessage(name, value));
            }

            public VerificationResult VerifyInput(string component, string stream, List<object> tuple)
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

            public VerificationResult VerifyOutput(string stream, List<object> tuple)
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

        protected LocalStorm Storm { get; private set; }

        protected string[] Arguments { get; private set; }

        protected static IDictionary<string, object> Configuration { get; private set; }

        protected static StormContext Context { get; private set; }

        protected static bool IsGuarantee
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
            ConnectMessage message = (ConnectMessage)GlobalStorm.Receive<ConnectMessage>();

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
            GlobalStorm.Send(new PidMessage(pid));
        }

        internal abstract void Start();
    }
}

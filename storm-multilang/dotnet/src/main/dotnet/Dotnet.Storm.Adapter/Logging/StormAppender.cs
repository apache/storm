using Dotnet.Storm.Adapter.Messaging;
using log4net;
using log4net.Appender;
using log4net.Core;
using log4net.Layout;
using log4net.Repository.Hierarchy;
using System.Reflection;

namespace Dotnet.Storm.Adapter.Logging
{
    public class StormAppender : AppenderSkeleton
    {
        private static StormAppender instance;

        private StormAppender() { }

        internal bool Enabled { get; set; }

        protected override void Append(LoggingEvent loggingEvent)
        {
            if (Enabled)
            {
                string message = RenderLoggingEvent(loggingEvent);
                LogLevel level = GetStormLevel(loggingEvent.Level);

                Channel.Send(new LogMessage(message, level));
            }
        }

        public static LogLevel GetStormLevel(Level level)
        {
            switch (level.Name)
            {
                case "FINE":
                case "TRACE":
                case "FINER":
                case "VERBOSE":
                case "FINEST":
                case "ALL":
                    return LogLevel.TRACE;
                case "log4net:DEBUG":
                case "DEBUG":
                    return LogLevel.DEBUG;
                case "OFF":
                case "INFO":
                    return LogLevel.INFO;
                case "WARN":
                case "NOTICE":
                    return LogLevel.WARN;
                case "EMERGENCY":
                case "FATAL":
                case "ALERT":
                case "CRITICAL":
                case "SEVERE":
                case "ERROR":
                    return LogLevel.ERROR;
                default:
                    return LogLevel.INFO;
            }
        }

        public static Level GetLogLevel(LogLevel level)
        {
            switch (level)
            {
                case LogLevel.TRACE:
                    return Level.Trace;
                case LogLevel.DEBUG:
                    return Level.Debug;
                case LogLevel.INFO:
                    return Level.Info;
                case LogLevel.WARN:
                    return Level.Warn;
                case LogLevel.ERROR:
                    return Level.Error;
                default:
                    return Level.Info;
            }
        }

        internal static void Enable()
        {
            instance.Enabled = true;
        }

        internal static void Disable()
        {
            instance.Enabled = false;
        }

        internal static void CreateAppender(Level level)
        {
            Hierarchy repository = (Hierarchy)LogManager.GetRepository(Assembly.GetEntryAssembly());

            // disabel all console like logger
            // it will not prevent the case other appenders write standard output
            // also remove any StormAppender configured in log4net.config file
            foreach (IAppender appender in repository.GetAppenders())
            {
                if (appender.GetType().IsAssignableFrom(typeof(ConsoleAppender)) || appender.GetType().IsAssignableFrom(typeof(StormAppender)))
                {
                    Logger root = repository.Root;
                    IAppenderAttachable attachable = root as IAppenderAttachable;
                    attachable.RemoveAppender(appender);
                }
                ((AppenderSkeleton)appender).Threshold = level;
            }

            instance = new StormAppender
            {
                Layout = new PatternLayout("%m"),
                Enabled = false,
                Threshold = level
            };
            repository.Root.AddAppender(instance);

        }
    }
}

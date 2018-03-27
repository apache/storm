using Dotnet.Storm.Adapter.Messaging;
using log4net;
using log4net.Appender;
using log4net.Core;
using log4net.Repository.Hierarchy;
using System.Reflection;

namespace Dotnet.Storm.Adapter.Logging
{
    public class StormAppender : AppenderSkeleton
    {
        internal bool Enabled { get; set; }

        protected override void Append(LoggingEvent loggingEvent)
        {
            if(Enabled)
            {
                string message = RenderLoggingEvent(loggingEvent);
                LogLevel level = GetStormLevel(loggingEvent.Level);

                GlobalStorm.Send(new LogMessage(message, level));
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
            var repository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            IAppender[] appenders = repository.GetAppenders();
            foreach (IAppender appender in appenders)
            {
                if (appender is StormAppender)
                {
                    (appender as StormAppender).Enabled = true;
                }
            }
        }

        internal static void Disable(log4net.Repository.ILoggerRepository repository, Level newLevel)
        {
            IAppender[] appenders = repository.GetAppenders();
            foreach (IAppender appender in appenders)
            {
                // don't wand any console logger to be enabled since it is bracking storm multilang protocol
                if (appender.GetType().IsAssignableFrom(typeof(ConsoleAppender)))
                {
                    Logger root = ((Hierarchy)repository).Root;
                    IAppenderAttachable attachable = root as IAppenderAttachable;
                    attachable.RemoveAppender(appender);
                }
                else
                {
                    if (appender is StormAppender)
                    {
                        // we have to disable Storm logger untill a connection will be established
                        (appender as StormAppender).Enabled = false;
                    }
                    ((AppenderSkeleton)appender).Threshold = newLevel;
                }
            }
        }
    }
}

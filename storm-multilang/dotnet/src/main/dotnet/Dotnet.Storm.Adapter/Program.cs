using System;
using System.IO;
using System.Reflection;
using CommandLine;
using Dotnet.Storm.Adapter.Components;
using Dotnet.Storm.Adapter.Logging;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Core;
using log4net.Repository.Hierarchy;

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

            parser.WithParsed(options =>
            {
                className = options.Class;
                assemblyName = options.Assembly;
                arguments = options.Arguments;

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

            // we want to disable storm logger since storm is not accessible yet
            StormAppender.Disable(repository, newLevel);
            Logger.Debug("Storm logger disabled.");

            Logger.Debug($"Current working directory: {Environment.CurrentDirectory}.");

            // we don't want any console logger to be enabled since it is bracking storm multilang protocol
            // it will not prevent the case other appenders write standard output
            IAppender[] appenders = repository.GetAppenders();
            foreach (IAppender appender in appenders)
            {
                if (appender.GetType().IsAssignableFrom(typeof(ConsoleAppender)))
                {
                    Logger root = ((Hierarchy)repository).Root;
                    IAppenderAttachable attachable = root as IAppenderAttachable;
                    attachable.RemoveAppender(appender);
                    Logger.Debug($"Removing console logger {appender.Name}.");
                }
            }

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

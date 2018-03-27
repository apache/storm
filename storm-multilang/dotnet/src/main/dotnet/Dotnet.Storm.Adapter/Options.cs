using CommandLine;

namespace Dotnet.Storm.Adapter
{
    public class Options
    {

        [Option('c', "class", Required = true, HelpText = "Component class name to be instantiated.")]
        public string Class { get; set; }

        [Option('a', "assembly", Required = false, HelpText = "The assembly which contains the component class.")]
        public string Assembly { get; set; }

        [Option('p', "parameters", Required = false, HelpText = "Component command line parameters.")]
        public string Arguments { get; set; }

        [Option('l', "loglevel", Required = false, Default = "INFO", HelpText = "Log level restriction. Possible values (TRACE,DEBUG,INFO,WARN,ERROR)")]
        public string LogLevel { get; set; }
    }
}

Overview
========
Dotnet.Strom.Adapter is a .NET Core 2.0 implementation of Storm multi-lang protocol. You can use it to implement CSharp components for your topology. 

Prerequisites
========
 
* .NET Core framework 2.0 and above
* Git

Install form NuGet
========
		PM> Install-Package Dotnet.Storm.Adapter

Build locally
========
Run next command

		cd /strom-mulilang/dotnet
		build.sh adapter

Creating NuGet package
========

		cd /strom-mulilang/dotnet
		build.sh nuget

Run example
========

		cd /strom-mulilang/dotnet
		run.sh

Command line parameters
========

* -c (class name) - component class to instantiate
* -a (assembly name) - dll, containing component class
* -p (parameters) - parameters will be available through Arguments property
* -l (log level) - one of TRACE, DEBUG, INFO, WARN, ERROR

The example of usage is 

		spouts:
		 - id: emit-sentence
		   className: org.apache.storm.flux.wrappers.spouts.FluxShellSpout
		   constructorArgs:
		     - ["dotnet", "Dotnet.Storm.Adapter.dll", "-c", "Dotnet.Storm.Example.EmitSentense", "-a", "Dotnet.Storm.Example", "-l", "debug"]
		     - [sentence]
		   parallelism: 1

API
========

## Common

- Properties

        protected readonly static ILog Logger;

        protected LocalStorm Storm;

        protected string[] Arguments;

        protected static IDictionary<string, object> Configuration;

        protected static StormContext Context;

        protected static bool IsGuarantee;

        protected static int MessageTimeout;
        
- Events

        protected event EventHandler<TaskIds> OnTaskIds;

- LocalStorm methods

        public void Sync()

        public void Error(string message)

        public void Metrics(string name, object value)

        public VerificationResult VerifyInput(string component, string stream, List<object> tuple)

        public VerificationResult VerifyOutput(string stream, List<object> tuple)
            
## Spout specific
- Properties

        protected new LocalStorm Storm

        protected bool IsEnabled = false;

- Events

        protected event EventHandler OnActivate;

        protected event EventHandler OnDeactivate;

- LocalStorm methods

        public void Emit(List<object> tuple, string stream = "default", long task = 0, bool needTaskIds = false)

- Methods

        protected abstract void Next();

## Bolt specific
- Events

        protected event EventHandler<EventArgs> OnTick;

- LocalStorm methods

        public void Ack(string id);

        public void Fail(string id);

        public void Emit(List<object> tuple, string stream = "default", long task = 0, List<string> anchors = null, bool needTaskIds = false);

- Methods

         protected abstract void Execute(StormTuple tuple);
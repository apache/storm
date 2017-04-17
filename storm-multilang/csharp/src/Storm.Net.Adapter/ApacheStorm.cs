using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Storm
{
    public class ApacheStorm
    {
        public static Context ctx;
        public static Queue<Command> pendingCommands = new Queue<Command>();
        public static Queue<String> pendingTaskIds = new Queue<string>();

        public static void LaunchPlugin(newPlugin createDelegate)
		{
            TopologyContext context = null;
            Config config = new Config();

            Context.pluginType = PluginType.UNKNOW;
            Type classType = createDelegate.Method.ReturnType;   
            Type[] interfaces = classType.GetInterfaces();

            foreach (Type eachType in interfaces)
            {
                if (eachType == typeof(ISpout))
                {
                    Context.pluginType = PluginType.SPOUT;
                    break;
                }
                else if (eachType == typeof(IBolt))
                {
                    Context.pluginType = PluginType.BOLT;
                    break;
                }
                else if (eachType == typeof(IBasicBolt))
                {
                    Context.pluginType = PluginType.BASICBOLT;
                    break;
                }
            }
            
            InitComponent(ref config, ref context);
            Context.Config = config;
            Context.TopologyContext = context;

			PluginType pluginType = Context.pluginType;
			Context.Logger.Info("LaunchPlugin, pluginType: {0}", new object[]
			{
				pluginType
			});

			switch (pluginType)
			{
				case PluginType.SPOUT:
				{
					Spout spout = new Spout(createDelegate);
                    spout.Launch();
					return;
				}
				case PluginType.BOLT:
				{
					Bolt bolt = new Bolt(createDelegate);
                    bolt.Launch();
					return;
				}
                case PluginType.BASICBOLT:
                {
                    BasicBolt basicBolt = new BasicBolt(createDelegate);
                    basicBolt.Launch();
                    return;
                }
				default:
				{
					Context.Logger.Error("unexpected pluginType: {0}!", new object[]
					{
						pluginType
					});
                    return;
				}
			}
		}


        /// <summary>
        /// write lines to default stream.
        /// </summary>
        /// <param name="message">stdout</param>
        public static void SendMsgToParent(string message)
        {
            if (string.IsNullOrEmpty(message))
                return;

            Console.OutputEncoding = Encoding.UTF8;
            Console.WriteLine(message);
            Console.WriteLine("end");
        }

        /// <summary>
        /// Init the Component
        /// </summary>
        public static void InitComponent(ref Config config, ref TopologyContext context)
        {
            string message = ReadMsg();

            JContainer container = JsonConvert.DeserializeObject(message) as JContainer;

            var _pidDir = container["pidDir"];
            if (_pidDir != null && _pidDir.GetType() == typeof(JValue))
            {
                string pidDir = (_pidDir as JValue).Value.ToString();
                SendPid(pidDir);
            }

            var _conf = container["conf"];
            if (_conf != null && _conf.GetType().BaseType == typeof(JContainer))
            {
                config = GetConfig(_conf as JContainer);
            }

            var _context = container["context"];
            if (_context != null && _context.GetType().BaseType == typeof(JContainer))
            {
                context = GetContext(_context as JContainer);
            }
        }

        public static Command ReadCommand()
        {
            if (pendingCommands.Count > 0)
                return pendingCommands.Dequeue();
            else
            {
                do
                {
                    string msg = ReadMsg();

                    bool isTaskId = JsonConvert.DeserializeObject(msg).GetType() == typeof(JArray);
                    if (isTaskId)
                    {
                        JArray container = JsonConvert.DeserializeObject(msg) as JArray;

                        if (container == null || container.Count == 0)
                            pendingTaskIds.Enqueue(" ");
                        else
                        {
                            pendingTaskIds.Enqueue((container[0] as JValue).Value.ToString());
                        }
                    }
                    else
                    {
                        return ConvertCommand(msg);
                    }
                }
                while (true);
            }
        }

        private static Command ConvertCommand(string msg)
        {
            JContainer container = JsonConvert.DeserializeObject(msg) as JContainer;
            var _command = container["command"];
            if (_command != null && _command.GetType() == typeof(JValue))
            {
                string name = (_command as JValue).Value.ToString();
                string id = "";

                var _id = container["id"];
                if (_id != null && _id.GetType() == typeof(JValue))
                {
                    id = (_id as JValue).Value.ToString();
                }
                return new Command() { Id = id, Name = name };
            }
            else
            {
                int taskId = -1;
                string streamId = "", id = "", component = "";
                List<object> tuple = new List<object>();

                var _tupleId = container["id"];
                if (_tupleId != null && _tupleId.GetType() == typeof(JValue))
                {
                    id = (_tupleId as JValue).Value.ToString();
                }

                var _component = container["comp"];
                if (_component != null && _component.GetType() == typeof(JValue))
                {
                    if ((_component as JValue).Value != null)
                        component = (_component as JValue).Value.ToString();
                }

                var _streamId = container["stream"];
                if (_streamId != null && _streamId.GetType() == typeof(JValue))
                {
                    streamId = (_streamId as JValue).Value.ToString();
                }

                var _taskId = container["task"];
                if (_taskId != null && _taskId.GetType() == typeof(JValue))
                {
                    Int32.TryParse((_taskId as JValue).Value.ToString(), out taskId);
                }

                var _values = container["tuple"];
                if (_values != null && _values.GetType() == typeof(JArray))
                {
                    JArray values = _values as JArray;
                    if (values.Count > 0)
                    {
                        ApacheStorm.ctx.CheckInputSchema(streamId, values.Count);
                        for (int i = 0; i < values.Count; i++)
                        {
                            Type type = ApacheStorm.ctx._schemaByCSharp.InputStreamSchema[streamId][i];                            
                            tuple.Add(values[i].ToObject(type));                            
                        }
                    }
                }

                return new Command() { Id = id, Component = component, StreamId = streamId, TaskId = taskId, Tuple = tuple };
            }
        }

        public static string ReadTaskId()
        {
            if (pendingTaskIds.Count > 0)
                return pendingTaskIds.Dequeue();
            else
            {
                do
                {
                    string msg = ReadMsg();

                    bool isTaskId = JsonConvert.DeserializeObject(msg).GetType() == typeof(JArray);
                    if (!isTaskId)
                    {
                        pendingCommands.Enqueue(ConvertCommand(msg));
                    }
                    else
                    {
                        JArray container = JsonConvert.DeserializeObject(msg) as JArray;
                        
                        if (container == null || container.Count == 0) //will be null at the end of bolt.
                            return "";
                        else
                            return (container[0] as JValue).Value.ToString();
                    }
                }
                while (true);
            }
        }

        public static StormTuple ReadTuple()
        {
            Command command = ReadCommand();
            return new StormTuple(command.Tuple, command.TaskId, command.StreamId, command.Id, command.Component);
        }

        /// <summary>
        /// reads lines and reconstructs newlines appropriately
        /// </summary>
        /// <returns>the stdin message string</returns>
        public static string ReadMsg()
        {
            StringBuilder message = new StringBuilder();
            Stream inputStream = Console.OpenStandardInput();

            do
            {
                List<byte> bytes = new List<byte>();
                do
                {
                    byte[] _bytes = new byte[1];
                    int outputLength = inputStream.Read(_bytes, 0, 1);                    
                    if (outputLength < 1 || _bytes[0] == 10)
                        break;

                    bytes.AddRange(_bytes);
                }
                while (true);

                string line = Encoding.UTF8.GetString(bytes.ToArray());

                if (string.IsNullOrEmpty(line))
                    Context.Logger.Error("Read EOF from stdin");

                if (line == "end")
                    break;

                message.AppendLine(line);
            }
            while (true);

            return message.ToString();
        }

        public static void ReportError(string message)
        {
            SendMsgToParent("{\"command\": \"error\", \"msg\": \"" + message + "\"}");
        }

        public static void Ack(StormTuple tuple)
        {
            SendMsgToParent("{\"command\": \"ack\", \"id\": \"" + tuple.GetTupleId() + "\"}");
        }

        public static void Fail(StormTuple tuple)
        {
            SendMsgToParent("{\"command\": \"fail\", \"id\": \"" + tuple.GetTupleId() + "\"}");
        }

        public static void RpcMetrics(string name, string parms)
        {
            SendMsgToParent("{\"command\": \"metrics\", \"name\": \"" + name + "\", \"params\": \"" + parms + "\"}");
        }

        public static void Sync()
        {
            SendMsgToParent("{\"command\": \"sync\"}");
        }

        /// <summary>
        /// sent pid to storm and create a pid file
        /// </summary>
        /// <param name="heartBeatDir">the heart beat dir</param>
        private static void SendPid(string heartBeatDir)
        {
            Process currentProcess = Process.GetCurrentProcess();
            int pid = currentProcess.Id;
            File.WriteAllText(heartBeatDir + "/" + pid.ToString(), "");
            SendMsgToParent("{\"pid\": " + pid.ToString() + "}");            
        }

        private static Config GetConfig(JContainer configContainer)
        {
            Config config = new Config();

            foreach (var item in configContainer)
            {
                if (item.GetType() == typeof(JProperty))
                {
                    JProperty temp = item as JProperty;

                    if (temp.Value.GetType() == typeof(JValue))
                        config.StormConf.Add(temp.Name, (temp.Value as JValue).Value);
                }
            }
            return config;
        }

        private static TopologyContext GetContext(JContainer contextContainer)
        {
            try
            {
                int taskId = -1;
                Dictionary<int, string> component = new Dictionary<int, string>();

                var _taskId = contextContainer["taskid"];
                if (_taskId.GetType() == typeof(JValue))
                    Int32.TryParse((_taskId as JValue).Value.ToString(), out taskId);

                var _component = contextContainer["task->component"];
                if (_component != null && _component.GetType().BaseType == typeof(JContainer))
                {
                    foreach (var item in _component)
                    {
                        if (item.GetType() == typeof(JProperty))
                        {
                            JProperty temp = item as JProperty;

                            if (temp.Value.GetType() == typeof(JValue))
                                component.Add(Convert.ToInt32(temp.Name), (temp.Value as JValue).Value.ToString());
                        }
                    }
                }

                return new TopologyContext(taskId, "", component);

            }
            catch (Exception ex)
            {
                Context.Logger.Error(ex.ToString());
                return null;
            }
        }
    }

    public class Spout
    {
		private newPlugin _createDelegate;
		private ISpout _spout;
        public Spout(newPlugin createDelegate)
		{
			this._createDelegate = createDelegate;
		}
		public void Launch()
		{
			Context.Logger.Info("[Spout] Launch ...");
            ApacheStorm.ctx = new SpoutContext();
            IPlugin iPlugin = this._createDelegate(ApacheStorm.ctx);
			if (!(iPlugin is ISpout))
			{
				Context.Logger.Error("[Spout] newPlugin must return ISpout!");
			}
			this._spout = (ISpout)iPlugin;
            //call Open method.
            this._spout.Open(Context.Config, Context.TopologyContext);

			Stopwatch stopwatch = new Stopwatch();
			while (true)
			{
                try
                {
                    stopwatch.Start();
                    Command command = ApacheStorm.ReadCommand();
                    if (command.Name == "next")
                    {
                        this._spout.NextTuple();
                    }
                    else if (command.Name == "ack")
                    {
                        long seqId = long.Parse(command.Id);
                        this._spout.Ack(seqId);
                    }
                    else if (command.Name == "fail")
                    {
                        long seqId = long.Parse(command.Id);
                        this._spout.Fail(seqId);
                    }
                    else
                    {
                        Context.Logger.Error("[Spout] unexpected message.");
                    }
                    ApacheStorm.Sync();
                    stopwatch.Stop();
                }
                catch (Exception ex)
                {
                    Context.Logger.Error(ex.ToString());
                }
			}
		}
    }

    public class Bolt
    {
		private newPlugin _createDelegate;
		private IBolt _bolt;
		
		public Bolt(newPlugin createDelegate)
		{
			this._createDelegate = createDelegate;
		}
		public void Launch()
		{
			Context.Logger.Info("[Bolt] Launch ...");
            ApacheStorm.ctx = new BoltContext();
            IPlugin iPlugin = this._createDelegate(ApacheStorm.ctx);
			if (!(iPlugin is IBolt))
			{
				Context.Logger.Error("[Bolt] newPlugin must return IBolt!");
			}
			this._bolt = (IBolt)iPlugin;

            try
            {
                //call Prepare method.
                this._bolt.Prepare(Context.Config, Context.TopologyContext);

                while (true)
                {
                    StormTuple tuple = ApacheStorm.ReadTuple();
                    if (tuple.IsHeartBeatTuple())
                        ApacheStorm.Sync();
                    else
                    {
                        this._bolt.Execute(tuple);
                    }
                }
            }
            catch (Exception ex)
            {
                Context.Logger.Error(ex.ToString());
            }
		}
    }

    public class BasicBolt
    {
        private newPlugin _createDelegate;
        private IBasicBolt _bolt;

        public BasicBolt(newPlugin createDelegate)
        {
            this._createDelegate = createDelegate;
        }
        public void Launch()
        {
            Context.Logger.Info("[BasicBolt] Launch ...");
            ApacheStorm.ctx = new BoltContext();
            IPlugin iPlugin = this._createDelegate(ApacheStorm.ctx);
            if (!(iPlugin is IBasicBolt))
            {
                Context.Logger.Error("[BasicBolt] newPlugin must return IBasicBolt!");
            }
            this._bolt = (IBasicBolt)iPlugin;

            //call Prepare method.
            this._bolt.Prepare(Context.Config, Context.TopologyContext);

            while (true)
            {
                StormTuple tuple = ApacheStorm.ReadTuple();
                if (tuple.IsHeartBeatTuple())
                    ApacheStorm.Sync();
                else
                {
                    try
                    {
                        this._bolt.Execute(tuple);
                        ApacheStorm.ctx.Ack(tuple);
                    }
                    catch (Exception ex)
                    {
                        Context.Logger.Error(ex.ToString());
                        ApacheStorm.ctx.Fail(tuple);
                    }
                }
            }
        }
    }
}
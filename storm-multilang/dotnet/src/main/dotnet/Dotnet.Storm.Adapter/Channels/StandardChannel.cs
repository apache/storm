using System;
using System.Text;
using log4net;
using Dotnet.Storm.Adapter.Messaging;
using Dotnet.Storm.Adapter.Serializers;

namespace Dotnet.Storm.Adapter.Channels
{
    internal class StandardChannel : Channel
    {
        private readonly static ILog Logger = LogManager.GetLogger(typeof(Channel));

        public override void Send(OutMessage message)
        {
            Console.WriteLine(Serializer.Serialize(message));
            Console.WriteLine("end");
        }

        public override InMessage Receive<T>()
        {
            try
            {
                string message = ReadMessage();

                if (message.StartsWith("["))
                {
                    return Serializer.Deserialize<TaskIdsMessage>(message);
                }
                return Serializer.Deserialize<T>(message);
            }
            catch (ArgumentNullException ex)
            {
                Logger.Error($"{ex.Message}");
                throw ex;
            }
            catch (Exception ex)
            {
                //we're expecting this shouldn't happen
                Logger.Error($"Message parsing error: {ex}");
            }

            // just skip incorrect message
            return null;
        }

        private static string ReadMessage()
        {
            StringBuilder message = new StringBuilder();
            string line;
            do
            {
                line = Console.ReadLine();

                if (line == null)
                {
                    throw new ArgumentNullException("Storm is dead.");
                }
                if (line == "end")
                    break;

                if (!string.IsNullOrEmpty(line))
                {
                    message.AppendLine(line);
                }
            }
            while (true);

            return message.ToString();
        }
    }
}

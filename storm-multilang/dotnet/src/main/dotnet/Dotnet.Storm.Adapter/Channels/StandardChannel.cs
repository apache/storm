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

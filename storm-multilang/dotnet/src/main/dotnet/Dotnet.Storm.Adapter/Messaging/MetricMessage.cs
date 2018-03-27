using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotnet.Storm.Adapter.Messaging
{
    class MetricMessage : OutMessage
    {
        [JsonProperty("command")]
        public const string Command = "metrics";

        [JsonProperty("name")]
        public string Name { get; private set; }

        [JsonProperty("params")]
        public object Value { get; private set; }

        public MetricMessage(string name, object value)
        {
            Name = name;
            Value = value;
        }
    }
}

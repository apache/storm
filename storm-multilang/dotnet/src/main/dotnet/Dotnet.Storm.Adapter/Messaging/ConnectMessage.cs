using System.Collections.Generic;
using Newtonsoft.Json;
using Dotnet.Storm.Adapter.Components;

namespace Dotnet.Storm.Adapter.Messaging
{
    class ConnectMessage : InMessage
    {
        [JsonProperty("conf")]
        public IDictionary<string, object> Configuration { get; set; }

        [JsonProperty("pidDir")]
        public string PidDir { get; set; }

        [JsonProperty("context")]
        public StormContext Context { get; set; }
    }
}

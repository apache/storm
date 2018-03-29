using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotnet.Storm.Adapter.Messaging
{
    class SpoutTuple : EmitMessage
    {
        [JsonProperty("task")]
        public long Task { get; internal set; }

        [JsonProperty("id")]
        public string Id { get; internal set; }

        [JsonProperty("stream")]
        public string Stream { get; internal set; }

        [JsonProperty("tuple")]
        public List<object> Tuple { get; internal set; }

        [JsonProperty("need_task_ids")]
        public bool NeedTaskIds { get; internal set; }
    }
}

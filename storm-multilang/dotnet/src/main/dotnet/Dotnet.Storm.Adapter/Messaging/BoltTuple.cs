using Newtonsoft.Json;
using System.Collections.Generic;

namespace Dotnet.Storm.Adapter.Messaging
{
    class BoltTuple : EmitMessage
    {
        [JsonProperty("task")]
        public long Task { get; internal set; }

        [JsonProperty("stream")]
        public string Stream { get; internal set; }

        [JsonProperty("tuple")]
        public List<object> Tuple { get; internal set; }

        [JsonProperty("anchors")]
        public List<string> Anchors { get; internal set; }

        [JsonProperty("need_task_ids")]
        public bool NeedTaskIds { get; internal set; }
    }
}

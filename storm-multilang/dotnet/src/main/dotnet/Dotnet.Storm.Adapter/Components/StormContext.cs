using Newtonsoft.Json;
using System.Collections.Generic;

namespace Dotnet.Storm.Adapter.Components
{
    public class StormContext
    {
        [JsonProperty("task->component")]
        public IDictionary<long, string> TaskToComponent { get; set; }

        [JsonProperty("taskId")]
        public long TaskId { get; set; }

        [JsonProperty("componentId")]
        public string ComponentId { get; set; }

        [JsonProperty("stream->target->grouping")]
        public IDictionary<string, IDictionary<string, Grouping>> StreamToTargetToGrouping { get; set; }

        [JsonProperty("streams")]
        public List<string> Streams { get; set; }

        [JsonProperty("stream->outputfields")]
        public IDictionary<string, List<string>> StreamToOputputFields { get; set; }

        [JsonProperty("source->stream->grouping")]
        public IDictionary<string, IDictionary<string, Grouping>> SourceToStreamToGrouping { get; set; }

        [JsonProperty("source->stream->fields")]
        public IDictionary<string, IDictionary<string, List<string>>> SourceToStreamToFields { get; set; }

    }
}

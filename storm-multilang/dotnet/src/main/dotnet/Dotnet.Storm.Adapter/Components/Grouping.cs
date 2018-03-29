using Newtonsoft.Json;
using System.Collections.Generic;

namespace Dotnet.Storm.Adapter.Components
{
    public class Grouping
    {
        [JsonProperty("type")]
        public string Type { get; set; }

        [JsonProperty("fields", Required = Required.Default)]
        public List<string> Fields { get; set; }
    }
}

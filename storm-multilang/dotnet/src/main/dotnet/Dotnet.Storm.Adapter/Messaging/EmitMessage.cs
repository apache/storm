using Newtonsoft.Json;

namespace Dotnet.Storm.Adapter.Messaging
{
    abstract class EmitMessage : OutMessage
    {
        [JsonProperty("command")]
        public readonly string Command = "emit";
    }
}

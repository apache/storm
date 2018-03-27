using Newtonsoft.Json;

namespace Dotnet.Storm.Adapter.Messaging
{
    class SyncMessage : OutMessage
    {
        [JsonProperty("command")]
        public const string Command = "sync";
    }
}

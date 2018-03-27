using Newtonsoft.Json;

namespace Dotnet.Storm.Adapter.Messaging
{
    class AckMessage : OutMessage
    {
        [JsonProperty("command")]
        public const string Command = "ack";

        [JsonProperty("id")]
        public string Id { get; private set; }

        public AckMessage(string id)
        {
            Id = id;
        }
    }
}

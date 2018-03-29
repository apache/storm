using Newtonsoft.Json;


namespace Dotnet.Storm.Adapter.Messaging
{
    class CommandMessage : InMessage
    {
        [JsonProperty("command")]
        public string Command { get; set; }

        [JsonProperty("id")]
        public string Id { get; private set; }

        public CommandMessage(string id)
        {
            Id = id;
        }
    }
}

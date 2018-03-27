using Newtonsoft.Json;

namespace Dotnet.Storm.Adapter.Messaging
{
    class ErrorMessage : OutMessage
    {
        [JsonProperty("command")]
        public const string Command = "error";

        [JsonProperty("msg")]
        public string Message { get; }

        public ErrorMessage(string message)
        {
            Message = message;
        }
    }
}

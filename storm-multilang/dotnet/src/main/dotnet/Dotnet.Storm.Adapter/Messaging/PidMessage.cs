using Newtonsoft.Json;

namespace Dotnet.Storm.Adapter.Messaging
{
    class PidMessage : OutMessage
    {
        public PidMessage(int pid)
        {
            Pid = pid;
        }

        [JsonProperty("pid")]
        public int Pid { get; set; }
    }
}

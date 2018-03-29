using Newtonsoft.Json;

namespace Dotnet.Storm.Adapter.Messaging
{
    abstract class Message
    {
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}

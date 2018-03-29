using Newtonsoft.Json;

namespace Dotnet.Storm.Adapter.Serializers
{
    internal class JsonSerializer : Serializer
    {
        internal override T Deserialize<T>(string input)
        {
            return JsonConvert.DeserializeObject<T>(input);
        }

        internal override string Serialize<T>(T input)
        {
            return JsonConvert.SerializeObject(input);
        }
    }
}

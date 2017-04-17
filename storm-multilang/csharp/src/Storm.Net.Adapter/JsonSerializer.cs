using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Storm
{
    public class JsonSerializer : ISerializer
    {
        private static readonly BinaryFormatter binaryFormatter = new BinaryFormatter();
        private static readonly Type ByteArrayType = typeof(byte[]);
        private static readonly byte[] EmptyArray = new byte[0];

        public List<byte[]> Serialize(List<object> dataList)
        {
            return dataList.Select((object t, int i) => JsonSerializer.CSharpObjectToBytes(t)).ToList<byte[]>();
        }

        public List<object> Deserialize(List<byte[]> dataList, List<Type> targetTypes)
        {
            return dataList.Select((byte[] t, int i) => JsonSerializer.CSharpBytesToObject(t, targetTypes[i])).ToList<object>();
        }

        public static object CSharpBytesToObject(byte[] bytes, Type type)
        {
            if (bytes == null)
            {
                return null;
            }
            if (bytes.Count<byte>() == 0)
            {
                return null;
            }
            if (type == JsonSerializer.ByteArrayType)
            {
                return bytes;
            }

            return JsonConvert.DeserializeObject(Encoding.Default.GetString(bytes), type);
        }
        public static byte[] CSharpObjectToBytes(object obj)
        {
            if (obj == null)
            {
                return JsonSerializer.EmptyArray;
            }

            return Encoding.Default.GetBytes(JsonConvert.SerializeObject(obj));
        }
    }
}
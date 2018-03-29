using Dotnet.Storm.Adapter.Messaging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotnet.Storm.Adapter.Serializers
{
    internal abstract class Serializer
    {
        private static Serializer instance;

        public static Serializer Instange
        {
            get
            {
                return instance;
            }
            internal set
            {
                instance = value;
            }
        }

        internal abstract string Serialize<T>(T input) where T : OutMessage;

        internal abstract T Deserialize<T>(string input) where T : InMessage;
    }
}

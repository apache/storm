using Dotnet.Storm.Adapter.Messaging;
using Dotnet.Storm.Adapter.Serializers;

namespace Dotnet.Storm.Adapter.Channels
{
    internal abstract class Channel
    {
        private static Channel instance;

        public static Channel Instance
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

        internal Serializer Serializer { get; set; }

        public abstract void Send(OutMessage message);

        public abstract InMessage Receive<T>() where T : InMessage;
    }
}

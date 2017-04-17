using System.Collections.Generic;

namespace Storm
{
    public interface ISpout : IPlugin
    {
        void Open(Config stormConf, TopologyContext context);
        void NextTuple();
        void Ack(long seqId);
        void Fail(long seqId);
    }
}
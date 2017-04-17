namespace Storm
{
    public interface IBasicBolt : IPlugin
    {
        void Prepare(Config stormConf, TopologyContext context);
        void Execute(StormTuple tuple);
    }
}
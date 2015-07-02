namespace Storm
{
    public interface IBolt : IPlugin
    {
        void Prepare(Config stormConf, TopologyContext context);
        void Execute(StormTuple tuple);
        //void cleanup();
    }
}
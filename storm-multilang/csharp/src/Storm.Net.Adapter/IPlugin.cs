namespace Storm
{
    public interface IPlugin
    {
    }

    public delegate IPlugin newPlugin(Context ctx);
}

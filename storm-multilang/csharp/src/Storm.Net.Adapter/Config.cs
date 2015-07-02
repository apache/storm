using System.Collections.Generic;

namespace Storm
{
    public class Config
    {
        public Dictionary<string, object> StormConf
        {
            get;
            set;
        }

        public Config()
        {
            StormConf = new Dictionary<string, object>();
        }
    }
}
using System.Collections.Generic;

namespace Storm
{
    public class Command
    {
        /// <summary>
        /// command nanme
        /// </summary>
        public string Name { set; get; }
        /// <summary>
        /// id
        /// </summary>
        public string Id { set; get; }

        public string Component { set; get; }

        public string StreamId { set; get; }

        public int TaskId { set; get; }

        public List<object> Tuple { set; get; }
    }
}
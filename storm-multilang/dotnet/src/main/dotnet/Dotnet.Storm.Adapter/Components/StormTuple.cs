using Dotnet.Storm.Adapter.Messaging;
using System.Collections.Generic;

namespace Dotnet.Storm.Adapter.Components
{
    public class StormTuple
    {
        internal StormTuple(ExecuteTuple tuple)
        {
            Id = tuple.Id;
            Component = tuple.Component;
            TaskId = tuple.TaskId;
            Stream = tuple.Stream;
            Tuple = tuple.Tuple;
        }

        public string Id { get; internal set; }

        public string Component { get; internal set; }

        public string TaskId { get; internal set; }

        public string Stream { get; internal set; }

        public List<object> Tuple { get; internal set; }
    }
}

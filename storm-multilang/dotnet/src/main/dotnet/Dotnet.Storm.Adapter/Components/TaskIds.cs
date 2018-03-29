using System;
using System.Collections.Generic;

namespace Dotnet.Storm.Adapter.Components
{
    public class TaskIds : EventArgs
    {
        public IList<long> Ids { get; private set; }

        public TaskIds(IList<long> ids)
        {
            Ids = ids;
        }
    }
}

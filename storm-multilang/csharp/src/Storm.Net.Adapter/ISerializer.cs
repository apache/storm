using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Storm
{
    public interface ISerializer
    {
        List<byte[]> Serialize(List<object> dataList);

        List<object> Deserialize(List<byte[]> dataList, List<Type> targetTypes);
    }
}

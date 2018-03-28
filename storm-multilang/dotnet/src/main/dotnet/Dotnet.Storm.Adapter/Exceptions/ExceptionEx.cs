using System;
using System.Collections.Generic;
using System.Text;

namespace Dotnet.Storm.Adapter.Extensions
{
    public static class ExceptionEx
    {
        public static string GetMessageTrace(this Exception ex)
        {
            StringBuilder result = new StringBuilder();

            do
            {
                result.Append(ex.Message);
                ex = ex.InnerException;
            }
            while (ex != null);

            return result.ToString(); ;
        }

    }
}

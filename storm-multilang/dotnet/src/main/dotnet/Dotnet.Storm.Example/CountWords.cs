using Dotnet.Storm.Adapter.Components;
using log4net;
using System;
using System.Collections.Generic;

namespace Dotnet.Storm.Example
{
    public class CountWords : BaseBolt
    {
        private IDictionary<string, long> counts = new Dictionary<string, long>();

        protected override void Execute(StormTuple tuple)
        {
            string word = tuple.Tuple[0].ToString();

            if(counts.ContainsKey(word))
            {
                counts[word]++;
                Logger.Info($"{word} {counts[word]}");
            }
            else
            {
                counts.Add(word, 1);
                Logger.Info($"{word} 1");
            }
        }
    }
}

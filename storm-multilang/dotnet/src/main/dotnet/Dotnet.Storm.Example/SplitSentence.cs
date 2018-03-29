/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
using Dotnet.Storm.Adapter.Components;
using System;
using System.Collections.Generic;

namespace Dotnet.Storm.Example
{
    public class SplitSentence : BaseBolt
    {
        public SplitSentence() : base() => OnTaskIds += new EventHandler<TaskIds>(ProcessTaskIds);

        private void ProcessTaskIds(object sender, TaskIds ids)
        {
            Logger.Info($"Received task ids: {string.Join(',', ids.Ids)}");
        }

        protected override void Execute(StormTuple tuple)
        {
            char[] separator = new char[] { ' ', '[', ']', '<', '>', '(', ')', '.', ',' };
            char[] trimChars = new char[] { '.', ',' };

            string[] line = tuple.Tuple[0].ToString().Split(separator);

            if(line != null && line.Length > 0)
            {
                foreach(string word in line)
                {
                    if(!string.IsNullOrEmpty(word))
                    {
                        Storm.Emit(new List<object> { word.Trim(trimChars) }, "default", 0, new List<string>() { tuple.Id }, true);
                    }
                }
            }
        }
    }
}

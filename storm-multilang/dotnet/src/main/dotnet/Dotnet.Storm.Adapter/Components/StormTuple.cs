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

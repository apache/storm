﻿/**
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
using Newtonsoft.Json;
using System.Collections.Generic;

namespace Dotnet.Storm.Adapter.Messaging
{
    class BoltTuple : EmitMessage
    {
        [JsonProperty("task")]
        public long Task { get; internal set; }

        [JsonProperty("stream")]
        public string Stream { get; internal set; }

        [JsonProperty("tuple")]
        public List<object> Tuple { get; internal set; }

        [JsonProperty("anchors")]
        public List<string> Anchors { get; internal set; }

        [JsonProperty("need_task_ids")]
        public bool NeedTaskIds { get; internal set; }
    }
}

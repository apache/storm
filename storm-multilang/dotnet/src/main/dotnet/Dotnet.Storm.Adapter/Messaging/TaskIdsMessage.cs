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
using System.Collections;
using System.Collections.Generic;

namespace Dotnet.Storm.Adapter.Messaging
{
    class TaskIdsMessage : InMessage, IList<long>
    {
        public TaskIdsMessage(IList<long> ids)
        {
            list = ids;
        }

        private IList<long> list;

        public long this[int index] { get => list[index]; set => list[index] = value; }

        public int Count => list.Count;

        public bool IsReadOnly => true;

        public void Add(long item) => list.Add(item);

        public void Clear() => list.Clear();

        public bool Contains(long item) => list.Contains(item);

        public void CopyTo(long[] array, int arrayIndex) => list.CopyTo(array, arrayIndex);

        public IEnumerator<long> GetEnumerator() => list.GetEnumerator();

        public int IndexOf(long item) => list.IndexOf(item);

        public void Insert(int index, long item) => list.Insert(index, item);

        public bool Remove(long item) => list.Remove(item);

        public void RemoveAt(int index) => list.RemoveAt(index);

        IEnumerator IEnumerable.GetEnumerator() => list.GetEnumerator();
    }
}

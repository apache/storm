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

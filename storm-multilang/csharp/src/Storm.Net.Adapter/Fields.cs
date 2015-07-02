using System;
using System.Collections.Generic;

namespace Storm
{
    public class Fields
    {
        private List<string> _fields;
        private Dictionary<string, int> _index = new Dictionary<string, int>();
        public Fields(params string[] fields)
        {
            List<string> _fields = new List<string>();
            _fields.AddRange(fields);
            this.Init(_fields);
        }
        public Fields(List<string> inputFields)
        {
            this.Init(inputFields);
        }
        private void Init(List<string> inputFields)
        {
            this._fields = new List<string>(inputFields.Count);
            foreach (string current in inputFields)
            {
                if (this._fields.Contains(current))
                {
                    throw new ArgumentException(string.Format("duplicate field {0}", current));
                }
                this._fields.Add(current);
            }
            this.Index();
        }
        public List<object> Select(Fields selector, List<object> tuple)
        {
            List<object> list = new List<object>(selector.Size());
            foreach (string current in selector.ToList())
            {
                list.Add(tuple[this._index[current]]);
            }
            return list;
        }
        public List<string> ToList()
        {
            return new List<string>(this._fields);
        }
        public int Size()
        {
            return this._fields.Count;
        }
        public string Get(int index)
        {
            return this._fields[index];
        }
        public int FieldIndex(string field)
        {
            return this._index[field];
        }
        public bool Contains(string field)
        {
            return this._index.ContainsKey(field);
        }
        private void Index()
        {
            for (int i = 0; i < this._fields.Count; i++)
            {
                this._index[this._fields[i]] = i;
            }
        }
        public override string ToString()
        {
            return this._fields.ToString();
        }
    }
}
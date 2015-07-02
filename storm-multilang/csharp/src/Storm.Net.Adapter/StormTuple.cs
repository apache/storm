using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Storm
{
    public class StormTuple
    {
        private List<object> values = new List<object>();
        private int taskId;
        private string streamId;
        private string component;
        private string tupleId;

        public StormTuple(List<object> tuple, int taskId, string streamId, string tupleId, string component)
        {
            this.values = tuple;
            this.taskId = taskId;
            this.streamId = streamId;
            this.component = component;
            this.tupleId = tupleId;
        }

        public Boolean IsHeartBeatTuple()
        {
            return this.taskId == -1 && this.streamId == "__heartbeat";
        }

        public override string ToString()
        {
            return base.ToString();
        }

        public int Size()
        {
            return this.values.Count;
        }

        public object GetValue(int i)
        {
            return this.values[i];
        }
        public string GetString(int i)
        {
            return (string)this.values[i];
        }
        public int GetInteger(int i)
        {
            return (int)this.values[i];
        }
        public long GetLong(int i)
        {
            return (long)this.values[i];
        }
        public bool GetBoolean(int i)
        {
            return (bool)this.values[i];
        }
        public short GetShort(int i)
        {
            return (short)this.values[i];
        }
        public byte GetByte(int i)
        {
            return (byte)this.values[i];
        }
        public double GetDouble(int i)
        {
            return (double)this.values[i];
        }
        public float GetFloat(int i)
        {
            return (float)this.values[i];
        }
        public byte[] GetBinary(int i)
        {
            return (byte[])this.values[i];
        }
        public List<object> GetValues()
        {
            return this.values;
        }
        public int GetSourceTask()
        {
            return this.taskId;
        }
        public string GetSourceStreamId()
        {
            return this.streamId;
        }
        public string GetTupleId()
        {
            return this.tupleId;
        }
        public string GetComponent()
        {
            return this.component;
        }
    }
}
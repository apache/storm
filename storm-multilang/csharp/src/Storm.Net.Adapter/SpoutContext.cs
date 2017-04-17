using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Storm
{
    public class SpoutContext : Context
    {
        public override void Emit(List<object> values, string taskId = null)
        {
            Context.Logger.Error("[SpoutContext] Bolt can not call this function!");
        }

        public override void Emit(List<StormTuple> anchors, List<object> values, string taskId = null)
        {
            Context.Logger.Error("[SpoutContext] Bolt can not call this function!");
        }

        public override void Emit(List<object> values, long seqId, string taskId = null)
        {
            this.Emit("default", values, seqId, taskId);
        }

        public override void Emit(string streamId, List<object> values, string taskId = null)
        {
            Context.Logger.Error("[SpoutContext] Bolt can not call this function!");
        }

        public override void Emit(string streamId, List<object> values, long seqId, string taskId = null)
        {
            base.CheckOutputSchema(streamId, values == null ? 0 : values.Count);

            if (string.IsNullOrEmpty(taskId))
            {
                string msg = @"""command"": ""emit"", ""id"": ""{0}"", ""stream"": ""{1}"", ""tuple"": {2}";
                ApacheStorm.SendMsgToParent("{" + string.Format(msg, seqId.ToString(), streamId, JsonConvert.SerializeObject(values)) + "}");
                ApacheStorm.ReadTaskId();
            }
            else
            {
                string msg = @"""command"": ""emit"", ""id"": ""{0}"", ""stream"": ""{1}"", ""task"": {2}, ""tuple"": {3}";
                ApacheStorm.SendMsgToParent("{" + string.Format(msg, seqId.ToString(), streamId, taskId, JsonConvert.SerializeObject(values)) + "}");
            }
        }

        public override void Emit(string streamId, List<StormTuple> anchors, List<object> tuple, string taskId = null)
        {
            Context.Logger.Error("[SpoutContext] Bolt can not call this function!");
        }

        public override void Ack(StormTuple tuple)
        {
            Context.Logger.Error("[SpoutContext] Bolt can not call this function!");
        }

        public override void Fail(StormTuple tuple)
        {
            Context.Logger.Error("[SpoutContext] Bolt can not call this function!");
        }

        public SpoutContext()
        {
        }
    }
}
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Storm
{
    public abstract class Context
    {
        public static PluginType pluginType;
        internal ComponentStreamSchema _schemaByCSharp;
        public static Logger Logger = new Logger();

        public static TopologyContext TopologyContext
        {
            set;
            get;
        }

        public static Config Config
        {
            get;
            set;
        }

        public abstract void Emit(List<object> values, string taskId = null);
        public abstract void Emit(List<StormTuple> anchors, List<object> values, string taskId = null);
        public abstract void Emit(List<object> values, long seqId, string taskId = null);
        public abstract void Emit(string streamId, List<object> values, string taskId = null);
        public abstract void Emit(string streamId, List<object> values, long seqId, string taskId = null);
        public abstract void Emit(string streamId, List<StormTuple> anchors, List<object> values, string taskId = null);
        public abstract void Ack(StormTuple tuple);
        public abstract void Fail(StormTuple tuple);

        public void DeclareComponentSchema(ComponentStreamSchema schema)
        {
            this._schemaByCSharp = schema;
        }

        internal void CheckOutputSchema(string streamId, int tupleFieldCount)
        {
            if (this._schemaByCSharp == null || this._schemaByCSharp.OutputStreamSchema == null)
            {
                Context.Logger.Error(string.Format("[Context.generateProxyMessage()] NO output schema declared!", new object[0]));
            }
            if (!this._schemaByCSharp.OutputStreamSchema.ContainsKey(streamId))
            {
                Context.Logger.Error(string.Format("[Context.generateProxyMessage()] NO output schema specified for {0}!", streamId));
            }
            if (this._schemaByCSharp.OutputStreamSchema[streamId].Count != tupleFieldCount)
            {
                Context.Logger.Error(string.Format("[Context.generateProxyMessage()] out schema ({0} fields) MISMATCH with data ({1} fields) for streamId: {2}!", this._schemaByCSharp.OutputStreamSchema.Count, tupleFieldCount, streamId));
            }
        }
        
        internal void CheckInputSchema(string streamId, int tupleFieldCount)
        {
            if (this._schemaByCSharp == null)
            {
                Context.Logger.Error("[Context.CheckInputSchema()] null schema");
            }
            if (this._schemaByCSharp.InputStreamSchema == null)
            {
                Context.Logger.Error("[Context.CheckInputSchema()] null schema.InputStreamSchema");
            }
            if (!this._schemaByCSharp.InputStreamSchema.ContainsKey(streamId))
            {
                Context.Logger.Error(string.Format("[Context.CheckInputSchema()] unkown streamId: {0}", streamId));
            }
            if (this._schemaByCSharp.InputStreamSchema[streamId].Count != tupleFieldCount)
            {
                Context.Logger.Error(string.Format("[Context.CheckInputSchema()] count mismatch, streamId: {0}, SchemaFieldCount: {1}, tupleFieldCount: {2}", streamId, this._schemaByCSharp.InputStreamSchema[streamId].Count, tupleFieldCount));
            }
        }
    }

    public class ComponentStreamSchema
    {
        public Dictionary<string, List<Type>> InputStreamSchema
        {
            get;
            set;
        }
        public Dictionary<string, List<Type>> OutputStreamSchema
        {
            get;
            set;
        }
        public ComponentStreamSchema(Dictionary<string, List<Type>> input, Dictionary<string, List<Type>> output)
        {
            this.InputStreamSchema = input;
            this.OutputStreamSchema = output;
        }
    }
}
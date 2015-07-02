using System;
using System.Collections.Generic;

namespace Storm
{
    public class TopologyContext
    {
        private int _taskId;
        private string _topologyId = "";
        private string _topologyName = "";
        private Dictionary<int, string> _taskToComponent;
        private Dictionary<string, List<int>> _componentToTasks;
        private Dictionary<string, Dictionary<string, Fields>> _componentToStreamToFields;
        public int TaskId
        {
            get
            {
                return this._taskId;
            }
        }
        public string TopologyId
        {
            get
            {
                return this._topologyId;
            }
        }
        public string TopologyName
        {
            get
            {
                return this._topologyName;
            }
        }
        public TopologyContext(int taskId, string topologyId, Dictionary<int, string> tasktoComponent, Dictionary<string, Dictionary<string, Fields>> componentToStreamToFields)
        {
            this._taskId = taskId;
            this._topologyId = topologyId;
            this._taskToComponent = tasktoComponent;
            this._componentToStreamToFields = componentToStreamToFields;
            string[] array = this._topologyId.Split(new string[]
                {
                "-"
                }, StringSplitOptions.RemoveEmptyEntries);
            this._topologyName = (array.Length > 0 ? array[0] : "");
        }
        public TopologyContext(int taskId, string topologyId, Dictionary<int, string> tasktoComponent)
            : this(taskId, topologyId, tasktoComponent, null)
        {
            if (this._componentToTasks == null)
            {
                this._componentToTasks = new Dictionary<string, List<int>>();
            }
            foreach (KeyValuePair<int, string> current in tasktoComponent)
            {
                if (!this._componentToTasks.ContainsKey(current.Value))
                {
                    this._componentToTasks.Add(current.Value, new List<int>());
                }
                this._componentToTasks[current.Value].Add(current.Key);
            }
            foreach (KeyValuePair<string, List<int>> current2 in this._componentToTasks)
            {
                current2.Value.Sort();
            }
        }
        public int GetThisTaskId()
        {
            return this._taskId;
        }
        public int GetThisTaskIndex()
        {
            List<int> componentTasks = this.GetComponentTasks(this.GetThisComponentId());
            componentTasks.Sort();
            int result = -1;
            for (int i = 0; i < componentTasks.Count; i++)
            {
                if (this._taskId == componentTasks[i])
                {
                    result = i;
                    break;
                }
            }
            return result;
        }
        public string GetComponentId(int taskId)
        {
            if (taskId == -1)
            {
                return "__system";
            }
            return this._taskToComponent[taskId];
        }
        public string GetThisComponentId()
        {
            return this.GetComponentId(this._taskId);
        }
        public List<int> GetComponentTasks(string componentId)
        {
            if (this._componentToTasks.ContainsKey(componentId))
            {
                List<int> collection = this._componentToTasks[componentId];
                return new List<int>(collection);
            }
            return new List<int>();
        }
    }
}
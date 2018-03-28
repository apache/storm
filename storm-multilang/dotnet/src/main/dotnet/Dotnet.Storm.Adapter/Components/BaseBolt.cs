using Dotnet.Storm.Adapter.Extensions;
using Dotnet.Storm.Adapter.Messaging;
using System;
using System.Collections.Generic;

namespace Dotnet.Storm.Adapter.Components
{
    public abstract class BaseBolt : Component
    {
        protected new class LocalStorm : Component.LocalStorm
        {
            public void Ack(string id)
            {
                GlobalStorm.Send(new AckMessage(id));
            }

            public void Fail(string id)
            {
                GlobalStorm.Send(new FailMessage(id));
            }

            public void Emit(List<object> tuple, string stream = "default", long task = 0, List<string> anchors = null, bool needTaskIds = false)
            {
                VerificationResult result = VerifyOutput(stream, tuple);

                if (result.IsError)
                {
                    Logger.Error($"{result} for nex tuple: {tuple}.");
                }
                else
                {
                    BoltTuple message = new BoltTuple()
                    {
                        Task = task,
                        Stream = stream,
                        Tuple = tuple,
                        Anchors = anchors,
                        NeedTaskIds = needTaskIds
                    };

                    GlobalStorm.Send(message);
                }
            }
        }

        private bool running = true;

        public BaseBolt()
        {
            Storm = new LocalStorm();
        }

        internal override void Start()
        {
            while(running)
            {
                Message message = GlobalStorm.Receive<ExecuteTuple>();
                if (message != null)
                {
                    // there are only two options: task_ids and tuple to execute
                    if (message is TaskIdsMessage ids)
                    {
                        OnTaskIds(this, new TaskIds(ids));
                    }
                    if (message is ExecuteTuple tuple)
                    {
                        VerificationResult result = Storm.VerifyInput(tuple.Component, tuple.Stream, tuple.Tuple);

                        if (result.IsError)
                        {
                            Logger.Error($"Validation failed: {result} in {tuple}.");
                        }
                        else
                        {
                            switch (tuple.Stream)
                            {
                                case "__heartbeat":
                                    DoHeartbeat();
                                    break;
                                case "__tick":
                                    DoTick();
                                    break;
                                default:
                                    DoExecute(tuple);
                                    break;
                            }
                        }
                    }
                }
            }
        }

        private void DoHeartbeat()
        {
            Storm.Sync();
        }

        private void DoTick()
        {
            OnTick?.Invoke(this, EventArgs.Empty);
        }

        private void DoExecute(ExecuteTuple tuple)
        {
            try
            {
                Execute(new StormTuple(tuple));
                if (IsGuarantee)
                {
                    Storm.Ack(tuple.Id);
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to process tuple. {ex.GetMessageTrace()}");
                if (IsGuarantee)
                {
                    Storm.Fail(tuple.Id);
                }
            }
        }

        #region Bolt interface
        protected new LocalStorm Storm { get; private set; }

        protected abstract void Execute(StormTuple tuple);

        protected event EventHandler<EventArgs> OnTick;

        protected event EventHandler<TaskIds> OnTaskIds;
        #endregion
    }
}

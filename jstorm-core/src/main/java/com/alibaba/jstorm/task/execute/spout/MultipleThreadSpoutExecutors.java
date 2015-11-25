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
package com.alibaba.jstorm.task.execute.spout;

import backtype.storm.task.TopologyContext;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.WorkerClassLoader;
import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.metric.JStormMetricsReporter;
import com.alibaba.jstorm.task.Task;
import com.alibaba.jstorm.task.TaskBaseMetric;
import com.alibaba.jstorm.task.TaskStatus;
import com.alibaba.jstorm.task.TaskTransfer;
import com.alibaba.jstorm.task.acker.Acker;
import com.alibaba.jstorm.task.comm.TaskSendTargets;
import com.alibaba.jstorm.task.comm.TupleInfo;
import com.alibaba.jstorm.task.error.ITaskReportErr;
import com.alibaba.jstorm.utils.RotatingMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * spout executor
 * <p/>
 * All spout actions will be done here
 * 
 * @author yannian/Longda
 */
public class MultipleThreadSpoutExecutors extends SpoutExecutors {
    private static Logger LOG = LoggerFactory.getLogger(MultipleThreadSpoutExecutors.class);

    public MultipleThreadSpoutExecutors(Task task) {
        super(task);

        ackerRunnableThread = new AsyncLoopThread(new AckerRunnable(), false, Thread.NORM_PRIORITY, false);
    }
    
    public void mkPending() {
    	pending = new RotatingMap<Long, TupleInfo>(Acker.TIMEOUT_BUCKET_NUM, null, false);
    }
    
    @Override 
    public void init() throws Exception {
    	super.init();
    	ackerRunnableThread.start();
    }

    @Override
    public String getThreadName() {
        return idStr + "-" + MultipleThreadSpoutExecutors.class.getSimpleName();
    }

	@Override
	public void run() {
		if (isFinishInit == false) {
			initWrapper();
		}

		super.nextTuple();
	}

    class AckerRunnable extends RunnableCallback {

        private AtomicBoolean shutdown = AsyncLoopRunnable.getShutdown();

        @Override
        public String getThreadName() {
            return idStr + "-" + AckerRunnable.class.getSimpleName();
        }

        @Override
        public void preRun() {
            WorkerClassLoader.switchThreadContext();
        }

        @Override
        public void postRun() {
            WorkerClassLoader.restoreThreadContext();
        }

        @Override
        public void run() {
            LOG.info("Successfully start Spout's acker thread " + idStr);

            while (shutdown.get() == false) {

                try {
                    exeQueue.consumeBatchWhenAvailable(MultipleThreadSpoutExecutors.this);
                    processControlEvent();
                } catch (Exception e) {
                    if (shutdown.get() == false) {
                        LOG.error("Actor occur unknow exception ", e);
                        report_error.report(e);
                    }
                }

            }

            LOG.info("Successfully shutdown Spout's acker thread " + idStr);
        }

        public Object getResult() {
            LOG.info("Begin to shutdown Spout's acker thread " + idStr);
            return -1;
        }

    }

}

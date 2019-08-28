/*
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

package org.apache.storm.task;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.generated.ShellComponent;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.rpc.IShellMetric;
import org.apache.storm.multilang.BoltMsg;
import org.apache.storm.multilang.ShellMsg;
import org.apache.storm.shade.com.google.common.util.concurrent.MoreExecutors;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.ObjectReader;
import org.apache.storm.utils.ShellBoltMessageQueue;
import org.apache.storm.utils.ShellLogHandler;
import org.apache.storm.utils.ShellProcess;
import org.apache.storm.utils.ShellUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bolt that shells out to another process to process tuples. ShellBolt communicates with that process over stdio using a special
 * protocol. An ~100 line library is required to implement that protocol, and adapter libraries currently exist for Ruby and Python.
 *
 * <p>To run a ShellBolt on a cluster, the scripts that are shelled out to must be in the resources directory within the
 * jar submitted to the master. During development/testing on a local machine, that resources directory just needs to be
 * on the classpath.
 *
 * <p>When creating topologies using the Java API, subclass this bolt and implement the IRichBolt interface to create components for the
 * topology that use other languages. For example:
 *
 *
 * <p>```java public class MyBolt extends ShellBolt implements IRichBolt { public MyBolt() { super("python", "mybolt.py"); }
 *
 * <p>public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields("field1", "field2")); } } ```
 */
public class ShellBolt implements IBolt {
    public static final String HEARTBEAT_STREAM_ID = "__heartbeat";
    public static final Logger LOG = LoggerFactory.getLogger(ShellBolt.class);
    private static final long serialVersionUID = -339575186639193348L;

    OutputCollector collector;
    Map<String, Tuple> inputs = new ConcurrentHashMap<>();

    private String[] command;
    private Map<String, String> env = new HashMap<>();
    private ShellLogHandler logHandler;
    private ShellProcess process;
    private volatile boolean running = true;
    private volatile Throwable exception;
    private ShellBoltMessageQueue pendingWrites = new ShellBoltMessageQueue();
    private Random rand;

    private Thread readerThread;
    private Thread writerThread;

    private TopologyContext context;

    private int workerTimeoutMills;
    private ScheduledExecutorService heartBeatExecutorService;
    private AtomicLong lastHeartbeatTimestamp = new AtomicLong();
    private AtomicBoolean sendHeartbeatFlag = new AtomicBoolean(false);
    private boolean isLocalMode = false;
    private boolean changeDirectory = true;

    public ShellBolt(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }

    public ShellBolt(String... command) {
        this.command = command;
    }

    public ShellBolt setEnv(Map<String, String> env) {
        this.env = env;
        return this;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public boolean shouldChangeChildCWD() {
        return changeDirectory;
    }

    /**
     * Set if the current working directory of the child process should change to the resources dir from extracted from the jar, or if it
     * should stay the same as the worker process to access things from the blob store.
     *
     * @param changeDirectory true change the directory (default) false leave the directory the same as the worker process.
     */
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public void changeChildCWD(boolean changeDirectory) {
        this.changeDirectory = changeDirectory;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        final OutputCollector collector) {
        if (ConfigUtils.isLocalMode(topoConf)) {
            isLocalMode = true;
        }
        Object maxPending = topoConf.get(Config.TOPOLOGY_SHELLBOLT_MAX_PENDING);
        if (maxPending != null) {
            this.pendingWrites = new ShellBoltMessageQueue(((Number) maxPending).intValue());
        }

        rand = new Random();
        this.collector = collector;

        this.context = context;

        if (topoConf.containsKey(Config.TOPOLOGY_SUBPROCESS_TIMEOUT_SECS)) {
            workerTimeoutMills = 1000 * ObjectReader.getInt(topoConf.get(Config.TOPOLOGY_SUBPROCESS_TIMEOUT_SECS));
        } else {
            workerTimeoutMills = 1000 * ObjectReader.getInt(topoConf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS));
        }

        process = new ShellProcess(command);
        if (!env.isEmpty()) {
            process.setEnv(env);
        }

        //subprocesses must send their pid first thing
        Number subpid = process.launch(topoConf, context, changeDirectory);
        LOG.info("Launched subprocess with pid " + subpid);

        logHandler = ShellUtils.getLogHandler(topoConf);
        logHandler.setUpContext(ShellBolt.class, process, this.context);

        // reader
        readerThread = new Thread(new BoltReaderRunnable());
        readerThread.start();

        writerThread = new Thread(new BoltWriterRunnable());
        writerThread.start();

        LOG.info("Start checking heartbeat...");
        setHeartbeat();

        heartBeatExecutorService = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));
        heartBeatExecutorService.scheduleAtFixedRate(new BoltHeartbeatTimerTask(this), 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void execute(Tuple input) {
        if (exception != null) {
            throw new RuntimeException(exception);
        }

        //just need an id
        String genId = Long.toString(rand.nextLong());
        inputs.put(genId, input);
        try {
            BoltMsg boltMsg = createBoltMessage(input, genId);

            pendingWrites.putBoltMsg(boltMsg);
        } catch (InterruptedException e) {
            // It's likely that Bolt is shutting down so no need to throw RuntimeException
            // just ignore
        }
    }

    private BoltMsg createBoltMessage(Tuple input, String genId) {
        BoltMsg boltMsg = new BoltMsg();
        boltMsg.setId(genId);
        boltMsg.setComp(input.getSourceComponent());
        boltMsg.setStream(input.getSourceStreamId());
        boltMsg.setTask(input.getSourceTask());
        boltMsg.setTuple(input.getValues());
        return boltMsg;
    }

    @Override
    public void cleanup() {
        running = false;
        heartBeatExecutorService.shutdownNow();
        writerThread.interrupt();
        readerThread.interrupt();
        process.destroy();
        inputs.clear();
    }

    private void handleAck(Object id) {
        Tuple acked = inputs.remove(id);
        if (acked == null) {
            throw new RuntimeException("Acked a non-existent or already acked/failed id: " + id);
        }
        collector.ack(acked);
    }

    private void handleFail(Object id) {
        Tuple failed = inputs.remove(id);
        if (failed == null) {
            throw new RuntimeException("Failed a non-existent or already acked/failed id: " + id);
        }
        collector.fail(failed);
    }

    private void handleError(String msg) {
        collector.reportError(new Exception("Shell Process Exception: " + msg));
    }

    private void handleEmit(ShellMsg shellMsg) throws InterruptedException {
        List<Tuple> anchors = new ArrayList<>();
        List<String> recvAnchors = shellMsg.getAnchors();
        if (recvAnchors != null) {
            for (String anchor : recvAnchors) {
                Tuple t = inputs.get(anchor);
                if (t == null) {
                    throw new RuntimeException("Anchored onto " + anchor + " after ack/fail");
                }
                anchors.add(t);
            }
        }

        if (shellMsg.getTask() == 0) {
            List<Integer> outtasks = collector.emit(shellMsg.getStream(), anchors, shellMsg.getTuple());
            if (shellMsg.areTaskIdsNeeded()) {
                pendingWrites.putTaskIds(outtasks);
            }
        } else {
            collector.emitDirect((int) shellMsg.getTask(),
                                  shellMsg.getStream(), anchors, shellMsg.getTuple());
        }
    }

    private void handleMetrics(ShellMsg shellMsg) {
        //get metric name
        String name = shellMsg.getMetricName();
        if (name.isEmpty()) {
            throw new RuntimeException("Receive Metrics name is empty");
        }

        //get metric by name
        IMetric metric = context.getRegisteredMetricByName(name);
        if (metric == null) {
            throw new RuntimeException("Could not find metric by name[" + name + "] ");
        }
        if (!(metric instanceof IShellMetric)) {
            throw new RuntimeException("Metric[" + name + "] is not IShellMetric, can not call by RPC");
        }
        IShellMetric shellMetric = (IShellMetric) metric;

        //call updateMetricFromRPC with params
        Object paramsObj = shellMsg.getMetricParams();
        try {
            shellMetric.updateMetricFromRPC(paramsObj);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setHeartbeat() {
        lastHeartbeatTimestamp.set(System.currentTimeMillis());
    }

    private long getLastHeartbeat() {
        return lastHeartbeatTimestamp.get();
    }

    private void die(Throwable exception) {
        String processInfo = process.getProcessInfoString() + process.getProcessTerminationInfoString();
        this.exception = new RuntimeException(processInfo, exception);
        String message = String.format("Halting process: ShellBolt died. Command: %s, ProcessInfo %s",
                                       Arrays.toString(command),
                                       processInfo);
        LOG.error(message, exception);
        collector.reportError(exception);
        if (!isLocalMode && (running || (exception instanceof Error))) { //don't exit if not running, unless it is an Error
            System.exit(11);
        }
    }

    private class BoltHeartbeatTimerTask extends TimerTask {
        private ShellBolt bolt;

        BoltHeartbeatTimerTask(ShellBolt bolt) {
            this.bolt = bolt;
        }

        @Override
        public void run() {
            long currentTimeMillis = System.currentTimeMillis();
            long lastHeartbeat = getLastHeartbeat();

            LOG.debug("BOLT - current time : {}, last heartbeat : {}, worker timeout (ms) : {}",
                      currentTimeMillis, lastHeartbeat, workerTimeoutMills);

            if (currentTimeMillis - lastHeartbeat > workerTimeoutMills) {
                bolt.die(new RuntimeException("subprocess heartbeat timeout"));
            }

            sendHeartbeatFlag.compareAndSet(false, true);
        }
    }

    private class BoltReaderRunnable implements Runnable {
        @Override
        public void run() {
            while (running) {
                try {
                    ShellMsg shellMsg = process.readShellMsg();

                    String command = shellMsg.getCommand();
                    if (command == null) {
                        throw new IllegalArgumentException("Command not found in bolt message: " + shellMsg);
                    }

                    setHeartbeat();

                    // We don't need to take care of sync, cause we're always updating heartbeat
                    switch (command) {
                        case "ack":
                            handleAck(shellMsg.getId());
                            break;
                        case "fail":
                            handleFail(shellMsg.getId());
                            break;
                        case "error":
                            handleError(shellMsg.getMsg());
                            break;
                        case "log":
                            logHandler.log(shellMsg);
                            break;
                        case "emit":
                            handleEmit(shellMsg);
                            break;
                        case "metrics":
                            handleMetrics(shellMsg);
                            break;
                        default:
                            break;
                    }
                } catch (InterruptedException e) {
                    // It's likely that Bolt is shutting down so no need to die.
                    // just ignore and loop will be terminated eventually
                } catch (Throwable t) {
                    die(t);
                }
            }
        }
    }

    private class BoltWriterRunnable implements Runnable {
        @Override
        public void run() {
            while (running) {
                try {
                    if (sendHeartbeatFlag.get()) {
                        LOG.debug("BOLT - sending heartbeat request to subprocess");

                        String genId = Long.toString(rand.nextLong());
                        process.writeBoltMsg(createHeartbeatBoltMessage(genId));
                        sendHeartbeatFlag.compareAndSet(true, false);
                    }

                    Object write = pendingWrites.poll(1, SECONDS);
                    if (write instanceof BoltMsg) {
                        process.writeBoltMsg((BoltMsg) write);
                    } else if (write instanceof List<?>) {
                        process.writeTaskIds((List<Integer>) write);
                    } else if (write != null) {
                        throw new RuntimeException(
                            "Unknown class type to write: " + write.getClass().getName());
                    }
                } catch (InterruptedException e) {
                    // It's likely that Bolt is shutting down so no need to die.
                    // just ignore and loop will be terminated eventually
                } catch (Throwable t) {
                    die(t);
                }
            }
        }

        private BoltMsg createHeartbeatBoltMessage(String genId) {
            BoltMsg msg = new BoltMsg();
            msg.setId(genId);
            msg.setTask(Constants.SYSTEM_TASK_ID);
            msg.setStream(HEARTBEAT_STREAM_ID);
            msg.setTuple(new ArrayList<>());
            return msg;
        }
    }
}

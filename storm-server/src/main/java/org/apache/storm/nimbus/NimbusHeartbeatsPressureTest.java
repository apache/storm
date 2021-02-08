/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.nimbus;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.storm.Config;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.SupervisorWorkerHeartbeat;
import org.apache.storm.generated.SupervisorWorkerHeartbeats;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

/**
 * Test for nimbus heartbeats max throughput, This is a client to collect the statistics.
 */
public class NimbusHeartbeatsPressureTest {
    /**
     * the args below can be configured.
     */
    private static String NIMBUS_HOST = "localhost";
    private static int NIMBUS_PORT = 6627;

    private static int THREADS_NUM = 50;
    private static int THREAD_SUBMIT_NUM = 1;
    private static int MOCKED_STORM_NUM = 5000;
    private static volatile boolean[] readyFlags = new boolean[THREADS_NUM];
    private static Random rand = new Random(47);
    private static List<double[]> totalCostTimesBook = new ArrayList<>();

    static {
        for (int i = 0; i < THREADS_NUM; i++) {
            readyFlags[i] = false;
        }
    }

    /**
     * Initialize a fake config.
     * @return conf
     */
    private static Config initializedConfig() {
        Config conf = new Config();
        conf.putAll(Utils.readDefaultConfig());
        ArrayList<String> nimbusSeeds = new ArrayList<>();
        nimbusSeeds.add(NIMBUS_HOST);

        conf.put(Config.NIMBUS_SEEDS, nimbusSeeds);
        conf.put(Config.NIMBUS_THRIFT_PORT, NIMBUS_PORT);
        return conf;
    }

    /**
     * Test max throughput with the specific config args.
     */
    public static void testMaxThroughput() {
        ExecutorService service = Executors.newFixedThreadPool(THREADS_NUM);

        long submitStart = System.currentTimeMillis();
        for (int i = 0; i < THREADS_NUM; i++) {
            service.submit(new HeartbeatSendTask(i, THREAD_SUBMIT_NUM));
        }
        long submitEnd = System.currentTimeMillis();
        println(THREADS_NUM + " tasks, " + THREAD_SUBMIT_NUM * THREADS_NUM + " submit cost "
                + (submitEnd - submitStart) / 1000D + "seconds");
        long totalStart = System.currentTimeMillis();
        while (!allTasksReady()) {
            try {

                Thread.sleep(10L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long totalEnd = System.currentTimeMillis();
        println(THREADS_NUM + " tasks, " + THREAD_SUBMIT_NUM * THREADS_NUM
                + " requests cost " + (totalEnd - totalStart) / 1000D + "seconds");
        printStatistics(totalCostTimesBook);
        try {
            service.shutdownNow();
            service.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static boolean allTasksReady() {
        for (boolean ready : readyFlags) {
            if (!ready) {
                return false;
            }
        }
        return true;
    }

    private static void println(Object msg) {
        if (msg instanceof Collection) {
            Iterator itr = ((Collection) msg).iterator();
            while (itr.hasNext()) {
                System.out.println(itr.next());
            }
        } else {
            System.out.println(msg);
        }
    }

    private static void printTimeCostArray(Double[] array) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (int i = 0; i < array.length; i++) {
            if (i != array.length - 1) {
                builder.append(array[i] + ",");
            } else {
                builder.append(array[i] + "");
            }
        }
        builder.append("]");
        System.out.println(builder.toString());
    }

    private static void printStatistics(List<double[]> data) {

        List<Double> totalPoints = new ArrayList<>();
        double total = 0D;
        for (double[] item : data) {
            for (Double point : item) {
                if (point != null) {
                    totalPoints.add(point);
                    total += point;
                }
            }
        }
        Double[] totalPointsArray = new Double[totalPoints.size()];

        totalPoints.toArray(totalPointsArray);
        Arrays.sort(totalPointsArray);
        // printTimeCostArray(totalPointsArray);
        println("===== statistics ================");
        println("===== min time cost: " + totalPointsArray[0] + " =====");
        println("===== max time cost: " + totalPointsArray[totalPointsArray.length - 2] + " =====");

        double meanVal = total / totalPointsArray.length;
        println("===== mean time cost: " + meanVal + " =====");
        int middleIndex = (int) (totalPointsArray.length * 0.5);
        println("===== median time cost: " + totalPointsArray[middleIndex] + " =====");
        int top90Index = (int) (totalPointsArray.length * 0.9);
        println("===== top90 time cost: " + totalPointsArray[top90Index] + " =====");
    }

    public static void main(String[] args) {
        testMaxThroughput();
    }

    static class HeartbeatSendTask implements Runnable {
        private double[] runtimesBook;
        private int taskId;
        private int tryTimes;
        private NimbusClient client;

        HeartbeatSendTask(int taskId, int tryTimes) {
            this.taskId = taskId;
            this.tryTimes = tryTimes;
            this.runtimesBook = new double[tryTimes];
            try {
                client = new NimbusClient(initializedConfig(), NIMBUS_HOST, NIMBUS_PORT, null, null);
            } catch (TTransportException e) {
                e.printStackTrace();
            }
        }

        private static SupervisorWorkerHeartbeat nextMockedWorkerbeat() {
            List<ExecutorInfo> executorInfos = new ArrayList<>();
            executorInfos.add(new ExecutorInfo(1, 1));
            executorInfos.add(new ExecutorInfo(2, 2));
            executorInfos.add(new ExecutorInfo(3, 3));
            executorInfos.add(new ExecutorInfo(4, 4));
            SupervisorWorkerHeartbeat heartbeat = new SupervisorWorkerHeartbeat();
            heartbeat.set_executors(executorInfos);
            // generate a random storm id
            heartbeat.set_storm_id("storm_name_example_" + rand.nextInt(MOCKED_STORM_NUM));
            heartbeat.set_time_secs(1221212121);
            return heartbeat;
        }

        private static SupervisorWorkerHeartbeats mockedHeartbeats() {
            SupervisorWorkerHeartbeats heartbeats = new SupervisorWorkerHeartbeats();
            heartbeats.set_supervisor_id("123124134123413412341351234143");
            List<SupervisorWorkerHeartbeat> workers = new ArrayList<>();
            for (int i = 0; i < 25; i++) {
                workers.add(nextMockedWorkerbeat());
            }
            heartbeats.set_worker_heartbeats(workers);
            return heartbeats;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < tryTimes; i++) {
                    long thisBegin = System.currentTimeMillis();
                    client.getClient().sendSupervisorWorkerHeartbeats(mockedHeartbeats());
                    long thisEnd = System.currentTimeMillis();
                    this.runtimesBook[i] = (thisEnd - thisBegin) / 1000D;
                }
                totalCostTimesBook.add(this.runtimesBook);
                readyFlags[taskId] = true;
                Thread.currentThread().interrupt();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

}

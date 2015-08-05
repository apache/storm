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
package com.alibaba.jstorm.message.netty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.messaging.TransportFactory;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

public class NettyUnitTest {

    private static final Logger LOG = LoggerFactory
            .getLogger(NettyUnitTest.class);

    private static int port = 6700;
    private static int task = 1;
    private static Lock lock = new ReentrantLock();
    private static Condition clientClose = lock.newCondition();
    private static Condition contextClose = lock.newCondition();

    private static Map storm_conf = new HashMap<Object, Object>();
    private static IContext context = null;

    static {
        storm_conf = Utils.readDefaultConfig();
        ConfigExtension.setLocalWorkerPort(storm_conf, port);
        boolean syncMode = false;
        if (syncMode) {
            DisruptorQueue.setLimited(true);
            ConfigExtension.setNettyMaxSendPending(storm_conf, 1);
            ConfigExtension.setNettySyncMode(storm_conf, true);
        } else {
            ConfigExtension.setNettySyncMode(storm_conf, false);
            ConfigExtension.setNettyASyncBlock(storm_conf, false);
        }

        // Check whether context can be reused or not
        context = TransportFactory.makeContext(storm_conf);
    }
    
    private IConnection initNettyServer() {
        return initNettyServer(port);
    }
    
    private IConnection initNettyServer(int port) {
        ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues = new ConcurrentHashMap<Integer, DisruptorQueue>();
        IConnection server = context.bind(null, port, deserializeQueues);
        
        WaitStrategy waitStrategy =
                (WaitStrategy) JStormUtils.createDisruptorWaitStrategy(storm_conf);
        DisruptorQueue recvQueue =
                DisruptorQueue.mkInstance("NettyUnitTest", ProducerType.SINGLE,
                        1024, waitStrategy);
        server.registerQueue(task, recvQueue);

        return server;
    }

    @Test
    public void test_small_message() {
        System.out.println("!!!!!!!!Start test_small_message !!!!!!!!!!!");
        String req_msg = "Aloha is the most Hawaiian word.";

        IConnection server = null;
        IConnection client = null;

        server = initNettyServer();

        client = context.connect(null, "localhost", port);

        List<TaskMessage> list = new ArrayList<TaskMessage>();
        TaskMessage message = new TaskMessage(task, req_msg.getBytes());
        list.add(message);
        
        client.send(message);

        byte[] recv = (byte[]) server.recv(task, 0);
        Assert.assertEquals(req_msg, new String(recv));

        System.out.println("!!!!!!!!!!!!!!!!!!Test one time!!!!!!!!!!!!!!!!!");

        server.close();
        client.close();

        System.out.println("!!!!!!!!!!!!End test_small_message!!!!!!!!!!!!!");
    }

    public String setupLargMsg() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Short.MAX_VALUE * 64; i++) {
            sb.append("Aloha is the most Hawaiian word.");
        }

        return sb.toString();
    }

    @Test
    public void test_large_msg() {
        System.out.println("!!!!!!!!!!start large message test!!!!!!!!");
        String req_msg = setupLargMsg();
        System.out.println("!!!!Finish batch data, size:" + req_msg.length()
                + "!!!!");

        IConnection server = null;
        IConnection client = null;

        server = initNettyServer();

        client = context.connect(null, "localhost", port);

        List<TaskMessage> list = new ArrayList<TaskMessage>();
        TaskMessage message = new TaskMessage(task, req_msg.getBytes());
        list.add(message);

        LOG.info("Client send data");
        client.send(message);

        byte[] recv = (byte[]) server.recv(task, 0);
        Assert.assertEquals(req_msg, new String(recv));

        client.close();
        server.close();
        System.out.println("!!!!!!!!!!End larget message test!!!!!!!!");
    }

    @Test
    public void test_server_delay() throws InterruptedException {
        System.out.println("!!!!!!!!!!Start delay message test!!!!!!!!");
        String req_msg = setupLargMsg();

        IConnection server = null;
        IConnection client = null;

        server = initNettyServer();

        client = context.connect(null, "localhost", port);

        List<TaskMessage> list = new ArrayList<TaskMessage>();
        TaskMessage message = new TaskMessage(task, req_msg.getBytes());
        list.add(message);

        LOG.info("Client send data");
        client.send(message);
        Thread.sleep(1000);

        byte[] recv = (byte[]) server.recv(task, 0);
        Assert.assertEquals(req_msg, new String(recv));

        server.close();
        client.close();
        System.out.println("!!!!!!!!!!End delay message test!!!!!!!!");
    }

    @Test
    public void test_first_client() throws InterruptedException {
        System.out.println("!!!!!!!!Start test_first_client !!!!!!!!!!!");
        final String req_msg = setupLargMsg();

        final IContext context = TransportFactory.makeContext(storm_conf);

        new Thread(new Runnable() {

            @Override
            public void run() {

                lock.lock();
                IConnection client = context.connect(null, "localhost", port);

                List<TaskMessage> list = new ArrayList<TaskMessage>();
                TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                list.add(message);

                client.send(message);
                System.out.println("!!Client has sent data");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();
                lock.unlock();

            }
        }).start();

        IConnection server = null;

        JStormUtils.sleepMs(1000);
        System.out.println("!!server begin start!!!!!");

        server = initNettyServer();
        JStormUtils.sleepMs(5000);

        System.out.println("Begin to receive message");
        byte[] recv = (byte[]) server.recv(task, 1);
        Assert.assertEquals(req_msg, new String(recv));

        System.out.println("Finished to receive message");
        
        lock.lock();
        clientClose.signal();
        server.close();
        contextClose.await();
        context.term();
        lock.unlock();

        System.out.println("!!!!!!!!!!!!End test_first_client!!!!!!!!!!!!!");
    }
    
    @Test
    public void test_msg_buffer_timeout() throws InterruptedException {
        System.out.println("!!!!!!!!Start test_msg_buffer_timeout !!!!!!!!!!!");
        final String req_msg = setupLargMsg();

        ConfigExtension.setNettyPendingBufferTimeout(storm_conf, 10*1000l);
        final IContext context = TransportFactory.makeContext(storm_conf);

        new Thread(new Runnable() {

            @Override
            public void run() {

                lock.lock();
                IConnection client = context.connect(null, "localhost", port);

                List<TaskMessage> list = new ArrayList<TaskMessage>();
                TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                list.add(message);

                client.send(message);
                System.out.println("!!Client has sent data");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();
                lock.unlock();

            }
        }).start();

        IConnection server = null;

        JStormUtils.sleepMs(11000);
        System.out.println("!!server begin start!!!!!");

        server = initNettyServer();
        JStormUtils.sleepMs(5000);

        System.out.println("Begin to receive message");
        byte[] recv = (byte[]) server.recv(task, 1);
        Assert.assertEquals(null, recv);

        System.out.println("Pending message was timouout:" + (recv == null));
        
        lock.lock();
        clientClose.signal();
        server.close();
        contextClose.await();
        context.term();
        lock.unlock();

        System.out.println("!!!!!!!!!!!!End test_msg_buffer_timeout!!!!!!!!!!!!!");
    }

    @Test
    public void test_batch() throws InterruptedException {
        System.out.println("!!!!!!!!!!Start batch message test!!!!!!!!");
        final int base = 100000;

        final IContext context = TransportFactory.makeContext(storm_conf);
        final IConnection server = initNettyServer();

        new Thread(new Runnable() {

            public void send() {
                final IConnection client =
                        context.connect(null, "localhost", port);

                List<TaskMessage> list = new ArrayList<TaskMessage>();

                for (int i = 1; i < Short.MAX_VALUE; i++) {

                    String req_msg = String.valueOf(i + base);

                    TaskMessage message =
                            new TaskMessage(task, req_msg.getBytes());
                    list.add(message);

                }

                client.send(list);

                System.out.println("Finish Send ");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();

            }

            @Override
            public void run() {
                lock.lock();
                try {
                    send();
                } finally {
                    lock.unlock();
                }
            }
        }).start();

        for (int i = 1; i < Short.MAX_VALUE; i++) {
            byte[] message = (byte[]) server.recv(task, 0);

            Assert.assertEquals(String.valueOf(i + base),
                    new String(message));

            if (i % 1000 == 0) {
                //System.out.println("Receive " + new String(message));
            }
        }

        System.out.println("Finish Receive ");

        lock.lock();
        clientClose.signal();
        server.close();
        contextClose.await();
        context.term();
        lock.unlock();
        System.out.println("!!!!!!!!!!End batch message test!!!!!!!!");
    }

    @Test
    public void test_slow_receive() throws InterruptedException {
        System.out
                .println("!!!!!!!!!!Start test_slow_receive message test!!!!!!!!");
        final int base = 100000;

        final IContext context = TransportFactory.makeContext(storm_conf);
        final IConnection server = initNettyServer();

        new Thread(new Runnable() {

            @Override
            public void run() {
                lock.lock();

                IConnection client = null;

                client = context.connect(null, "localhost", port);

                List<TaskMessage> list = new ArrayList<TaskMessage>();

                for (int i = 1; i < Short.MAX_VALUE; i++) {

                    String req_msg = String.valueOf(i + base);

                    TaskMessage message =
                            new TaskMessage(task, req_msg.getBytes());
                    list.add(message);

                    if (i % 1000 == 0) {
                        System.out.println("send " + i);
                        client.send(list);
                        list = new ArrayList<TaskMessage>();
                    }

                }

                client.send(list);

                System.out.println("Finish Send ");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();
                lock.unlock();
            }
        }).start();

        for (int i = 1; i < Short.MAX_VALUE; i++) {
            byte[] message = (byte[]) server.recv(task, 0);
            JStormUtils.sleepMs(1);

            Assert.assertEquals(String.valueOf(i + base),
                    new String(message));

            if (i % 1000 == 0) {
                //System.out.println("Receive " + new String(message));
            }
        }

        System.out.println("Finish Receive ");

        lock.lock();
        clientClose.signal();
        server.close();
        contextClose.await();
        context.term();
        lock.unlock();
        System.out
                .println("!!!!!!!!!!End test_slow_receive message test!!!!!!!!");
    }

    @Test
    public void test_slow_receive_big() throws InterruptedException {
        System.out
                .println("!!!!!!!!!!Start test_slow_receive_big message test!!!!!!!!");
        final int base = 100;
        final String req_msg = setupLargMsg();

        final IContext context = TransportFactory.makeContext(storm_conf);
        final IConnection server = initNettyServer();

        new Thread(new Runnable() {

            @Override
            public void run() {
                final IConnection client =
                        context.connect(null, "localhost", port);

                lock.lock();
                for (int i = 1; i < base; i++) {

                    TaskMessage message =
                            new TaskMessage(task, req_msg.getBytes());
                    System.out.println("send " + i);
                    client.send(message);

                }

                System.out.println("Finish Send ");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();
                lock.unlock();

            }
        }).start();

        for (int i = 1; i < base; i++) {
            byte[] message = (byte[]) server.recv(task, 0);
            JStormUtils.sleepMs(100);

            Assert.assertEquals(req_msg, new String(message));
            System.out.println("receive msg-" + i);

        }

        System.out.println("Finish Receive ");

        lock.lock();
        clientClose.signal();
        server.close();
        contextClose.await();
        context.term();
        lock.unlock();
        System.out
                .println("!!!!!!!!!!End test_slow_receive_big message test!!!!!!!!");
    }

    @Test
    public void test_client_reboot() throws InterruptedException {
        System.out.println("!!!!!!!!!!Start client reboot test!!!!!!!!");
        final String req_msg = setupLargMsg();

        final IContext context = TransportFactory.makeContext(storm_conf);

        new Thread(new Runnable() {

            @Override
            public void run() {

                IConnection client = context.connect(null, "localhost", port);

                lock.lock();

                List<TaskMessage> list = new ArrayList<TaskMessage>();
                TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                list.add(message);

                client.send(message);

                System.out.println("Send first");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                client.close();

                IConnection client2 = context.connect(null, "localhost", port);
                System.out.println("!!!!!!! restart client !!!!!!!!!!");

                client2.send(message);
                System.out.println("Send second");
                JStormUtils.sleepMs(1000);

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client2.close();
                contextClose.signal();
                lock.unlock();
            }
        }).start();

        IConnection server = initNettyServer();

        byte[] recv = (byte[]) server.recv(task, 0);
        System.out.println("Sever receive first");
        Assert.assertEquals(req_msg, new String(recv));

        Thread.sleep(1000);

        byte[] recv2 = (byte[]) server.recv(task, 0);
        System.out.println("Sever receive second");
        Assert.assertEquals(req_msg, new String(recv2));

        lock.lock();
        clientClose.signal();
        server.close();
        contextClose.await();
        context.term();
        lock.unlock();
        System.out.println("!!!!!!!!!!End client reboot test!!!!!!!!");
    }

    @Test
    public void test_server_reboot() throws InterruptedException {
        System.out.println("!!!!!!!!!!Start server reboot test!!!!!!!!");
        final String req_msg = setupLargMsg();

        final IContext context = TransportFactory.makeContext(storm_conf);
        IConnection server = null;

        new Thread(new Runnable() {

            @Override
            public void run() {
                final IConnection client =
                        context.connect(null, "localhost", port);

                lock.lock();

                List<TaskMessage> list = new ArrayList<TaskMessage>();
                TaskMessage message = new TaskMessage(task, req_msg.getBytes());
                list.add(message);

                client.send(message);

                System.out.println("Send first");

                JStormUtils.sleepMs(10000);

                System.out.println("Begin to Send second");
                client.send(message);
                System.out.println("Send second");

                JStormUtils.sleepMs(15000);
                client.send(message);
                System.out.println("Send third time");

                try {
                    clientClose.await();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                client.close();
                contextClose.signal();
                lock.unlock();

            }
        }).start();

        server = initNettyServer();

        byte[] recv = (byte[]) server.recv(task, 0);
        System.out.println("Receive first");
        Assert.assertEquals(req_msg, new String(recv));

        server.close();

        System.out.println("!!shutdow server and sleep 30s, please wait!!");
        Thread.sleep(30000);

        IConnection server2 = server = initNettyServer();
        System.out.println("!!!!!!!!!!!!!!!!!!!! restart server !!!!!!!!!!!");

        byte[] recv2 = (byte[]) server2.recv(task, 0);
        Assert.assertEquals(req_msg, new String(recv2));

        lock.lock();
        clientClose.signal();
        server2.close();
        contextClose.await();
        context.term();
        lock.unlock();
        System.out.println("!!!!!!!!!!End server reboot test!!!!!!!!");
    }

    /**
     * Due to there is only one client to one server in one jvm It can't do this
     * test
     * 
     * @throws InterruptedException
     */
    public void test_multiple_client() throws InterruptedException {
        System.out.println("!!!!!!!!Start test_multiple_client !!!!!!!!!!!");
        final String req_msg = setupLargMsg();

        final int clientNum = 3;
        final AtomicLong received = new AtomicLong(clientNum);

        for (int i = 0; i < clientNum; i++) {

            new Thread(new Runnable() {

                @Override
                public void run() {

                    IConnection client =
                            context.connect(null, "localhost", port);

                    List<TaskMessage> list = new ArrayList<TaskMessage>();
                    TaskMessage message =
                            new TaskMessage(task, req_msg.getBytes());
                    list.add(message);

                    client.send(message);
                    System.out.println("!!Client has sent data");

                    while (received.get() != 0) {
                        JStormUtils.sleepMs(1000);
                    }

                    client.close();

                }
            }).start();
        }

        IConnection server = null;

        JStormUtils.sleepMs(1000);
        System.out.println("!!server begin start!!!!!");

        server = initNettyServer();

        for (int i = 0; i < clientNum; i++) {
            byte[] recv = (byte[]) server.recv(task, 0);
            Assert.assertEquals(req_msg, new String(recv));
            received.decrementAndGet();
        }

        server.close();

        System.out.println("!!!!!!!!!!!!End test_multiple_client!!!!!!!!!!!!!");
    }

    @Test
    public void test_multiple_server() throws InterruptedException {
        System.out.println("!!!!!!!!Start test_multiple_server !!!!!!!!!!!");
        final String req_msg = setupLargMsg();

        final int clientNum = 3;
        final AtomicLong received = new AtomicLong(clientNum);

        for (int i = 0; i < clientNum; i++) {
            final int realPort = port + i;

            new Thread(new Runnable() {

                @Override
                public void run() {

                    IConnection server = null;

                    JStormUtils.sleepMs(1000);
                    System.out.println("!!server begin start!!!!!");

                    server = initNettyServer(realPort);

                    byte[] recv = (byte[]) server.recv(task, 0);
                    Assert.assertEquals(req_msg, new String(recv));
                    received.decrementAndGet();
                    System.out.println("!!server received !!!!!" + realPort);

                    server.close();

                }
            }).start();
        }

        List<TaskMessage> list = new ArrayList<TaskMessage>();
        TaskMessage message = new TaskMessage(task, req_msg.getBytes());
        list.add(message);

        List<IConnection> clients = new ArrayList<IConnection>();

        for (int i = 0; i < clientNum; i++) {
            final int realPort = port + i;

            IConnection client = context.connect(null, "localhost", realPort);
            clients.add(client);

            client.send(message);
            System.out.println("!!Client has sent data to " + realPort);
        }

        while (received.get() != 0) {
            JStormUtils.sleepMs(1000);
        }

        for (int i = 0; i < clientNum; i++) {
            clients.get(i).close();
        }

        System.out.println("!!!!!!!!!!!!End test_multiple_server!!!!!!!!!!!!!");
    }
}

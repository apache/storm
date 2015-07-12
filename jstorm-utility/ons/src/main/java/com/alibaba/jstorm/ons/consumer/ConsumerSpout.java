package com.alibaba.jstorm.ons.consumer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.metric.MetricClient;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.metric.JStormHistogram;
import com.alibaba.jstorm.ons.OnsTuple;
import com.alibaba.jstorm.utils.JStormUtils;
import com.aliyun.openservices.ons.api.*;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;


public class ConsumerSpout implements IRichSpout, IAckValueSpout, IFailValueSpout, MessageListener {
    /**  */
    private static final long serialVersionUID = 8476906628618859716L;
    private static final Logger LOG = Logger.getLogger(ConsumerSpout.class);
    
    public static final String ONS_SPOUT_FLOW_CONTROL = "OnsSpoutFlowControl";
    public static final String ONS_SPOUT_AUTO_ACK = "OnsSpoutAutoAck";
    public static final String ONS_MSG_MAX_FAIL_TIMES = "OnsMsgMaxFailTimes";

    protected SpoutOutputCollector collector;
    protected transient Consumer consumer;
    protected transient ConsumerConfig consumerConfig;

    protected Map conf;
    protected String id;
    protected boolean flowControl;
    protected boolean autoAck;
    protected long    maxFailTimes;
    protected boolean active = true;

    protected transient LinkedBlockingDeque<OnsTuple> sendingQueue;

    protected transient MetricClient metricClient;
    protected transient JStormHistogram waithHistogram;
    protected transient JStormHistogram processHistogram;


    public ConsumerSpout() {

    }


    public void initMetricClient(TopologyContext context) {
        metricClient = new MetricClient(context);
        waithHistogram = metricClient.registerHistogram("OnsTupleWait", null);
        processHistogram = metricClient.registerHistogram("OnsTupleProcess", null);
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.collector = collector;
        this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
        this.sendingQueue = new LinkedBlockingDeque<OnsTuple>();

        this.flowControl = JStormUtils.parseBoolean(conf.get(ONS_SPOUT_FLOW_CONTROL), true);
        this.autoAck = JStormUtils.parseBoolean(conf.get(ONS_SPOUT_AUTO_ACK), false);
        this.maxFailTimes = JStormUtils.parseLong(conf.get(ONS_MSG_MAX_FAIL_TIMES), 5);

        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init MetaSpout:").append(id);
        sb.append(", flowControl:").append(flowControl);
        sb.append(", autoAck:").append(autoAck);
        LOG.info(sb.toString());

        initMetricClient(context);

        try {
        	consumerConfig = new ConsumerConfig(conf);
            consumer = ConsumerFactory.mkInstance(consumerConfig, this);
        }
        catch (Exception e) {
            LOG.error("Failed to create Meta Consumer ", e);
            throw new RuntimeException("Failed to create MetaConsumer" + id, e);
        }

        if (consumer == null) {
            LOG.warn(id + " already exist consumer in current worker, don't need to fetch data ");

            new Thread(new Runnable() {

                @Override
                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(10000);
                        }
                        catch (InterruptedException e) {
                            break;
                        }

                        StringBuilder sb = new StringBuilder();
                        sb.append("Only on meta consumer can be run on one process,");
                        sb.append(" but there are mutliple spout consumes with the same topic@groupid meta, so the second one ");
                        sb.append(id).append(" do nothing ");
                        LOG.info(sb.toString());
                    }
                }
            }).start();
        }

        LOG.info("Successfully init " + id);
    }


    @Override
    public void close() {
        if (consumer != null && active == true) {
        	active = false;
        	consumer.shutdown();
            
        }
    }


    @Override
    public void activate() {
        if (consumer != null && active == false) {
            active = true;
            consumer.start();
        }

    }


    @Override
    public void deactivate() {
        if (consumer != null && active == true) {
            active = false;
            consumer.shutdown();
        }
    }


    public void sendTuple(OnsTuple OnsTuple) {
        OnsTuple.updateEmitMs();
        collector.emit(new Values(OnsTuple), OnsTuple.getCreateMs());
    }


    @Override
    public void nextTuple() {
        OnsTuple OnsTuple = null;
        try {
            OnsTuple = sendingQueue.take();
        }
        catch (InterruptedException e) {
        }

        if (OnsTuple == null) {
            return;
        }

        sendTuple(OnsTuple);

    }


    @Deprecated
    public void ack(Object msgId) {
        LOG.warn("Shouldn't go this function");
    }


    @Deprecated
    public void fail(Object msgId) {
        LOG.warn("Shouldn't go this function");
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("OnsTuple"));
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    @Override
    public void fail(Object msgId, List<Object> values) {
        OnsTuple OnsTuple = (OnsTuple) values.get(0);
        AtomicInteger failTimes = OnsTuple.getFailureTimes();

        int failNum = failTimes.incrementAndGet();
        if (failNum > maxFailTimes) {
            LOG.warn("Message " + OnsTuple.getMessage().getMsgID() + " fail times " + failNum);
            finishTuple(OnsTuple);
            return;
        }

        if (flowControl) {
            sendingQueue.offer(OnsTuple);
        }
        else {
            sendTuple(OnsTuple);
        }
    }


    public void finishTuple(OnsTuple OnsTuple) {
        waithHistogram.update(OnsTuple.getEmitMs() - OnsTuple.getCreateMs());
        processHistogram.update(System.currentTimeMillis() - OnsTuple.getEmitMs());
        OnsTuple.done();
    }


    @Override
    public void ack(Object msgId, List<Object> values) {
        OnsTuple OnsTuple = (OnsTuple) values.get(0);
        finishTuple(OnsTuple);
    }


    public Consumer getConsumer() {
        return consumer;
    }


    @Override
    public Action consume(Message message, ConsumeContext context) {
        try {
            OnsTuple OnsTuple = new OnsTuple(message);

            if (flowControl) {
                sendingQueue.offer(OnsTuple);
            }
            else {
                sendTuple(OnsTuple);
            }

            if (autoAck) {
                return Action.CommitMessage;
            }
            else {
            	OnsTuple.waitFinish();
                if (OnsTuple.isSuccess() == true) {
                    return Action.CommitMessage;
                }
                else {
                    return Action.ReconsumeLater;
                }
            }

        }
        catch (Exception e) {
            LOG.error("Failed to emit " + id, e);
            return Action.ReconsumeLater;
        }
    }
}

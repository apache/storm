package com.dianping.cosmos;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.dianping.puma.api.ConfigurationBuilder;
import com.dianping.puma.api.EventListener;
import com.dianping.puma.api.PumaClient;
import com.dianping.puma.core.event.ChangedEvent;
import com.dianping.puma.core.event.RowChangedEvent;


public class PumaSpout implements IRichSpout{
    public static final Logger LOG = LoggerFactory.getLogger(PumaSpout.class);
    
    private SpoutOutputCollector collector;
    private PumaEventListener listener;
    private BlockingQueue<RowChangedEvent> receiveQueue;
    private Map<String, RowChangedEvent> waitingForAck;
    
    private Map<String, String[]> watchTables;
    private String pumaHost;
    private int pumaPort;
    private String pumaName;
    private String pumaTarget;
    private int pumaServerId;
    private String pumaSeqFileBase;
    
    public PumaSpout(String host, int port, String name, String target, HashMap<String, String[]> tables) {
        this(host, port, name, target, tables, null);
    }
    
    public PumaSpout(String host, int port, String name, String target, HashMap<String, String[]> tables, String seqFileBase) {
        this(host, port, name, target, tables, 9999, seqFileBase);
    }
    
    public PumaSpout(String host, int port, String name, String target, HashMap<String, String[]> tables, int serverId, String seqFileBase) {
        pumaHost = host;
        pumaPort = port;
        pumaName = name;
        pumaTarget = target;
        watchTables = tables;
        pumaServerId = serverId;
        pumaSeqFileBase = seqFileBase;
    }
    
    protected static String getMsgId(RowChangedEvent e) {
        return e.getBinlogServerId() + "." + e.getBinlog() + "." + e.getBinlogPos();
    }
    
    protected static String getStreamId(RowChangedEvent e) {
        return e.getDatabase() + "." + e.getTable();
    }
    
    class PumaEventListener implements EventListener {

        @Override
        public void onEvent(ChangedEvent event) throws Exception {
            if (!(event instanceof RowChangedEvent)) {
                LOG.error("received event " + event +" which is not a RowChangedEvent");
                return;
            }
            RowChangedEvent e = (RowChangedEvent)event;
            receiveQueue.add(e);
        }

        @Override
        public boolean onException(ChangedEvent event, Exception e) {
            return false;
        }

        @Override
        public void onConnectException(Exception e) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void onConnected() {
            LOG.info("pumaspout connected");
        }

        @Override
        public void onSkipEvent(ChangedEvent event) {
            // TODO Auto-generated method stub
            
        }
        
    }
    
    @Override
    public void ack(Object msgId) {
        LOG.debug("ack: " + msgId);
        waitingForAck.remove(msgId);
    }

    @Override
    public void activate() {
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deactivate() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("fail: " + msgId + ", resend event");
        RowChangedEvent event = waitingForAck.get(msgId);
        collector.emit(getStreamId(event), new Values(event), getMsgId(event));
    }

    @Override
    public void nextTuple() {
        RowChangedEvent event = null;
        try {
            event = receiveQueue.take();
        } catch (InterruptedException e) {
            return;
        }
        
        String msgId = getMsgId(event);
        collector.emit(getStreamId(event), new Values(event), msgId);
        waitingForAck.put(msgId, event);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector _collector) {
        collector = _collector;
        receiveQueue = new LinkedBlockingQueue<RowChangedEvent>();
        waitingForAck = new ConcurrentHashMap<String, RowChangedEvent>();
        
        ConfigurationBuilder configBuilder = new ConfigurationBuilder();
        configBuilder.ddl(false);
        configBuilder.dml(true);
        configBuilder.transaction(false);
        if (pumaSeqFileBase != null) {
            configBuilder.seqFileBase(pumaSeqFileBase);
        }
        configBuilder.host(pumaHost);
        configBuilder.port(pumaPort);
        configBuilder.serverId(pumaServerId);
        configBuilder.name(pumaName);
        for (Entry<String, String[]> e : watchTables.entrySet()) {
            String db = e.getKey();
            String[] tabs = e.getValue();
            configBuilder.tables(db, tabs);
        }
        configBuilder.target(pumaTarget);     
        PumaClient pc = new PumaClient(configBuilder.build());
        
        listener = new PumaEventListener();
        pc.register(listener);
        pc.start();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Entry<String, String[]> entry : watchTables.entrySet()) {
            String db = entry.getKey();
            for (String table : entry.getValue()) {
                String dbTable = db + "." + table;
                declarer.declareStream(dbTable, new Fields("event"));
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}

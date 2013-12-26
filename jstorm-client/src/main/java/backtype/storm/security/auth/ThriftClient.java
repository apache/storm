package backtype.storm.security.auth;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import javax.security.auth.login.Configuration;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorEventType;
import com.netflix.curator.framework.api.CuratorListener;
import com.netflix.curator.framework.api.UnhandledErrorListener;


public class ThriftClient {	
    private static final Logger LOG = LoggerFactory.getLogger(ThriftClient.class);
    private static final String MASTER_PATH = "/master";
    private TTransport _transport;
    protected TProtocol _protocol;
    private CuratorFramework zkobj;
    private String masterHost;
    private final String zkMasterDir;
    private Map<Object, Object> conf;

    public ThriftClient(Map storm_conf) throws Exception {
        this(storm_conf, null);
    }

    @SuppressWarnings("unchecked")
	public ThriftClient(Map storm_conf, Integer timeout) throws Exception {
    	conf = storm_conf;
    	String root = String.valueOf(storm_conf.get(Config.STORM_ZOOKEEPER_ROOT));
    	zkMasterDir = root + MASTER_PATH;
    	
    	LOG.info("zkServer:" + (List<String>) storm_conf.get(Config.STORM_ZOOKEEPER_SERVERS) 
    			+ ", zkPort:" + (Integer)storm_conf.get(Config.STORM_ZOOKEEPER_PORT));  //++
    	zkobj = Utils.newCurator(storm_conf, 
					(List<String>) storm_conf.get(Config.STORM_ZOOKEEPER_SERVERS),
					storm_conf.get(Config.STORM_ZOOKEEPER_PORT), 
					root);
    	zkobj.start();
    	if(zkobj.checkExists().forPath(MASTER_PATH) == null)
    		throw new RuntimeException("No alive nimbus "); 
    	zkobj.close();
    	
    	zkobj = Utils.newCurator(storm_conf, 
    				(List<String>) storm_conf.get(Config.STORM_ZOOKEEPER_SERVERS),
    				storm_conf.get(Config.STORM_ZOOKEEPER_PORT), 
    				zkMasterDir);
    	zkobj.getCuratorListenable().addListener(new CuratorListener() {

			@Override
			public void eventReceived(CuratorFramework arg0, CuratorEvent e)
					throws Exception {
				// TODO Auto-generated method stub
				if(e.getType().equals(CuratorEventType.WATCHED)) {
                    WatchedEvent event = e.getWatchedEvent();
                    KeeperState state = event.getState();
                    EventType type = event.getType();
                    String path = event.getPath();
                    if(!(state.equals(KeeperState.SyncConnected))) {
                        LOG.warn("Received event " + state + ":" + type + ":"
                                + path + " with disconnected Zookeeper.");
                    } else {
                        LOG.info("Received event " + state + ":" + type + ":"
                                + path);
                    }
                    
                    if(!type.equals(EventType.None)) {
                    	flushClient(conf, null);
        				flush();
                    }
                }
			}
    		
    	});
    	
    	zkobj.getUnhandledErrorListenable().addListener(
                new UnhandledErrorListener() {
                    @Override
                    public void unhandledError(String msg, Throwable error) {
                        String errmsg = "Unrecoverable Zookeeper error, halting process: "
                                + msg;
                        LOG.error(errmsg, error);
                        Runtime.getRuntime().halt(1);
                    }
                });
    	zkobj.start();
    	flushClient(storm_conf, timeout);
    }

    public TTransport transport() {
        return _transport;
    }
    
    protected void flushClient(Map storm_conf, Integer timeout) throws Exception {
    	try {
    		flushHost();
    		String[] host_port = masterHost.split(":");
    		if(host_port.length != 2) {
    			throw new InvalidParameterException("Host format error: " + masterHost);
    		}
    		String host = host_port[0];
    		int port = Integer.parseInt(host_port[1]);
    		LOG.info("Begin to connect " + host + ":" + port);
        
            //locate login configuration 
            Configuration login_conf = AuthUtils.GetConfiguration(storm_conf);

            //construct a transport plugin
            ITransportPlugin  transportPlugin = AuthUtils.GetTransportPlugin(storm_conf, login_conf);

            //create a socket with server
            if(host==null) {
                throw new IllegalArgumentException("host is not set");
            }
            if(port<=0) {
                throw new IllegalArgumentException("invalid port: "+port);
            }            
            TSocket socket = new TSocket(host, port);
            if(timeout!=null) {
                socket.setTimeout(timeout);
            }
            final TTransport underlyingTransport = socket;

            //establish client-server transport via plugin
            _transport =  transportPlugin.connect(underlyingTransport, host); 
        } catch (IOException ex) {
            throw new RuntimeException("Create transport error");
        }
        _protocol = null;
        if (_transport != null)
            _protocol = new  TBinaryProtocol(_transport);
    } 
    
    private void flushHost() throws Exception {
    	masterHost = new String(zkobj.getData().forPath("/"));
    }

    public void close() {
    	if(_transport != null)
    		_transport.close();
    	if(zkobj != null)
    		zkobj.close();
    }
    
    public String getMasterHost() {
    	return masterHost;
    }
    
    protected void flush() {
    	
    }
}

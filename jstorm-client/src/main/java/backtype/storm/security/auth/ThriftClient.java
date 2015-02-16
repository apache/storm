package backtype.storm.security.auth;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.Configuration;

import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class ThriftClient {
	private static final Logger LOG = LoggerFactory
			.getLogger(ThriftClient.class);
	private static final String MASTER_PATH = "/nimbus_master";
	private TTransport _transport;
	protected TProtocol _protocol;
	private String masterHost;

	private Map<Object, Object> conf;

	public ThriftClient(Map storm_conf) throws Exception {
		this(storm_conf, null);
	}

	@SuppressWarnings("unchecked")
	public ThriftClient(Map storm_conf, Integer timeout) throws Exception {
		conf = storm_conf;
		masterHost = getMaster(storm_conf, timeout);
		connect(storm_conf, timeout);
	}
	
	public static String getMaster(Map storm_conf, Integer timeout) throws Exception {
		CuratorFramework zkobj = null;
		String  masterHost = null;
		
		try {
			String root = String.valueOf(storm_conf
					.get(Config.STORM_ZOOKEEPER_ROOT));
			String zkMasterDir = root + MASTER_PATH;
	
			
			zkobj = Utils.newCurator(storm_conf,
					(List<String>) storm_conf.get(Config.STORM_ZOOKEEPER_SERVERS),
					storm_conf.get(Config.STORM_ZOOKEEPER_PORT), zkMasterDir);
			zkobj.start();
			if (zkobj.checkExists().forPath("/") == null) {
				throw new RuntimeException("No alive nimbus ");
			}
			
			masterHost = new String(zkobj.getData().forPath("/"));

			LOG.info("masterHost:"  + masterHost);
			return masterHost;
		}finally {
			if (zkobj != null) {
				zkobj.close();
				zkobj = null;
			}
		}
	}

	public TTransport transport() {
		return _transport;
	}

	protected void connect(Map storm_conf, Integer timeout)
			throws Exception {
		try {
			String[] host_port = masterHost.split(":");
			if (host_port.length != 2) {
				throw new InvalidParameterException("Host format error: "
						+ masterHost);
			}
			String host = host_port[0];
			int port = Integer.parseInt(host_port[1]);
			LOG.info("Begin to connect " + host + ":" + port);

			// locate login configuration
			Configuration login_conf = AuthUtils.GetConfiguration(storm_conf);

			// construct a transport plugin
			ITransportPlugin transportPlugin = AuthUtils.GetTransportPlugin(
					storm_conf, login_conf);

			// create a socket with server
			if (host == null) {
				throw new IllegalArgumentException("host is not set");
			}
			if (port <= 0) {
				throw new IllegalArgumentException("invalid port: " + port);
			}

			TSocket socket = new TSocket(host, port);
			if (timeout != null) {
				socket.setTimeout(timeout);
			}
			final TTransport underlyingTransport = socket;

			// establish client-server transport via plugin
			_transport = transportPlugin.connect(underlyingTransport, host);
		} catch (IOException ex) {
			throw new RuntimeException("Create transport error");
		}
		_protocol = null;
		if (_transport != null)
			_protocol = new TBinaryProtocol(_transport);
	}
	
	


	public void close() {
		if (_transport != null)
			_transport.close();
	}

	public String getMasterHost() {
		return masterHost;
	}
	
	public Map<Object, Object> getConf() {
		return conf;
	}

	protected void flush() {

	}
}

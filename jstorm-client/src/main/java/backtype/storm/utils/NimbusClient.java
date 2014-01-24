package backtype.storm.utils;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.Nimbus;
import backtype.storm.security.auth.ThriftClient;

public class NimbusClient extends ThriftClient {
	private Nimbus.Client _client;
	private static final Logger LOG = LoggerFactory
			.getLogger(NimbusClient.class);

	@SuppressWarnings("unchecked")
	public static NimbusClient getConfiguredClient(Map conf) {
		try {
			// String nimbusHost = (String) conf.get(Config.NIMBUS_HOST);
			// int nimbusPort =
			// Utils.getInt(conf.get(Config.NIMBUS_THRIFT_PORT));
			// return new NimbusClient(conf, nimbusHost, nimbusPort);
			return new NimbusClient(conf);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	private NimbusClient(Map conf) throws Exception {
		this(conf, null);
	}

	@SuppressWarnings("unchecked")
	private NimbusClient(Map conf, Integer timeout) throws Exception {
		super(conf, timeout);
		flush();
	}

	public Nimbus.Client getClient() {
		return _client;
	}

	@Override
	protected void flush() {
		// TODO Auto-generated method stub
		_client = new Nimbus.Client(_protocol);
	}
}

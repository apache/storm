package com.alibaba.jstorm.daemon.worker.metrics;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;

public class AlimonitorClient {

	public static Logger LOG = Logger.getLogger(AlimonitorClient.class);

	public static final String ERROR_MSG = "";

	private final String COLLECTION_FLAG = "collection_flag";

	private final String ERROR_INFO = "error_info";

	private final String MSG = "MSG";

	private String port;

	private String requestIP;

	private String monitorName;

	private boolean post = true;

	public AlimonitorClient(String requestIP, String port) {
		this.requestIP = requestIP;
		this.port = port;
	}

	// Send to localhost:15776 by default
	public AlimonitorClient() {

	}

	public void setMonitorName(String monitorName) {
		this.monitorName = monitorName;
	}

	public <K, V> boolean sendRequest(K key, V value) throws Exception {
		return sendRequest(0, " ", key, value);
	}

	public <K, V> boolean sendRequest(int collection_flag,
			String error_message, K key, V value) throws Exception {
		Map msg = new HashMap<K, V>();
		msg.put(key, value);
		return sendRequest(collection_flag, error_message, msg);
	}

	/**
	 * 
	 * @param collection_flag
	 *            0标识采集正常，非0表示采集错误
	 * @param error_message
	 *            采集错误时的描述信息 (default: "")
	 * @param msg
	 *            监控项数据{}
	 * @return 发送是否成功
	 * @throws Exception
	 */
	public boolean sendRequest(int collection_flag, String error_message,
			Map<String, Object> msg) throws Exception {
		boolean ret = false;

		return ret;
	}

	/**
	 * 
	 * @param collection_flag
	 *            0标识采集正常，非0表示采集错误
	 * @param error_message
	 *            采集错误时的描述信息 (default: "")
	 * @param msg
	 *            监控项数据[]
	 * @return 发送是否成功
	 * @throws Exception
	 */
	public boolean sendRequest(int collection_flag, String error_message,
			List<Map<String, Object>> msgList) throws Exception {
		boolean ret = false;

		return ret;
	}

	private boolean httpGet(StringBuilder postAddr) {
		boolean ret = false;
		return ret;
	}

	private boolean httpPost(String url, String msg) {
		boolean ret = false;

		return ret;
	}

	public void close() {
	}
}

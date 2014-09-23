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
		this.requestIP = "127.0.0.1";
		this.port = "15776";
	}
	
	public void setMonitorName(String monitorName) {
		this.monitorName = monitorName;
	}

	public String buildURL() {
		return "http://" + requestIP + ":" + port + "/passive";
	}
	
	public String buildRqstAddr() {
		return "http://" + requestIP + ":" + port + "/passive?name=" + monitorName + "&msg=";
	}

	public <K,V> boolean sendRequest(K key,V value) throws Exception {
				return sendRequest(0," ",key,value);
	}
	public <K,V> boolean sendRequest(int collection_flag, String error_message,
			K key,V value) throws Exception {
				Map msg=new HashMap<K,V>();
				msg.put(key, value);
				return sendRequest(collection_flag,error_message,msg);
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
	public  boolean sendRequest(int collection_flag, String error_message,
			Map<String,Object> msg) throws Exception {
		boolean ret = false;
		
		if(msg.size() == 0)
			return false;
		StringBuilder sb = new StringBuilder();
		//{K,V}
		sb.append("{");
		sb.append("\'").append(MSG).append("\'");
		sb.append(':');
		sb.append("{");
		for (Entry<String, Object> entry: msg.entrySet()) {
			sb.append("\'").append(entry.getKey()).append("\'");
			sb.append(":");
			if(entry.getValue() instanceof String)
				sb.append("\"").append(entry.getValue()).append("\"");				
			else
				sb.append(entry.getValue());
			sb.append(",");
		}
		//remove the last ","
		if(msg.size()>0)
			sb.deleteCharAt(sb.length()-1);
		sb.append("}");
		sb.append(',');
		sb.append("\'").append(COLLECTION_FLAG).append("\'");
		sb.append(':');
		sb.append(collection_flag);
		sb.append(',');
		sb.append("\'").append(ERROR_INFO).append("\'");
		sb.append(':').append("\'").append(error_message).append("\'");	
		sb.append("}");
		String kvMsg = sb.toString();
		LOG.debug(kvMsg);

		if (post == true) {
			String url = buildURL();
			ret = httpPost(url, kvMsg);
		} else {
		    String request = buildRqstAddr(); 
		    StringBuilder postAddr= new StringBuilder();
		    postAddr.append(request);
		    postAddr.append(URLEncoder.encode(kvMsg));
		
		    ret = httpGet(postAddr);
		}

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
	public  boolean sendRequest(int collection_flag, String error_message,
			List<Map<String,Object>> msgList) throws Exception {
		boolean ret = false;
		
		//[{K:V},{K:V}]
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		sb.append("\'").append(MSG).append("\'");
		sb.append(':');
		sb.append("[");
		for(Map<String,Object> msg:msgList){	
			
			sb.append("{");

			for (Entry<String, Object> entry: msg.entrySet()) {
				sb.append("\'").append(entry.getKey()).append("\'");
				sb.append(":");
				if(entry.getValue() instanceof String)
					sb.append("\"").append(entry.getValue()).append("\"");				
				else
					sb.append(entry.getValue());
				sb.append(",");
			}
			//remove the last ","
			if(msg.size()>0)
				sb.deleteCharAt(sb.length()-1);
			sb.append("}");				
			sb.append(",");
		}
		//remove the last ","
		if(msgList.size()>0)
			sb.deleteCharAt(sb.length()-1);		
		sb.append("]");	
		sb.append(',');
		sb.append("\'").append(COLLECTION_FLAG).append("\'");
		sb.append(':');
		sb.append(collection_flag);
		sb.append(',');
		sb.append("\'").append(ERROR_INFO).append("\'");
		sb.append(':').append("\'").append(error_message).append("\'");	
		sb.append("}");
		String msg = sb.toString();
		LOG.debug(msg);
		
		if (post == true) {
			String url = buildURL();
			ret = httpPost(url, msg);
		} else {
		    String request = buildRqstAddr(); 
		    StringBuilder postAddr= new StringBuilder();
		    postAddr.append(request);
		    postAddr.append(URLEncoder.encode(msg));
		
		    ret = httpGet(postAddr);
		}
		 
		return ret;
	}
	
	private boolean httpGet(StringBuilder postAddr) {
		boolean ret = false;
		
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		CloseableHttpResponse response = null;

		try {
			HttpGet request = new HttpGet(postAddr.toString());
		    response = httpClient.execute(request);
    	    HttpEntity entity = response.getEntity();
            if (entity != null) {
        	    LOG.debug(EntityUtils.toString(entity));
            }
            EntityUtils.consume(entity);
            ret = true;
		} catch (Exception e) {
			LOG.error("Exception when sending http request to alimonitor", e);
		} finally {
			try {
				if (response != null)
				    response.close();
			    httpClient.close();
			} catch (Exception e) {
				LOG.error("Exception when closing httpclient", e);
			}
		}
		
		return ret;
	}
	
	private boolean httpPost(String url, String msg) {
		boolean ret = false;
		
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		CloseableHttpResponse response = null;

		try {
			HttpPost request = new HttpPost(url);
			List <NameValuePair> nvps = new ArrayList <NameValuePair>();
			nvps.add(new BasicNameValuePair("name", monitorName));
			nvps.add(new BasicNameValuePair("msg", msg));
			request.setEntity(new UrlEncodedFormEntity(nvps));
		    response = httpClient.execute(request);
    	    HttpEntity entity = response.getEntity();
            if (entity != null) {
        	    LOG.debug(EntityUtils.toString(entity));
            }
            EntityUtils.consume(entity);
            ret = true;
		} catch (Exception e) {
			LOG.error("Exception when sending http request to alimonitor", e);
		} finally {
			try {
				if (response != null)
				    response.close();
			    httpClient.close();
			} catch (Exception e) {
				LOG.error("Exception when closing httpclient", e);
			}
		}
		
		return ret;
	}
	
	public void close() {
	}
}


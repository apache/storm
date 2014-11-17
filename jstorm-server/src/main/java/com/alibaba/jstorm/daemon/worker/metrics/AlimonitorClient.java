package com.alibaba.jstorm.daemon.worker.metrics;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import backtype.storm.utils.Utils;



public class AlimonitorClient extends MetricSendClient {

	public static Logger LOG = Logger.getLogger(AlimonitorClient.class);
	
	// Send to localhost:15776 by default
	public static final String DEFAUT_ADDR        = "127.0.0.1";
	public static final String DEFAULT_PORT       = "15776";
	public static final int    DEFAUTL_FLAG       = 0;
	public static final String DEFAULT_ERROR_INFO = "";
	
	private final String COLLECTION_FLAG = "collection_flag";
	private final String ERROR_INFO      = "error_info";
	private final String MSG             = "MSG";
	
	private String port;
	private String requestIP;
	private String monitorName;
    private int    collectionFlag;
	private String errorInfo;
	
	private boolean post; 

	public AlimonitorClient() {
	}
	
	public AlimonitorClient(String requestIP, String port, boolean post) {
		this.requestIP = requestIP;
		this.port = port;
		this.post = post;
		this.monitorName = null;
		this.collectionFlag = 0;
		this.errorInfo = null;
	}
	
	public void setIpAddr(String ipAddr) {
		this.requestIP = ipAddr;
	}
	
	public void setPort(String port) {
		this.port = port;
	}
	
	public void setMonitorName(String monitorName) {
		this.monitorName = monitorName;
	}
	
	public void setCollectionFlag(int flag) {
		this.collectionFlag = flag;
	}
	
	public void setErrorInfo(String msg) {
		this.errorInfo = msg;
	}
	
	public void setPostFlag(boolean post) {
		this.post = post;
	}

	public String buildURL() {
		return "http://" + requestIP + ":" + port + "/passive";
	}
	
	public String buildRqstAddr() {
		return "http://" + requestIP + ":" + port + "/passive?name=" + monitorName + "&msg=";
	}	
	
	@Override
	public boolean send(Map<String,Object> msg) {
		try {
			if(monitorName == null) {
				LOG.warn("monitor name is null");
				return false;
			}
		    return sendRequest(collectionFlag, errorInfo, msg);
		} catch (Exception e) {
			LOG.error("Failed to sendRequest", e);
			return false;
		}
	}
	
	@Override
	public boolean send(List<Map<String,Object>> msg) {
		try {
			if(monitorName == null) {
				LOG.warn("monitor name is null");
				return false;
			}
		    return sendRequest(collectionFlag, errorInfo, msg);
		} catch (Exception e) {
			LOG.error("Failed to sendRequest", e);
			return false;
		}
	}
	
	public Map buildAliMonitorMsg(int collection_flag, String error_message) {
		// Json format of the message sent to Alimonitor
		// {
		//	 "collection_flag":int,
		//	 "error_info":string,
		//	 "MSG": ojbect | array
		// }
		Map ret = new HashMap();
		ret.put(COLLECTION_FLAG, collection_flag);
		ret.put(ERROR_INFO, error_message);
		ret.put(MSG, null);
		
		return ret;
	}
	
	private void addMsgData(Map jsonObj, Map<String, Object> map) {
		jsonObj.put(MSG, map);
	}
	
	private void addMsgData(Map jsonObj, List<Map<String, Object>> mapList) {
//		JSONArray jsonArray = new JSONArray();
//		for(Map<String, Object> map : mapList) {
//			jsonArray.add(map);
//		}
		
		jsonObj.put(MSG, mapList);
	}

	private boolean sendRequest(int collection_flag, String error_message,
			Map<String,Object> msg) throws Exception {
		boolean ret = false;
		
		if (msg.size() == 0) return ret;
		
		Map jsonObj = buildAliMonitorMsg(collection_flag, error_message);
		addMsgData(jsonObj, msg);
		String jsonMsg = jsonObj.toString();
		LOG.info(jsonMsg);

		if (post == true) {
			String url = buildURL();
			ret = httpPost(url, jsonMsg);
		} else {
		    String request = buildRqstAddr(); 
		    StringBuilder postAddr= new StringBuilder();
		    postAddr.append(request);
		    postAddr.append(URLEncoder.encode(jsonMsg));
		
		    ret = httpGet(postAddr);
		}

		return ret;
	}

	private boolean sendRequest(int collection_flag, String error_message,
			List<Map<String,Object>> msgList) throws Exception {
		boolean ret = false;
		
		if (msgList.size() == 0) return ret;
		
		Map jsonObj = buildAliMonitorMsg(collection_flag, error_message);
		addMsgData(jsonObj, msgList);

		String jsonMsg = Utils.to_json(jsonObj); 
		LOG.info(jsonMsg);
		
		if (post == true) {
			String url = buildURL();
			ret = httpPost(url, jsonMsg);
		} else {
		    String request = buildRqstAddr(); 
		    StringBuilder postAddr= new StringBuilder();
		    postAddr.append(request);
		    postAddr.append(URLEncoder.encode(jsonMsg));
		
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
        	    LOG.info(EntityUtils.toString(entity));
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
        	    LOG.info(EntityUtils.toString(entity));
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


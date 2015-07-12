package com.dianping.cosmos.monitor;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 向cat写入metric相关信息
 * @author xinchun.wang
 *
 */
public class HttpClientService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientService.class);
	
//	private static JSONUtil jsonUtil = JSONUtil.getInstance();
	
	protected String excuteGet(String url, boolean useURI) throws Exception {
		HttpClient httpClient = getHttpClient();
		HttpUriRequest request = getGetRequest(url, useURI);
		
		HttpResponse httpResponse = httpClient.execute(request);

		String response = parseResponse(url, httpResponse);
		return response;
	}
	
	protected String excutePost(String url, List<NameValuePair> nvps) throws Exception {
		HttpClient httpClient = getHttpClient();
		HttpPost httpPost = new HttpPost(url);
		httpPost.setEntity(new UrlEncodedFormEntity(nvps));
		HttpResponse httpResponse = httpClient.execute(httpPost);
		String response = parseResponse(url, httpResponse);
		return response;
	}

	private String parseResponse(String url, HttpResponse httpResponse)
			throws Exception, IOException {
		int status = httpResponse.getStatusLine().getStatusCode();
		if(status != 200){
			String errorMsg = "Error occurs in calling acl service: " + url + ", with status:" + status;
			throw new Exception(errorMsg);
		}
		HttpEntity entry = httpResponse.getEntity();
		String response = EntityUtils.toString(entry, "UTF-8");
		return response;
	}

	private HttpClient getHttpClient() {
		HttpClient httpClient = new DefaultHttpClient();
		httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000);
		httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 5000);
		return httpClient;
	}

	private HttpUriRequest getGetRequest(String url, boolean useURI) throws Exception {
		HttpUriRequest request;
		if(useURI){
			URL requestURL = new URL(url);
			URI uri = new URI(
				requestURL.getProtocol(),
				null,
				requestURL.getHost(), 
				requestURL.getPort(),
				requestURL.getPath(), 
				requestURL.getQuery(), 
				null);
			request = new HttpGet(uri);
		}
		else{
				request = new HttpGet(url);
		}
		return request;
	}
	
//	protected boolean parseResultMap(String response, String url) throws Exception{
//		Map<?, ?> result = jsonUtil.formatJSON2Map(response);
//		if(result == null){
//			return false;
//		}
//		String code = (String)result.get("statusCode");
//		if("-1".equals(code)){
//            throw new Exception(String.valueOf(result.get("errorMsg")));
//		}
//		return true;
//	}
	

	public void get(String url) throws Exception{
		String response = excuteGet(url, false);
		if(response == null){
			LOGGER.error("call uri error, response is null, uri = " + url);
		}
		//parseResultMap(response, url);
	}
	
	public void getByURI(String url) throws Exception{
		String response = excuteGet(url, true);
		if(response == null){
			LOGGER.error("call uri error, response is null, uri = " + url);
		}
        //parseResultMap(response, url);
	}
}

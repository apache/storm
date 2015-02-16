package com.alibaba.jstorm.ui.model.data;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;

import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ComponentTask;
import com.alibaba.jstorm.ui.model.LogPageIndex;
import com.alibaba.jstorm.utils.HttpserverUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

/**
 * task log view page service. <br />
 * implement view the specified task log through proxy way. current support
 * </ul>
 * 
 * @author L <qiyuan4f@gmail.com>
 * @version 1.0.0 <2014-04-20 21:23>
 * @since JDK1.6
 */
@ManagedBean(name = "confpage")
@ViewScoped
public class ConfPage implements Serializable {

	private static final long serialVersionUID = 4326599394273506083L;

	private static final Logger LOG = Logger.getLogger(ConfPage.class);

	/**
	 * proxy url, which call the log service on the task node.
	 */
	private static final String PROXY_URL = "http://%s:%s/logview?%s=%s";

	private String confData = "";

	private String host;
	
	private int port;
	
	private Map conf;

	public ConfPage() throws Exception {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("host") != null) {
			host = ctx.getExternalContext().getRequestParameterMap()
					.get("host");
		}
		
		if (ctx.getExternalContext().getRequestParameterMap().get("port") != null) {
        	
			port = JStormUtils.parseInt(ctx.getExternalContext()
					.getRequestParameterMap().get("port"), 0);
		}

		init();
	}

	private void init() throws Exception {
		

		try {
			
			conf = UIUtils.readUiConfig();
			
			if (port == 0) {
				port = ConfigExtension.getSupervisorDeamonHttpserverPort(conf);
			}

			// proxy call
			queryConf();

		} catch (Exception e) {
			LOG.error(e.getCause(), e);
			throw e;
		}
	}


	/**
	 * proxy query log for the specified task.
	 * 
	 * @param task
	 *            the specified task
	 */
	private void queryConf() {
		// PROXY_URL = "http://%s:%s/logview?%s=%s&log=%s";
		String baseUrl = String.format(PROXY_URL, NetWorkUtils.host2Ip(host), port,
				HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD,
				HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_SHOW_CONF);
		String url = baseUrl;
		
		try {
			// 1. proxy call the task host log view service
			HttpClient client = HttpClientBuilder.create().build();
			HttpPost post = new HttpPost(url);
			HttpResponse response = client.execute(post);

			// 2. check the request is success, then read the log
			if (response.getStatusLine().getStatusCode() == 200) {
				String data = EntityUtils.toString(response.getEntity(), ConfigExtension.getLogViewEncoding(conf));

				setConfData(parseJsonConf(data));
			} else {
				setConfData(EntityUtils.toString(response.getEntity()));
			}
		} catch (Exception e) {
			setConfData(e.getMessage());
			LOG.error(e.getCause(), e);
		}
	}

	private String parseJsonConf(String jsonData) {
		Map<Object, Object> remoteConf = (Map)Utils.from_json(jsonData);
		
		StringBuilder sb = new StringBuilder();
		
		for (Entry entry : remoteConf.entrySet()) {
			sb.append(entry.getKey());
			sb.append(":");
			sb.append(entry.getValue());
			sb.append("\n");
		}
		
		return sb.toString();
	}

	public String getConfData() {
		return confData;
	}

	public void setConfData(String confData) {
		this.confData = confData;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	
	
	

}

package com.alibaba.jstorm.ui.model.data;

import java.io.Serializable;
import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.utils.HttpserverUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;


@ManagedBean(name = "jstackpage")
@ViewScoped
public class JStackPage implements Serializable {

	private static final long serialVersionUID = 4326599394273506083L;

	private static final Logger LOG = Logger.getLogger(JStackPage.class);

	/**
	 * proxy url, which call the log service on the task node.
	 */
	private static final String PROXY_URL = "http://%s:%s/logview?%s=%s&%s=%s";

	/**
	 * jstack data
	 */
	private String data = "";

	private int workerPort;

	/**
	 * Http server port
	 */
	private String host;
	private int port;

	private Map conf;

	public JStackPage() throws Exception {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("host") != null) {
			host = ctx.getExternalContext().getRequestParameterMap()
					.get("host");
		}

		if (ctx.getExternalContext().getRequestParameterMap().get("port") != null) {

			port = JStormUtils.parseInt(ctx.getExternalContext()
					.getRequestParameterMap().get("port"), 0);
		}
		
		if (ctx.getExternalContext().getRequestParameterMap()
				.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_WORKER_PORT) != null) {
			workerPort = JStormUtils.parseInt(ctx.getExternalContext()
					.getRequestParameterMap()
					.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_WORKER_PORT));
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
			queryLog(conf);

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
	private void queryLog(Map conf) {
		// PROXY_URL = "http://%s:%s/logview?%s=%s&%s=%s";
		String baseUrl = String
				.format(PROXY_URL, NetWorkUtils.host2Ip(host), port,
						HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD,
						HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_JSTACK,
						HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_WORKER_PORT,
						workerPort);
		String url = baseUrl;
		try {
			// 1. proxy call the task host log view service
			HttpClient client = HttpClientBuilder.create().build();
			HttpPost post = new HttpPost(url);
			HttpResponse response = client.execute(post);

			setData(EntityUtils.toString(response.getEntity()));

		} catch (Exception e) {
			setData(e.getMessage());
			LOG.error(e.getCause(), e);
		}
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public int getworkerPort() {
		return workerPort;
	}

	public void setworkerPort(int workerPort) {
		this.workerPort = workerPort;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

}

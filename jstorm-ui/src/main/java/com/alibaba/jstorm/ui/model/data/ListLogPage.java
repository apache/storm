package com.alibaba.jstorm.ui.model.data;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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
import com.alibaba.jstorm.utils.FileAttribute;
import com.alibaba.jstorm.utils.HttpserverUtils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;

/**
 * task log view page service. <br />
 * implement view the specified task log through proxy way. current support
 * </ul>
 * 
 * @author longda
 * @version 1.0.0 <2014-04-20 21:23>
 * @since JDK1.6
 */
@ManagedBean(name = "listlogpage")
@ViewScoped
public class ListLogPage implements Serializable {

	private static final long serialVersionUID = 4326599394273506085L;

	private static final Logger LOG = Logger.getLogger(LogPage.class);

	/**
	 * proxy url, which call the log service on the task node.
	 */
	private static final String PROXY_URL = "http://%s:%s/logview?%s=%s&%s=%s";

	private String host;

	private String summary;

	private List<FileAttribute> files = new ArrayList<FileAttribute>();

	private List<FileAttribute> dirs = new ArrayList<FileAttribute>();
	/**
	 * Http server port
	 */
	private int port;

	private String portStr;

	private String parent;

	private void getTargetDir(FacesContext ctx) throws Exception {
		String dir = null;
		if (ctx.getExternalContext().getRequestParameterMap().get("dir") != null) {

			dir = ctx.getExternalContext().getRequestParameterMap().get("dir");
		}

		String paramParent = null;

		if (ctx.getExternalContext().getRequestParameterMap().get("parent") != null) {

			paramParent = ctx.getExternalContext().getRequestParameterMap()
					.get("parent");
		}

		if (paramParent == null && dir == null) {
			parent = ".";
		} else if (paramParent == null && dir != null) {
			parent = dir;
		} else if (paramParent != null && dir == null) {
			parent = paramParent;
		} else {
			parent = paramParent + File.separator + dir;
		}

	}

	public ListLogPage() throws Exception {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("host") != null) {
			host = (String) ctx.getExternalContext().getRequestParameterMap()
					.get("host");
		}

		if (ctx.getExternalContext().getRequestParameterMap().get("port") != null) {

			port = JStormUtils.parseInt(ctx.getExternalContext()
					.getRequestParameterMap().get("port"), 0);
		}

		getTargetDir(ctx);

		init();
	}

	private void init() throws Exception {

		try {

			if (port == 0) {
				Map conf = UIUtils.readUiConfig();

				port = ConfigExtension.getSupervisorDeamonHttpserverPort(conf);
			}

			portStr = String.valueOf(port);

			// proxy call
			listLogs();

		} catch (Exception e) {
			LOG.error(e.getCause(), e);
			throw e;
		}

	}

	private void parseString(String input) {
		Map<String, Map> map = (Map<String, Map>) JStormUtils
				.from_json(input);

		for (Map jobj : map.values()) {
			FileAttribute attribute = FileAttribute.fromJSONObject(jobj);
			if (attribute != null) {

				if (JStormUtils.parseBoolean(attribute.getIsDir(), false) == true) {
					dirs.add(attribute);
				} else {
					files.add(attribute);
				}

			}

		}

		summary = "There are " + files.size() + " files";
	}

	/**
	 * proxy query log for the specified task.
	 * 
	 * @param task
	 *            the specified task
	 */
	private void listLogs() {

		// PROXY_URL = "http://%s:%s/logview?%s=%s&dir=%s";
		String url = String.format(PROXY_URL, NetWorkUtils.host2Ip(host), port,
				HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD,
				HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_LIST,
				HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_DIR, parent);
		try {
			// 1. proxy call the task host log view service
			HttpClient client = HttpClientBuilder.create().build();
			HttpPost post = new HttpPost(url);
			HttpResponse response = client.execute(post);

			// 2. check the request is success, then read the log
			if (response.getStatusLine().getStatusCode() == 200) {
				String data = EntityUtils.toString(response.getEntity());

				parseString(data);

			} else {
				String data = EntityUtils.toString(response.getEntity());
				summary = ("Failed to get files\n" + data);
			}
		} catch (Exception e) {
			summary = ("Failed to get files\n" + e.getMessage());
			LOG.error(e.getCause(), e);
		}
	}

	public String getSummary() {
		return summary;
	}

	public void setSummary(String summary) {
		this.summary = summary;
	}

	public List<FileAttribute> getFiles() {
		return files;
	}

	public void setFiles(List<FileAttribute> files) {
		this.files = files;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public List<FileAttribute> getDirs() {
		return dirs;
	}

	public void setDirs(List<FileAttribute> dirs) {
		this.dirs = dirs;
	}

	public String getPortStr() {
		return portStr;
	}

	public void setPortStr(String portStr) {
		this.portStr = portStr;
	}

	public String getParent() {
		return parent;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}

	
}

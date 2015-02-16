package com.alibaba.jstorm.ui.model.data;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
@ManagedBean(name = "logpage")
@ViewScoped
public class LogPage implements Serializable {

	private static final long serialVersionUID = 4326599394273506083L;

	private static final Logger LOG = Logger.getLogger(LogPage.class);

	/**
	 * proxy url, which call the log service on the task node.
	 */
	private static final String PROXY_URL = "http://%s:%s/logview?%s=%s&log=%s";

	/**
	 * store the log content.
	 */
	private String log = "";

	private List<LogPageIndex> pages = new ArrayList<LogPageIndex>();

	/**
	 * Log file name
	 */
	private String logFileName = "Log";

	/**
	 * Http server port
	 */
	private int port;

	private String position;

	private Map conf;

	private String host;
	
	private String clusterName;

	public LogPage() throws Exception {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("clusterName") != null) {
			clusterName = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("clusterName");
		}
		
		if (ctx.getExternalContext().getRequestParameterMap()
				.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_POS) != null) {
			position = ctx.getExternalContext().getRequestParameterMap()
					.get(HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_POS);
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
			
			generateLogFileName();

			// proxy call
			queryLog(conf);

		} catch (Exception e) {
			LOG.error(e.getCause(), e);
			throw e;
		}
	}

	private void generateLogFileName() throws Exception {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("host") != null) {
			host = ctx.getExternalContext().getRequestParameterMap()
					.get("host");
		}
		
		String log = null;
		if (ctx.getExternalContext().getRequestParameterMap().get("log") != null) {
			log = ctx.getExternalContext().getRequestParameterMap()
					.get("log");
		}
		
		String workerPort = null;
		if (ctx.getExternalContext().getRequestParameterMap().get("workerPort") != null) {
			workerPort = ctx.getExternalContext().getRequestParameterMap()
					.get("workerPort");
		}

		if (StringUtils.isBlank(host) == false) {
			if (StringUtils.isBlank(log) == false) {
			    String parent = null;
			    if (ctx.getExternalContext().getRequestParameterMap().get("parent") != null) {
				    parent = ctx.getExternalContext().getRequestParameterMap()
						    .get("parent");
			    }
			
			    if (parent == null) {
				    logFileName = log;
			    }else {
				    logFileName = parent + File.separator + log;
			    }
			} else if (StringUtils.isBlank(workerPort) == false) {
				String topologyId = null;
				if (ctx.getExternalContext().getRequestParameterMap().get("topologyId") != null) {
				    topologyId = ctx.getExternalContext().getRequestParameterMap()
						    .get("topologyId");
			    }
				
				NimbusClient client = null;
				
				try {
					client = UIUtils.getNimbusClient(conf, clusterName);
					TopologyInfo summ = client.getClient().getTopologyInfo(topologyId);
					logFileName = JStormUtils.genLogName(summ.get_name(), Integer.valueOf(workerPort));
				}finally {
					if (client != null) {
						client.close();
					}
				}
			}
			return;
		}

		String topologyid = null;
		String taskid = null;
		String clusterName = null;

		// resolve the arguments
		if (ctx.getExternalContext().getRequestParameterMap().get("clusterName") != null) {
			clusterName = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("clusterName");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("topologyid") != null) {
			topologyid = ctx.getExternalContext().getRequestParameterMap()
					.get("topologyid");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("taskid") != null) {
			taskid = ctx.getExternalContext().getRequestParameterMap()
					.get("taskid");
		}

		if (topologyid == null) {
			throw new NotAliveException("Input topologyId is null ");
		}
		if (taskid == null) {
			throw new NotAliveException("Input taskId is null ");
		}

		NimbusClient client = null;

		try {
			client = UIUtils.getNimbusClient(conf, clusterName);

			TopologyInfo summ = client.getClient().getTopologyInfo(topologyid);

			// find the specified task entity
			TaskSummary taskSummary = null;
			for (TaskSummary _taskSummary : summ.get_tasks()) {
				if (taskid.equals(String.valueOf(_taskSummary.get_task_id()))) {
					taskSummary = _taskSummary;
					break;
				}
			}

			if (taskSummary == null) {
				throw new NotAliveException("topologyid=" + topologyid
						+ ", taskid=" + taskid);
			}

			ComponentTask componentTask = UIUtils.getComponentTask(taskSummary,
					topologyid);

			host = componentTask.getHost();

//			logFileName = componentTask.getTopologyid() + "-worker-"
//					+ componentTask.getPort() + ".log";
			logFileName = JStormUtils.genLogName(summ.get_name(), 
					Integer.valueOf(componentTask.getPort()));

		} catch (TException e) {
			LOG.error(e.getCause(), e);
			throw e;
		} catch (NotAliveException e) {
			LOG.error(e.getCause(), e);
			throw e;
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	private void insertPage(long index) {
		long pos = index * HttpserverUtils.HTTPSERVER_LOGVIEW_PAGESIZE;

		LogPageIndex page = new LogPageIndex();
		page.setIndex(String.valueOf(index));
		page.setPos(String.valueOf(pos));

		pages.add(page);
	}

	private void genPageUrl(String sizeStr) {
		long size = Long.valueOf(sizeStr);

		long item = (size + HttpserverUtils.HTTPSERVER_LOGVIEW_PAGESIZE - 1)
				/ HttpserverUtils.HTTPSERVER_LOGVIEW_PAGESIZE;

		if (item <= 10) {
			for (long i = item - 1; i >= 0; i--) {
				insertPage(i);
			}
			return;
		}

		long current = item - 1;

		if (position != null) {
			current = (Long.valueOf(position)
					+ HttpserverUtils.HTTPSERVER_LOGVIEW_PAGESIZE - 1)
					/ HttpserverUtils.HTTPSERVER_LOGVIEW_PAGESIZE;
		}

		if (item - current <= 5) {
			for (long i = item - 1; i > current; i--) {
				insertPage(i);
			}
		} else {
			insertPage(item - 1);
			for (long i = current + 4; i > current; i--) {
				insertPage(i);
			}
		}

		if (current >= 5) {
			for (long i = 1; i < 5; i++) {
				insertPage(current - i);
			}
			insertPage(Long.valueOf(0));
		} else {
			for (long i = current - 1; i >= 0; i--) {
				insertPage(i);
			}
		}
	}

	/**
	 * proxy query log for the specified task.
	 * 
	 * @param task
	 *            the specified task
	 */
	private void queryLog(Map conf) {
		// PROXY_URL = "http://%s:%s/logview?%s=%s&log=%s";
		String baseUrl = String.format(PROXY_URL, NetWorkUtils.host2Ip(host), port,
				HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD,
				HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_SHOW, logFileName);
		String url = baseUrl;
		if (position != null) {
			url += ("&" + HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_POS + "=" + position);
		}
		try {
			// 1. proxy call the task host log view service
			HttpClient client = HttpClientBuilder.create().build();
			HttpPost post = new HttpPost(url);
			HttpResponse response = client.execute(post);

			// 2. check the request is success, then read the log
			if (response.getStatusLine().getStatusCode() == 200) {
				String data = EntityUtils.toString(response.getEntity(), ConfigExtension.getLogViewEncoding(conf));

				String sizeStr = data.substring(0, 16);
				genPageUrl(sizeStr);

				setLog(data);
			} else {
				setLog(EntityUtils.toString(response.getEntity()));
			}
		} catch (Exception e) {
			setLog(e.getMessage());
			LOG.error(e.getCause(), e);
		}
	}

	/**
	 * get the log content
	 * 
	 * @return log content
	 */
	public String getLog() {
		return log;
	}

	/**
	 * set the log content
	 * 
	 * @param log
	 *            log content
	 */
	public void setLog(String log) {
		this.log = log;
	}

	public List<LogPageIndex> getPages() {
		return pages;
	}

	public void setPages(List<LogPageIndex> pages) {
		this.pages = pages;
	}

	public String getPort() {
		return String.valueOf(port);
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getPosition() {
		return position;
	}

	public void setPosition(String position) {
		this.position = position;
	}

	public String getLogFileName() {
		return logFileName;
	}

	public void setLogFileName(String logFileName) {
		this.logFileName = logFileName;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	
	

}

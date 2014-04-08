package com.alibaba.jstorm.ui.model.data;

import java.io.Serializable;
import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ComponentTask;

@ManagedBean(name = "logpage")
@ViewScoped
public class LogPage implements Serializable {
	
	private static final long serialVersionUID = 4326599394273506083L;

	private static final Logger LOG = Logger.getLogger(LogPage.class);
	
	private static final String START_AGENT = "http://%s:8182/tail?task_id=aloha&file=%s&encode=text&hungry=true&maxsize=1";
	
	private String topologyid = null;
	private String taskid = null;
	
	private ComponentTask componentTask = null;
	
	private String log = "";
	
	public LogPage() throws TException, NotAliveException {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("topologyid") != null) {
			topologyid = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("topologyid");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("taskid") != null) {
			taskid  = ctx.getExternalContext().getRequestParameterMap()
					.get("taskid");
		}

		if (topologyid == null) {
			throw new NotAliveException("Input topologyId is null ");
		}
		if (taskid == null) {
			throw new NotAliveException("Input taskId is null ");
		}

		init();
	}
	
	private void init() throws TException, NotAliveException {
		NimbusClient client = null;

		try {
			Map conf = UIUtils.readUiConfig();
			client = NimbusClient.getConfiguredClient(conf);

			TopologyInfo summ = client.getClient().getTopologyInfo(topologyid);

			TaskSummary taskSummary = null;
			for (TaskSummary _taskSummary : summ.get_tasks()) {
				if (taskid.equals(String.valueOf(_taskSummary.get_task_id()))) {
					taskSummary = _taskSummary;
					break;
				}
			}

			if (taskSummary == null) {
				throw new NotAliveException("topologyid=" + topologyid + ", taskid=" + taskid);
			}
			
			componentTask = UIUtils.getComponentTask(taskSummary, topologyid);

			queryLogForTask(componentTask);
			
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
	
	private void queryLogForTask(ComponentTask task) {
		
		String host = task.getHost();
		String logFile = "/home/admin/aloha/logs/" + task.getTopologyid() + "-worker-" + task.getPort() + ".log";
		
		host="10.232.12.32";
		logFile="/home/admin/diamond/logs/client-request.log";
		
		String url = String.format(START_AGENT, host, logFile);
//		HttpUtils.HttpResponse response = HttpUtils.httpGet(url);
		
//		if (response.getStatus() == 200) {
//			setLog(response.getBody());
//		}
		
	}
	
	public ComponentTask getComponentTask() {
		return componentTask;
	}

	public void setComponentTask(ComponentTask componentTask) {
		this.componentTask = componentTask;
	}
	
	public String getLog() {
		return log;
	}

	public void setLog(String log) {
		this.log = log;
	}
	

}

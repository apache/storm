package com.alibaba.jstorm.ui.model.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import org.apache.log4j.Logger;
import org.apache.thrift7.TException;

import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.common.stats.StatBuckets;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ErrorSummary;
import com.alibaba.jstorm.ui.model.TaskSumm;

@ManagedBean(name = "taskpage")
@ViewScoped
public class Taskpage {

	private static final Logger LOG = Logger.getLogger(Taskpage.class);

	private String topologyid = null;
	private String componentId = null;
	private String taskid = null;
	private String window = null;
	private List<TaskSumm> tsumms = null;
	private List<ErrorSummary> esumms = null;
	private String clusterName;

	public Taskpage() throws TException, NotAliveException {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("clusterName") != null) {
			clusterName = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("clusterName");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("topologyid") != null) {
			topologyid = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("topologyid");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("taskid") != null) {
			taskid = (String) ctx.getExternalContext().getRequestParameterMap()
					.get("taskid");
		}

		if (topologyid == null) {
			throw new NotAliveException("Input topologyId is null ");
		}

		window = UIUtils.getWindow(ctx);
		init();
	}

	private void init() throws TException, NotAliveException {

		NimbusClient client = null;

		try {
			Map conf = Utils.readStormConfig();
			client = UIUtils.getNimbusClient(conf, clusterName);

			TopologyInfo summ = client.getClient().getTopologyInfo(topologyid);

			List<TaskSummary> ts = summ.get_tasks();
			if (ts == null) {
				throw new NotAliveException("No TaskSummary");
			}
			TaskSummary t = null;
			if (ts != null) {
				int tssize = ts.size();
				for (int i = 0; i < tssize; i++) {
					if (ts.get(i).get_task_id() == Integer.parseInt(taskid)) {
						t = ts.get(i);
					}
				}
			}

			tsumms = taskSummaryTable(t, summ);
			esumms = taskErrorTable(t);
		} catch (TException e) {
			LOG.error(e.getCause(), e);
			throw e;
		} catch (NotAliveException e) {
			LOG.error(e.getCause(), e);
			throw e;
		} catch (Exception e) {
			LOG.error(e.getCause(), e);
			throw new TException(e);
		}finally {
			if (client != null) {
				client.close();
			}
		}

	}

	public List<TaskSumm> taskSummaryTable(TaskSummary task, TopologyInfo summ) {
		List<TaskSumm> tsums = new ArrayList<TaskSumm>();
		TaskSumm tsumm = new TaskSumm(String.valueOf(task.get_task_id()),
				task.get_host(), String.valueOf(task.get_port()),
				summ.get_name(), task.get_component_id(),
				StatBuckets.prettyUptimeStr(task.get_uptime_secs()));

		tsums.add(tsumm);
		return tsums;
	}

	public List<ErrorSummary> taskErrorTable(TaskSummary task) {
		List<ErrorSummary> esums = new ArrayList<ErrorSummary>();
		List<ErrorInfo> errors = task.get_errors();
		if (errors != null) {
			int errorsize = errors.size();

			for (int i = 0; i < errorsize; i++) {
				ErrorInfo einfo = errors.get(i);
				ErrorSummary esumm = new ErrorSummary(String.valueOf(einfo
						.get_error_time_secs()), einfo.get_error());

				esums.add(esumm);
			}
		}
		return esums;
	}

	public List<TaskSumm> getTsumms() {
		return tsumms;
	}

	public void setTsumms(List<TaskSumm> tsumms) {
		this.tsumms = tsumms;
	}

	public List<ErrorSummary> getEsumms() {
		return esumms;
	}

	public void setEsumms(List<ErrorSummary> esumms) {
		this.esumms = esumms;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
	
	
}

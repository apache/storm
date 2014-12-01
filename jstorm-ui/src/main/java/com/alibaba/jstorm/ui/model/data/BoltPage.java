/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.jstorm.ui.model.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import org.apache.log4j.Logger;
import org.apache.thrift7.TException;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TaskMetricData;
import backtype.storm.generated.TaskStats;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologyMetricInfo;
import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.stats.StatBuckets;
import com.alibaba.jstorm.common.stats.StaticsType;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ComponentInput;
import com.alibaba.jstorm.ui.model.ComponentOutput;
import com.alibaba.jstorm.ui.model.ComponentSummary;
import com.alibaba.jstorm.ui.model.ComponentTask;
import com.alibaba.jstorm.ui.model.WinComponentStats;
import com.alibaba.jstorm.ui.model.TaskMetrics;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * 
 * @author xin.zhou
 */
@ManagedBean(name = "boltpage")
@ViewScoped
public class BoltPage implements Serializable {

	private static final long serialVersionUID = 2629472722725558979L;

	private static final Logger LOG = Logger.getLogger(BoltPage.class);

	private String clusterName = null;
	private String topologyid = null;
	private String window = null;
	private String componentid = null;
	private String topologyName = null;
	private List<ComponentSummary> coms = null;
	private List<WinComponentStats> comstats = null;
	private List<ComponentOutput> coos = null;
	private List<ComponentInput> cois = null;
	private List<ComponentTask> cts = null;
	private List<TaskMetrics> taskmetrics = null;

	public BoltPage() throws TException, NotAliveException {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("clusterName") != null) {
			clusterName = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("clusterName");
		}
		
		if (ctx.getExternalContext().getRequestParameterMap().get("topologyid") != null) {
			topologyid = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("topologyid");
		}

		if (ctx.getExternalContext().getRequestParameterMap()
				.get("componentid") != null) {
			componentid = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("componentid");
		}

		window = UIUtils.getWindow(ctx);

		if (topologyid == null) {
			throw new NotAliveException("Input topologyId is null ");
		}

		init();
	}

	public BoltPage(String clusterName, String topologyId, String componentId, String window)
			throws TException, NotAliveException {
		this.clusterName = clusterName;
		this.topologyid = topologyId;
		this.componentid = componentId;
		this.window = window;

		init();
	}

	private List<ComponentSummary> getComponentSummaries(TopologyInfo summ,
			List<TaskSummary> ts) {
		List<ComponentSummary> ret = new ArrayList<ComponentSummary>();

		ComponentSummary cs = new ComponentSummary();

		cs.setComponentId(componentid);
		cs.setTopologyname(summ.get_name());
		cs.setParallelism(String.valueOf(ts.size()));

		ret.add(cs);

		return ret;
	}

	private List<ComponentTask> getComponentTasks(List<TaskSummary> taskList,
			String window) {
		List<ComponentTask> ret = new ArrayList<ComponentTask>();

		for (TaskSummary task : taskList) {
			ComponentTask componentTask = UIUtils.getComponentTask(task, topologyid);
			
			if (componentTask.getStatus().equals(ConfigExtension.TASK_STATUS_ACTIVE)) {
			    TaskStats taskStats = task.get_stats();

			    Map<String, Long> emitted = UIUtils.mergeStream(
					    taskStats.get_emitted(), Long.valueOf(0));
			    Map<String, Double> sendTps = UIUtils.mergeStream(
					    taskStats.get_send_tps(), Double.valueOf(0));
			    Map<String, Double> recvTps = UIUtils.mergeStream(
					    taskStats.get_recv_tps(), Double.valueOf(0));
			    Map<String, Long> acked = UIUtils.mergeStream(
					    taskStats.get_acked(), Long.valueOf(0));
			    Map<String, Long> failed = UIUtils.mergeStream(
					    taskStats.get_failed(), Long.valueOf(0));
			    Map<String, Double> process = UIUtils.mergeStream(
					    taskStats.get_process_ms_avg(), Double.valueOf(0));		

			    componentTask.setEmitted(JStormUtils.formatValue(emitted
					    .get(window)));
			    componentTask.setSendTps(JStormUtils.formatValue(sendTps
					    .get(window)));
			    componentTask.setRecvTps(JStormUtils.formatValue(recvTps
					    .get(window)));
			    componentTask.setAcked(JStormUtils.formatValue(acked.get(window)));
			    componentTask
					    .setFailed(JStormUtils.formatValue(failed.get(window)));
			    componentTask.setProcess(JStormUtils.formatValue(process
					    .get(window)));
			}

			ret.add(componentTask);
		}

		return ret;
	}

	private List<WinComponentStats> getWinComponentStats(
			List<TaskSummary> taskList, String window) {
		List<WinComponentStats> ret = new ArrayList<WinComponentStats>();

		Map<StaticsType, Object> staticsMap = UIUtils.mergeTasks(taskList,
				window);

		WinComponentStats winComponentStats = new WinComponentStats();

		winComponentStats.setWindow(window);
		winComponentStats.setValues(staticsMap);

		ret.add(winComponentStats);

		return ret;
	}

	private void getInputOutputSummary(List<TaskSummary> taskSummaries,
			String window) {
		coos = new ArrayList<ComponentOutput>();
		cois = new ArrayList<ComponentInput>();

		List<Map<String, Long>> emittedList = new ArrayList<Map<String, Long>>();
		List<Map<String, Double>> sendTpsList = new ArrayList<Map<String, Double>>();
		List<Map<GlobalStreamId, Double>> recvTpsList = new ArrayList<Map<GlobalStreamId, Double>>();
		List<Map<GlobalStreamId, Long>> ackedList = new ArrayList<Map<GlobalStreamId, Long>>();
		List<Map<GlobalStreamId, Long>> failedList = new ArrayList<Map<GlobalStreamId, Long>>();
		List<Map<GlobalStreamId, Double>> processList = new ArrayList<Map<GlobalStreamId, Double>>();

		for (TaskSummary taskSummary : taskSummaries) {
			if (taskSummary.get_status() == null) {
				// this is for old JStorm version
				taskSummary.set_status(ConfigExtension.TASK_STATUS_ACTIVE);
			}
			
			if (taskSummary.get_status().equals(ConfigExtension.TASK_STATUS_ACTIVE) == false)
				continue;
			
			TaskStats taskStats = taskSummary.get_stats();

			emittedList.add(taskStats.get_emitted().get(window));
			sendTpsList.add(taskStats.get_send_tps().get(window));
			recvTpsList.add(taskStats.get_recv_tps().get(window));
			ackedList.add(taskStats.get_acked().get(window));
			failedList.add(taskStats.get_failed().get(window));
			processList.add(taskStats.get_process_ms_avg().get(window));

		}

		Map<String, Long> emitted = JStormUtils.mergeMapList(emittedList);
		Map<String, Double> sendTps = JStormUtils.mergeMapList(sendTpsList);
		Map<GlobalStreamId, Double> recvTps = JStormUtils
				.mergeMapList(recvTpsList);
		Map<GlobalStreamId, Long> acked = JStormUtils.mergeMapList(ackedList);
		Map<GlobalStreamId, Long> failed = JStormUtils.mergeMapList(failedList);
		
		Map<GlobalStreamId, Double> processSum = JStormUtils
                .mergeMapList(processList);
        
        Map<GlobalStreamId, Double> process = new HashMap<GlobalStreamId, Double>();
        for (Entry<GlobalStreamId, Double> entry : processSum.entrySet()) {
            GlobalStreamId streamId = entry.getKey();
            Double value = entry.getValue();
            
            if (value != null && processList.size() > 0) {
                process.put(streamId, value/processList.size());
            }
        }

		for (Entry<String, Long> emittedEntry : emitted.entrySet()) {
			String outputStreamId = emittedEntry.getKey();
			Long emittedValue = emittedEntry.getValue();
			Double sendTpsValue = sendTps.get(outputStreamId);

			ComponentOutput co = new ComponentOutput();

			co.setValues(outputStreamId, emittedValue, sendTpsValue);

			coos.add(co);
		}

		for (Entry<GlobalStreamId, Double> entry : process.entrySet()) {
			GlobalStreamId streamId = entry.getKey();
			Double processValue = entry.getValue();

			Double recvTpsValue = recvTps.get(streamId);
			Long ackedValue = acked.get(streamId);
			Long failedValue = failed.get(streamId);

			ComponentInput ci = new ComponentInput();
			ci.setValues(streamId, recvTpsValue, processValue, ackedValue,
					failedValue);

			cois.add(ci);

		}

		return;

	}
	public List<TaskMetrics> getTaskMetricsList(List<TaskMetricData> totalTskMetrList) {
		if (totalTskMetrList == null) return null;
    	List<TaskMetrics> ret = new ArrayList<TaskMetrics>();
    	LOG.debug("get task metrics list: component ID: " + this.componentid);
	    for (TaskMetricData taskMetricData : totalTskMetrList) {
	    	if ((taskMetricData.get_component_id()).equals(this.componentid)) {
	    		TaskMetrics taskMetircs = new TaskMetrics();
	    		taskMetircs.updateTaskMetricData(taskMetricData);
	    		ret.add(taskMetircs);
	    	}
	    }
	    return ret;
	}

	@SuppressWarnings("rawtypes")
	private void init() throws TException, NotAliveException {

		NimbusClient client = null;

		try {
			Map conf = UIUtils.readUiConfig();
			client = UIUtils.getNimbusClient(conf, clusterName);

			TopologyInfo summ = client.getClient().getTopologyInfo(topologyid);
			StormTopology topology = client.getClient().getTopology(topologyid);
			TopologyMetricInfo topologyMetricInfo = client.getClient().getTopologyMetric(topologyid);

			String type = UIUtils.componentType(topology, componentid);

			List<TaskSummary> ts = UIUtils.getTaskList(summ.get_tasks(),
					componentid);
			
			topologyName = summ.get_name();

			coms = getComponentSummaries(summ, ts);

			cts = getComponentTasks(ts, window);

			comstats = getWinComponentStats(ts, window);

			getInputOutputSummary(ts, window);
			List<TaskMetricData> totoaltaskmetrics = topologyMetricInfo.get_task_metric_list();
			taskmetrics = getTaskMetricsList(totoaltaskmetrics);

		} catch (TException e) {
			LOG.error(e.getCause(), e);
			throw e;
		} catch (NotAliveException e) {
			LOG.error(e.getCause(), e);
			throw e;
		} catch (Exception e) {
			LOG.error(e.getCause(), e);
			throw new TException(e);
		} finally {
			if (client != null) {
				client.close();
			}
		}

	}

	public List<WinComponentStats> getComstats() {
		return comstats;
	}

	public void setComstats(List<WinComponentStats> comstats) {
		this.comstats = comstats;
	}

	public List<ComponentOutput> getCoos() {
		return coos;
	}

	public void setCoos(List<ComponentOutput> coos) {
		this.coos = coos;
	}

	public List<ComponentInput> getCois() {
		return cois;
	}

	public void setCois(List<ComponentInput> cois) {
		this.cois = cois;
	}

	public List<ComponentTask> getCts() {
		return cts;
	}

	public void setCts(List<ComponentTask> cts) {
		this.cts = cts;
	}

	public List<ComponentSummary> getComs() {
		return coms;
	}

	public void setComs(List<ComponentSummary> coms) {
		this.coms = coms;
	}
	
	public List<TaskMetrics> gettaskmetrics() {
		return this.taskmetrics;
	}
	
	public void settaskmetrics(List<TaskMetrics> taskmetrs) {
		this.taskmetrics = taskmetrs;
	}

	public String getTopologyName() {
		return topologyName;
	}

	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}

	public static void main(String[] args) {
		try {
			BoltPage instance = new BoltPage("/jstorm", "sequence_test-3-1363789458",
					"Total", StatBuckets.ALL_WINDOW_STR);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

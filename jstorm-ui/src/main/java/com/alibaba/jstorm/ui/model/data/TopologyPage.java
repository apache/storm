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

import backtype.storm.Config;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.UserDefMetric;
import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.common.stats.StatBuckets;
import com.alibaba.jstorm.metric.UserDefMetricData;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.Components;
import com.alibaba.jstorm.ui.model.TopologySumm;
import com.alibaba.jstorm.ui.model.WinComponentStats;
import com.alibaba.jstorm.utils.JStormUtils;

/**
 * 
 * @author xin.zhou/Longda
 */
@ManagedBean(name = "topologypage")
@ViewScoped
public class TopologyPage implements Serializable {

	private static final long serialVersionUID = -214838419818487219L;

	private static final Logger LOG = Logger.getLogger(TopologyPage.class);

	private String clusterName = null;
	private String topologyid = null;
	private String window = null;
	private List<TopologySumm> tsumm = null;
	private List<WinComponentStats> tstats = null;
	private List<Components> scom = null;
	private List<Components> bcom = null;
	private List<UserDefMetric> udm = null;

	public TopologyPage() throws Exception {

		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("clusterName") != null) {
			clusterName = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("clusterName");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("topologyid") != null) {
			topologyid = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("topologyid");
		}
		LOG.debug("Query clusterName=" + clusterName + ", topology=" + topologyid);

		window = UIUtils.getWindow(ctx);
		LOG.info("Window:" + window);

		if (topologyid == null) {

			LOG.error("Not set topologyid");
			throw new NotAliveException("Input topologyId is null ");

		}

		init();
	}

	public TopologyPage(String clusterName, String topologyId, String window) throws Exception {
		this.clusterName = clusterName;
		this.topologyid = topologyId;
		this.window = window;

		LOG.info("Window:" + window);
		init();
	}

	private void init() throws Exception {

		NimbusClient client = null;

		try {
			Map conf = UIUtils.readUiConfig();
			
			client = UIUtils.getNimbusClient(conf, clusterName);

			TopologyInfo summ = client.getClient().getTopologyInfo(topologyid);
			StormTopology topology = client.getClient().getTopology(topologyid);

			List<TaskSummary> ts = summ.get_tasks();
			
			udm = summ.get_userDefMetric();

			tsumm = UIUtils.topologySummary(summ);

			getComponents(ts, topology);

			tstats = topologyStatsTable(scom, bcom);

		} catch (Exception e) {
			LOG.error("Failed to get topology information,", e);
			throw e;
		} finally {

			if (client != null) {
				client.close();
			}
		}
	}

	private List<Components> getComponents(
			Map<String, List<TaskSummary>> componentMap, String type) {
		List<Components> components = new ArrayList<Components>();

		for (Entry<String, List<TaskSummary>> entry : componentMap.entrySet()) {
			String componentId = entry.getKey();
			List<TaskSummary> taskList = entry.getValue();

			Components component = UIUtils.getComponent(taskList, componentId,
					type, window);

			String lastError = UIUtils.mostRecentError(taskList);
			component.setLastError(lastError);

			components.add(component);

		}

		return components;
	}

	/**
	 * get spout or bolt's List<Components>
	 * 
	 * 
	 * @param ts
	 * @param topology
	 * @throws NotAliveException
	 */
	private void getComponents(List<TaskSummary> ts, StormTopology topology)
			throws NotAliveException {
		if (ts == null) {
			LOG.error("Task list is empty");
			throw new NotAliveException("Task list is empty");
		}

		Map<String, List<TaskSummary>> spoutTasks = new HashMap<String, List<TaskSummary>>();
		Map<String, List<TaskSummary>> boltTasks = new HashMap<String, List<TaskSummary>>();

		for (TaskSummary t : ts) {
			if (t == null) {
				continue;
			}

			String componentid = t.get_component_id();
			String componentType = UIUtils.componentType(topology, componentid);
			if (componentType.equals(UIUtils.SPOUT_STR)) {

				UIUtils.addTask(spoutTasks, t, componentid);
			} else if (componentType.equals(UIUtils.BOLT_STR)) {
				UIUtils.addTask(boltTasks, t, componentid);
			}

		}

		scom = getComponents(spoutTasks, UIUtils.SPOUT_STR);
		bcom = getComponents(boltTasks, UIUtils.BOLT_STR);
	}

	private List<WinComponentStats> topologyStatsTable(List<Components> scom,
			List<Components> bcom) {
		List<Components> all = new ArrayList<Components>();
		all.addAll(scom);
		all.addAll(bcom);

		Long emitted = 0l;
		Double sendTps = 0.0;
		Double recvTps = 0.0;
		Long acked = 0l;
		Long failed = 0l;
		for (Components component : all) {
			emitted += Long.valueOf(component.getEmitted());
			sendTps += Double.valueOf(component.getSendTps());
			recvTps += Double.valueOf(component.getRecvTps());
			acked += Long.valueOf(component.getAcked());
			failed += Long.valueOf(component.getFailed());

		}

		Double process = 0.0;
		Long spoutNum = Long.valueOf(0);
		for (Components component : scom) {
			if (component.getProcess() != null) {
				process += (Double.valueOf(component.getProcess()) * Long
						.valueOf(component.getParallelism()));
			}
			spoutNum = Long.valueOf(component.getParallelism());
		}
		Double avergProcess = process / spoutNum;

		WinComponentStats topologyStats = new WinComponentStats();
		topologyStats.setWindow(window);
		topologyStats.setEmitted(JStormUtils.formatValue(emitted));
		topologyStats.setSendTps(JStormUtils.formatValue(sendTps));
		topologyStats.setRecvTps(JStormUtils.formatValue(recvTps));
		topologyStats.setAcked(JStormUtils.formatValue(acked));
		topologyStats.setFailed(JStormUtils.formatValue(failed));
		topologyStats.setProcess(JStormUtils.formatValue(avergProcess));

		List<WinComponentStats> tss = new ArrayList<WinComponentStats>();
		tss.add(topologyStats);
		return tss;
	}
	
	public List<UserDefMetric> getUdm() {
	    return udm;
	}
	
	public void setUdm(List<UserDefMetric> udm) {
	    this.udm = udm;
	}

	public String getTopologyid() {
		return topologyid;
	}

	public void setTopologyid(String topologyid) {
		this.topologyid = topologyid;
	}

	public String getWindow() {
		return window;
	}

	public void setWindow(String window) {
		this.window = window;
	}

	public List<TopologySumm> getTsumm() {
		return tsumm;
	}

	public void setTsumm(List<TopologySumm> tsumm) {
		this.tsumm = tsumm;
	}

	public List<WinComponentStats> getTstats() {
		return tstats;
	}

	public void setTstats(List<WinComponentStats> tstats) {
		this.tstats = tstats;
	}

	public List<Components> getScom() {
		return scom;
	}

	public void setScom(List<Components> scom) {
		this.scom = scom;
	}

	public List<Components> getBcom() {
		return bcom;
	}

	public void setBcom(List<Components> bcom) {
		this.bcom = bcom;
	}

	public static void main(String[] args) {

		try {
			TopologyPage instance = new TopologyPage("jstorm", 
					"sequence_test-1-1386516240", StatBuckets.ALL_WINDOW_STR);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

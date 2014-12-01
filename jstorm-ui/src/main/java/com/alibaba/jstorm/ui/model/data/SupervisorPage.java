package com.alibaba.jstorm.ui.model.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.SupervisorWorkers;
import backtype.storm.generated.TopologyMetricInfo;
import backtype.storm.generated.WorkerSummary;
import backtype.storm.generated.WorkerMetricData;
import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.SupervisorSumm;
import com.alibaba.jstorm.ui.model.WorkerSumm;
import com.alibaba.jstorm.ui.model.WorkerMetrics;

/**
 * 
 * @author xin.zhou/Longda
 */
@ManagedBean(name = "supervisorpage")
@ViewScoped
public class SupervisorPage implements Serializable {

	private static final long serialVersionUID = -6103468103521877721L;

	private static final Logger LOG = Logger.getLogger(SupervisorPage.class);

	private String clusterName = null;
	private String host = "localhost";
	private String ip = null;

	private List<SupervisorSumm> ssumm = null;
	private List<WorkerSumm> wsumm = null;
	private List<String> topologyList = null;
	private List<TopologyMetricInfo> topologyMetricsList = null;
	private List<WorkerMetrics> workermetrics = null;

	public SupervisorPage() throws Exception {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("clusterName") != null) {
			clusterName = (String) ctx.getExternalContext().getRequestParameterMap()
					.get("clusterName");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("host") != null) {
			host = (String) ctx.getExternalContext().getRequestParameterMap()
					.get("host");
		}

		init(host);
	}

	@SuppressWarnings("rawtypes")
	public void init(String host) throws Exception {

		NimbusClient client = null;
		try {

			Map conf = UIUtils.readUiConfig();
			client = UIUtils.getNimbusClient(conf, clusterName);
			SupervisorWorkers supervisorWorkers = client.getClient()
					.getSupervisorWorkers(host);
			ssumm = new ArrayList<SupervisorSumm>();
			SupervisorSumm supervSumm = new SupervisorSumm(supervisorWorkers.get_supervisor());
			ssumm.add(supervSumm);
			ip = supervSumm.getIp();
			generateWorkerSum(supervisorWorkers.get_workers());
			getTopoList();
			getTopoMetrList(client);
			getWorkerMetrData();

		} catch (Exception e) {
			LOG.error("Failed to get cluster information:", e);
			throw e;
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	private void generateWorkerSum(List<WorkerSummary> workerSummaries) {
		wsumm = new ArrayList<WorkerSumm>();

		for (WorkerSummary workerSummary : workerSummaries) {
			wsumm.add(new WorkerSumm(workerSummary));
		}
	}

	public List<SupervisorSumm> getSsumm() {
		return ssumm;
	}

	public void setSsumm(List<SupervisorSumm> ssumm) {
		this.ssumm = ssumm;
	}

	public List<WorkerSumm> getWsumm() {
		return wsumm;
	}

	public void setWsumm(List<WorkerSumm> wsumm) {
		this.wsumm = wsumm;
	}
	public void setworkermetrics(List<WorkerMetrics> wrkMetrList) {
		this.workermetrics = wrkMetrList;
	}
	public List<WorkerMetrics> getworkermetrics(){
		return this.workermetrics;
	}
	public void getTopoList() {
		if (topologyList == null) {
		    topologyList = new ArrayList<String>();
		}
		if (wsumm == null) return;
		for(WorkerSumm workerSumm : wsumm) {
			String topologyId = workerSumm.getTopology();
			if (!(topologyList.contains(topologyId))) {
				topologyList.add(topologyId);
			}
		}
	}
	public void getTopoMetrList(NimbusClient client) throws Exception {
		if (topologyList == null) return;
		if (topologyMetricsList == null) {
			topologyMetricsList = new ArrayList<TopologyMetricInfo>();
		}
		for (String topologyId : topologyList) {
			try {
			    TopologyMetricInfo topoMetrInfo = client.getClient().getTopologyMetric(topologyId);
			    topologyMetricsList.add(topoMetrInfo);
			} catch (Exception e) {
				LOG.error("Failed to get topology metrics information:", e);
				throw e;
			}
		}
	}
	public void getWorkerMetrData() {
		if (topologyMetricsList == null) return;
		if (workermetrics == null) {
			workermetrics = new ArrayList<WorkerMetrics>();
		}
		for (TopologyMetricInfo topoMetr : topologyMetricsList) {
			List<WorkerMetricData> wrkMetrLstFromTopo = topoMetr.get_worker_metric_list();
			if (wrkMetrLstFromTopo == null) return;
			for (WorkerMetricData wrkMetrData : wrkMetrLstFromTopo) {
				if (wrkMetrData.get_hostname().equals(host) ||
						wrkMetrData.get_hostname().equals(ip)) {
					WorkerMetrics workerMetrics = new WorkerMetrics();
					workerMetrics.updateWorkerMetricData(wrkMetrData);
					workermetrics.add(workerMetrics);
				}
			}
		}
	}

	public static void main(String[] args) {
		try {
			SupervisorPage m = new SupervisorPage();
			// m.init("free-56-151.shucang.alipay.net");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
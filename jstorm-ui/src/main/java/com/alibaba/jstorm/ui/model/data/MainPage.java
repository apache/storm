package com.alibaba.jstorm.ui.model.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.cluster.ClusterState;
import com.alibaba.jstorm.common.stats.StatBuckets;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ClusterInfo;
import com.alibaba.jstorm.ui.model.ClusterSumm;
import com.alibaba.jstorm.ui.model.NimbusSlave;
import com.alibaba.jstorm.ui.model.SupervisorSumm;
import com.alibaba.jstorm.ui.model.TopologySumm;
import com.alibaba.jstorm.utils.NetWorkUtils;
import com.alibaba.jstorm.zk.ZkTool;
import com.google.common.collect.Lists;

/**
 * 
 * @author xin.zhou/Longda
 */
@ManagedBean(name = "mainpage")
@ViewScoped
public class MainPage implements Serializable {

	private static final long serialVersionUID = -6103468103521877721L;

	private static final Logger LOG = Logger.getLogger(MainPage.class);

	private String host = "localhost";

	private String clusterName = null;
	private ClusterSummary summ = null;
	private List<ClusterSumm> csumm = null;
	private List<TopologySumm> tsumm = null;
	private List<SupervisorSumm> ssumm = null;

	private List<NimbusSlave> slaves = null;
	private List<String> zkServers = null;
	private String zkPort = null;

	public MainPage() throws Exception {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("clusterName") != null) {
			clusterName = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("clusterName");
		}
		
		init();
	}

	@SuppressWarnings("rawtypes")
	private void init() throws Exception {

		NimbusClient client = null;
		ClusterState cluster_state = null;
		try {
			LOG.info("MainPage init...");
			Map conf = UIUtils.readUiConfig();
			
			client = UIUtils.getNimbusClient(conf, clusterName);
			summ = client.getClient().getClusterInfo();

			tsumm = UIUtils.topologySummary(summ.get_topologies());
			csumm = UIUtils.clusterSummary(summ, client, conf);
			ssumm = UIUtils.supervisorSummary(summ.get_supervisors());

			cluster_state = ZkTool.mk_distributed_cluster_state(client
					.getConf());
			slaves = getNimbusSlave(cluster_state, conf);

			zkServers = getZkServer(conf);
			zkPort = String.valueOf(conf.get(Config.STORM_ZOOKEEPER_PORT));

		} catch (Exception e) {
			String errorInfo = e.getMessage();
            if (errorInfo != null) {
			    if (errorInfo.indexOf("No alive nimbus") == -1) {
			        LOG.error("Failed to get cluster information:", e);
			        throw e;
			    }
            }
		} finally {
			if (client != null) {
				client.close();
			}
			if (cluster_state != null) {
				cluster_state.close();
			}
		}
	}

	private List<NimbusSlave> getNimbusSlave(ClusterState cluster_state,
			Map conf) throws Exception {
		int port = ConfigExtension.getNimbusDeamonHttpserverPort(conf);
		List<NimbusSlave> slaves = Lists.newArrayList();
		Map<String, String> followerMap = ZkTool.get_followers(cluster_state);
		if (!followerMap.isEmpty()) {
			for (Entry<String, String> entry : followerMap.entrySet()) {
				String uptime = StatBuckets.prettyUptimeStr(Integer
						.valueOf(entry.getValue()));
				slaves.add(new NimbusSlave(entry.getKey(), uptime, port));
			}
		}
		return slaves;
	}

	private List<String> getZkServer(Map conf) {
		List<String> servers = Lists.newArrayList();
		for (String ip : (List<String>) conf
				.get(Config.STORM_ZOOKEEPER_SERVERS)) {
			servers.add(NetWorkUtils.ip2Host(ip));
		}
		return servers;
	}

	public List<ClusterSumm> getCsumm() {
		return csumm;
	}

	public ClusterSummary getSumm() {
		return summ;
	}

	public void setSumm(ClusterSummary summ) {
		this.summ = summ;
	}

	public void setCsumm(List<ClusterSumm> csumm) {
		this.csumm = csumm;
	}

	public void setTsumm(List<TopologySumm> tsumm) {
		this.tsumm = tsumm;
	}

	public void setSsumm(List<SupervisorSumm> ssumm) {
		this.ssumm = ssumm;
	}

	public List<TopologySumm> getTsumm() {

		return tsumm;
	}

	public List<SupervisorSumm> getSsumm() {

		return ssumm;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public List<NimbusSlave> getSlaves() {
		return slaves;
	}

	public void setSlaves(List<NimbusSlave> slaves) {
		this.slaves = slaves;
	}

	public List<String> getZkServers() {
		return zkServers;
	}

	public void setZkServers(List<String> zkServers) {
		this.zkServers = zkServers;
	}

	public String getZkPort() {
		return zkPort;
	}

	public void setZkPort(String zkPort) {
		this.zkPort = zkPort;
	}

	public static void main(String[] args) {
		try {
			MainPage m = new MainPage();
			System.out.println(m.getCsumm());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

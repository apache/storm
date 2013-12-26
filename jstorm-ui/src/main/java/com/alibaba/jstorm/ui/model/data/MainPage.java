package com.alibaba.jstorm.ui.model.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ClusterSumm;
import com.alibaba.jstorm.ui.model.SupervisorSumm;
import com.alibaba.jstorm.ui.model.TopologySumm;

/**
 * 
 * @author xin.zhou/Longda
 */
@ManagedBean(name = "mainpage")
@ViewScoped
public class MainPage implements Serializable {

	private static final long serialVersionUID = -6103468103521877721L;

	private static final Logger  LOG   = Logger.getLogger(MainPage.class);
    
    private String               host  = "localhost";
    
    private ClusterSummary       summ  = null;
    private List<ClusterSumm>    csumm = null;
    private List<TopologySumm>   tsumm = null;
    private List<SupervisorSumm> ssumm = null;
    
    public MainPage() throws Exception {
        init();
    }
    
    @SuppressWarnings("rawtypes")
    private void init() throws Exception {
        
        NimbusClient client = null;
        try {

            Map conf = UIUtils.readUiConfig();
            client = NimbusClient.getConfiguredClient(conf);
            summ = client.getClient().getClusterInfo();
            
            tsumm = UIUtils.topologySummary(summ.get_topologies());
            csumm = UIUtils.clusterSummary(summ, client.getMasterHost());
            ssumm = UIUtils.supervisorSummary(summ.get_supervisors());
        } catch (Exception e) {
            LOG.error("Failed to get cluster information:", e);
            throw e;
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
    
    public List<ClusterSumm> getCsumm() {
        return csumm;
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
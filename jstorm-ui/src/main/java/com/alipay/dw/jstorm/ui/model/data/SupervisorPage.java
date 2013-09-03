package com.alipay.dw.jstorm.ui.model.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import org.apache.log4j.Logger;

import backtype.storm.generated.SupervisorWorkers;
import backtype.storm.generated.WorkerSummary;
import backtype.storm.utils.NimbusClient;

import com.alipay.dw.jstorm.ui.UIUtils;
import com.alipay.dw.jstorm.ui.model.SupervisorSumm;
import com.alipay.dw.jstorm.ui.model.WorkerSumm;

/**
 * 
 * @author xin.zhou/Longda
 */
@ManagedBean(name = "supervisorpage")
@ViewScoped
public class SupervisorPage implements Serializable {

	private static final long serialVersionUID = -6103468103521877721L;

	private static final Logger  LOG   = Logger.getLogger(SupervisorPage.class);
    
    private String               host  = "localhost";
    
    private List<SupervisorSumm> ssumm  = null;
    private List<WorkerSumm>     wsumm = null;

    
    public SupervisorPage() throws Exception {
        FacesContext ctx = FacesContext.getCurrentInstance();
        if (ctx.getExternalContext().getRequestParameterMap().get("host") != null) {
            host = (String) ctx.getExternalContext()
                    .getRequestParameterMap().get("host");
        }
        
        init(host);
    }
    
    @SuppressWarnings("rawtypes")
    public void init(String host) throws Exception {
        
        NimbusClient client = null;
        try {

            Map conf = UIUtils.readUiConfig();
            client = NimbusClient.getConfiguredClient(conf);
            SupervisorWorkers supervisorWorkers = client.getClient().getSupervisorWorkers(host);
            ssumm = new ArrayList<SupervisorSumm> ();
            ssumm.add(new SupervisorSumm(supervisorWorkers.get_supervisor()));
            generateWorkerSum(supervisorWorkers.get_workers());
            
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

    public static void main(String[] args) {
        try {
            SupervisorPage m = new SupervisorPage();
            //m.init("free-56-151.shucang.alipay.net");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
}
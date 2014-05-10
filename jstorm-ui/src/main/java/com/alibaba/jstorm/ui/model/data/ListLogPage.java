package com.alibaba.jstorm.ui.model.data;

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
import org.apache.thrift.TException;
import org.json.simple.JSONObject;

import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.utils.NimbusClient;

import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.ui.UIUtils;
import com.alibaba.jstorm.ui.model.ComponentTask;
import com.alibaba.jstorm.ui.model.LogPageIndex;
import com.alibaba.jstorm.utils.FileAttribute;
import com.alibaba.jstorm.utils.HttpserverUtils;
import com.alibaba.jstorm.utils.JStormUtils;

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
    
    private static final long   serialVersionUID = 4326599394273506085L;
    
    private static final Logger LOG              = Logger.getLogger(LogPage.class);
    
    /**
     * proxy url, which call the log service on the task node.
     */
    private static final String PROXY_URL        = "http://%s:%s/logview?%s=%s";
    
    private String host;
    
    private String summary;
    
    private List<FileAttribute> files = new ArrayList<FileAttribute>();
    /**
     * Http server port
     */
    private int                 port;
    
    public ListLogPage() throws Exception {
        FacesContext ctx = FacesContext.getCurrentInstance();
        if (ctx.getExternalContext().getRequestParameterMap().get("host") != null) {
			host = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("host");
		}
        
        init();
    }
    
    private void init() throws Exception {
        
        try {
            Map conf = UIUtils.readUiConfig();
            
            port = ConfigExtension.getDeamonHttpserverPort(conf);
            
            // proxy call
            listLogs();
            
        } catch (Exception e) {
            LOG.error(e.getCause(), e);
            throw e;
        } 
        
    }
    
    private void parseString(String input) {
    	Map<String, JSONObject> map = (Map<String, JSONObject>)JStormUtils.from_json(input);
    	
    	for (JSONObject jobj: map.values()) {
    		FileAttribute attribute = FileAttribute.fromJSONObject(jobj);
    		if (attribute != null) {
    			files.add(attribute);
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

        
        //PROXY_URL = "http://%s:%s/logview?%s=%s";
        String url = String.format(PROXY_URL, host, port,
                HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD,
                HttpserverUtils.HTTPSERVER_LOGVIEW_PARAM_CMD_LIST);
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
    
    
}

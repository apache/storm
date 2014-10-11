/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.jstorm.ui.model.data;

import java.io.Serializable;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;

import com.alibaba.jstorm.common.stats.StatBuckets;

/**
 * 
 * @author xin.zhou
 */
@ManagedBean(name = "pm")
@ViewScoped
public class Param implements Serializable {

	private static final long serialVersionUID = -1087749257427646824L;
	private String clusterName = null;
	private String topologyid = "";
	private String window = null;
	private String componentid = "";
	private String taskid = "";

	public Param() {
		init();
	}

	private void init() {
		FacesContext ctx = FacesContext.getCurrentInstance();
		if (ctx.getExternalContext().getRequestParameterMap().get("clusterName") != null) {
			clusterName = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("clusterName");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("topologyid") != null) {
			topologyid = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("topologyid");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("window") != null) {
			window = (String) ctx.getExternalContext().getRequestParameterMap()
					.get("window");
		}
		if (ctx.getExternalContext().getRequestParameterMap()
				.get("componentid") != null) {
			componentid = (String) ctx.getExternalContext()
					.getRequestParameterMap().get("componentid");
		}
		if (ctx.getExternalContext().getRequestParameterMap().get("taskid") != null) {
			taskid = (String) ctx.getExternalContext().getRequestParameterMap()
					.get("taskid");
		}

		window = StatBuckets.getShowTimeStr(window);
	}
	
	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
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

	public String getComponentid() {
		return componentid;
	}

	public void setComponentid(String componentid) {
		this.componentid = componentid;
	}

	public String getTaskid() {
		return taskid;
	}

	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}
}

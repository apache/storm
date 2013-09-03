package com.alipay.dw.jstorm.ui.model;

import java.io.Serializable;

import backtype.storm.generated.SupervisorSummary;

import com.alipay.dw.jstorm.common.stats.StatBuckets;

/**
 * mainpage:SupervisorSummary
 *
 * @author xin.zhou
 *
 */
public class SupervisorSumm implements Serializable {

	private static final long serialVersionUID = -5631649054937247850L;
	private String host;
    private String uptime;
    private String slots;
    private String usedSlots;

    public SupervisorSumm(String host, String uptime, String slots, String usedSlots) {
        this.host = host;
        this.uptime = uptime;
        this.slots = slots;
        this.usedSlots = usedSlots;
    }
    
    public SupervisorSumm(SupervisorSummary s) {
        this.host = s.get_host();
        this.uptime= StatBuckets.prettyUptimeStr(s.get_uptime_secs());
        this.slots = String.valueOf(s.get_num_workers());
        this.usedSlots = String.valueOf(s.get_num_used_workers());
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUptime() {
        return uptime;
    }

    public void setUptime(String uptime) {
        this.uptime = uptime;
    }

    public String getSlots() {
        return slots;
    }

    public void setSlots(String slots) {
        this.slots = slots;
    }

    public String getUsedSlots() {
        return usedSlots;
    }

    public void setUsedSlots(String usedSlots) {
        this.usedSlots = usedSlots;
    }
}

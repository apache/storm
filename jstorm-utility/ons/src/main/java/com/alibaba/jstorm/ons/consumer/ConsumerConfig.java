package com.alibaba.jstorm.ons.consumer;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.jstorm.ons.OnsConfig;
import com.alibaba.jstorm.utils.JStormUtils;
import com.aliyun.openservices.ons.api.PropertyKeyConst;

public class ConsumerConfig extends OnsConfig{

	private static final long serialVersionUID = 4292162795544528064L;
	private final String consumerId;
	private final int    consumerThreadNum;
	
	
	private final String nameServer;
	
	
	public ConsumerConfig(Map conf) {
		super(conf);
		
		consumerId = (String)conf.get(PropertyKeyConst.ConsumerId);
		if (StringUtils.isBlank(consumerId)) {
			throw new RuntimeException(PropertyKeyConst.ConsumerId  + " hasn't been set");
		}
		consumerThreadNum = JStormUtils.parseInt(
				conf.get(PropertyKeyConst.ConsumeThreadNums), 4);
		
		nameServer = (String)conf.get(PropertyKeyConst.NAMESRV_ADDR);
        if (nameServer != null) {
            String namekey = "rocketmq.namesrv.domain";

            String value = System.getProperty(namekey);
            if (value == null) {

                System.setProperty(namekey, nameServer);
            } else if (value.equals(nameServer) == false) {
                throw new RuntimeException("Different nameserver address in the same worker " + value + ":"
                        + nameServer);

            }
        }
		
	}


	public String getConsumerId() {
		return consumerId;
	}


	public int getConsumerThreadNum() {
		return consumerThreadNum;
	}


	public String getNameServer() {
		return nameServer;
	}
	
	

}

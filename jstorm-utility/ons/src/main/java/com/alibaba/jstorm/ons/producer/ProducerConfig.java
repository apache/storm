package com.alibaba.jstorm.ons.producer;

import java.util.Map;

import com.alibaba.jstorm.ons.OnsConfig;
import com.aliyun.openservices.ons.api.PropertyKeyConst;

public class ProducerConfig extends OnsConfig{

	private static final long serialVersionUID = 1532254745626913230L;

	private final String producerId ;
	
	public ProducerConfig(Map conf) {
		super(conf);
		
		producerId = (String)conf.get(PropertyKeyConst.ProducerId);
		if (producerId == null) {
			throw new RuntimeException(PropertyKeyConst.ProducerId + " hasn't been set");
		}
		
		
	}

	public String getProducerId() {
		return producerId;
	}
	
}

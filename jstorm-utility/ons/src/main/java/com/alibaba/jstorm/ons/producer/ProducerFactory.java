package com.alibaba.jstorm.ons.producer;

import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ProducerFactory {

	private static final Logger LOG = Logger.getLogger(ProducerFactory.class);

	public static Map<String, Producer> producers = new HashMap<String, Producer>();

	public static synchronized Producer mkInstance(ProducerConfig producerConfig) throws Exception{

		String producerId = producerConfig.getProducerId();
		Producer producer = producers.get(producerId);
		if (producer != null) {

			LOG.info("Producer of " + producerId + " has been created, don't recreate it ");
			return producer;
		}
		
		Properties properties = new Properties();
		properties.put(PropertyKeyConst.ProducerId,  producerConfig.getProducerId());
		properties.put(PropertyKeyConst.AccessKey, producerConfig.getAccessKey());
		properties.put(PropertyKeyConst.SecretKey, producerConfig.getSecretKey());
		
		producer = ONSFactory.createProducer(properties);
		producer.start();


		producers.put(producerId, producer);
		LOG.info("Successfully create " + producerId + " producer");

		return producer;

	}
	
	public static synchronized void rmInstance(String producerId) {
		Producer producer = producers.remove(producerId);
		if (producer == null) {

			LOG.info("Producer of " + producerId + " has already been shutdown ");
			return ;
		}
		
		producer.shutdown();
		LOG.info("Producer of " + producerId + " has been shutdown ");
		return ;
	}

}

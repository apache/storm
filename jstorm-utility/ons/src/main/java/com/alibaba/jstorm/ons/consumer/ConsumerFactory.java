package com.alibaba.jstorm.ons.consumer;

import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.MessageListener;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerFactory {

	private static final Logger LOG = Logger.getLogger(ConsumerFactory.class);

	public static Map<String, Consumer> consumers = new HashMap<String, Consumer>();

	public static synchronized Consumer mkInstance(ConsumerConfig consumerConfig, MessageListener listener) throws Exception {
		

		String consumerId = consumerConfig.getConsumerId();
		Consumer consumer = consumers.get(consumerId);
		if (consumer != null) {

			LOG.info("Consumer of " + consumerId + " has been created, don't recreate it ");

			// Attention, this place return null to info duplicated consumer
			return null;
		}

		Properties properties = new Properties();
		properties.put(PropertyKeyConst.AccessKey, consumerConfig.getAccessKey());
		properties.put(PropertyKeyConst.SecretKey, consumerConfig.getSecretKey());
		properties.put(PropertyKeyConst.ConsumerId, consumerId);
		properties.put(PropertyKeyConst.ConsumeThreadNums, consumerConfig.getConsumerThreadNum());
		consumer = ONSFactory.createConsumer(properties);

		consumer.subscribe(consumerConfig.getTopic(), consumerConfig.getSubExpress(), listener);
		consumer.start();

		consumers.put(consumerId, consumer);
		LOG.info("Successfully create " + consumerId + " consumer");

		return consumer;

	}

}

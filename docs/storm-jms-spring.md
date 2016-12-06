---
title: Storm JMS Integration
layout: documentation
documentation: true
---
###Connecting to JMS Using Spring's JMS Support

Create a Spring applicationContext.xml file that defines one or more destination (topic/queue) beans, as well as a connecton factory.

	<?xml version="1.0" encoding="UTF-8"?>
	<beans 
	  xmlns="http://www.springframework.org/schema/beans" 
	  xmlns:amq="http://activemq.apache.org/schema/core"
	  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	  xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd
	  http://activemq.apache.org/schema/core http://activemq.apache.org/schema/core/activemq-core.xsd">
	
		<amq:queue id="notificationQueue" physicalName="backtype.storm.contrib.example.queue" />
		
		<amq:topic id="notificationTopic" physicalName="backtype.storm.contrib.example.topic" />
	
		<amq:connectionFactory id="jmsConnectionFactory"
			brokerURL="tcp://localhost:61616" />
		
	</beans>